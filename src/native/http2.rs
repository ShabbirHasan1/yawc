//! WebSockets over HTTP/2 (RFC 8441).
//!
//! RFC 8441 carries WebSocket connections over a single HTTP/2 stream opened with an
//! extended CONNECT request. Only the handshake changes: once the stream is established
//! the frames are the same RFC 6455 frames, masking included, and `permessage-deflate`
//! still applies because HPACK compresses headers rather than payloads.
//!
//! # Differences from the HTTP/1.1 handshake
//!
//! The request uses `:method = CONNECT` with `:protocol = websocket` instead of a `GET`
//! with `Upgrade`. `Sec-WebSocket-Key`, `Upgrade` and `Connection` are not sent: RFC 8441
//! section 5 forbids them, since HTTP/2 stream framing replaces what the key was
//! guarding. A successful handshake answers `200`, not `101`, and there is no
//! `Sec-WebSocket-Accept` to check.
//!
//! # Server support
//!
//! A server can only accept extended CONNECT if it advertises
//! `SETTINGS_ENABLE_CONNECT_PROTOCOL`. With hyper that means calling
//! [`enable_connect_protocol`] on the HTTP/2 server builder. Without it, hyper never
//! surfaces the request and the peer resets the stream, which shows up on the client as
//! [`WebSocketError::ExtendedConnectNotSupported`].
//!
//! [`enable_connect_protocol`]: https://docs.rs/hyper/latest/hyper/server/conn/http2/struct.Builder.html#method.enable_connect_protocol

use bytes::Bytes;
use http_body_util::Empty;
use hyper::{
    ext::Protocol, header, header::HeaderValue, http::Extensions, Method, Request, Response,
    StatusCode,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncRead, AsyncWrite};
use url::Url;

use super::{HttpRequestBuilder, HttpStream, HttpWebSocket, Negotiation, Options, Role};
use crate::{compression::WebSocketExtensions, Result, WebSocketError};

/// The value of the `:protocol` pseudo-header identifying a WebSocket stream.
pub(super) const WEBSOCKET_PROTOCOL: &str = "websocket";

/// Returns whether a request is an RFC 8441 extended CONNECT for WebSockets.
///
/// Used to pick between the HTTP/1.1 and HTTP/2 handshake paths on the server. A plain
/// `CONNECT` without the `websocket` protocol is a tunnel request, not a WebSocket one,
/// and is not matched here.
pub(super) fn is_extended_connect<B>(request: &Request<B>) -> bool {
    is_websocket_connect(request.method(), request.extensions())
}

/// Same check as [`is_extended_connect`], against a request's parts.
pub(super) fn is_websocket_connect(method: &Method, extensions: &Extensions) -> bool {
    method == Method::CONNECT
        && extensions
            .get::<Protocol>()
            .is_some_and(|protocol| protocol.as_str() == WEBSOCKET_PROTOCOL)
}

/// Builds the extended CONNECT request for a WebSocket handshake over HTTP/2.
///
/// `builder` carries any caller-supplied headers. The URL supplies the authority and
/// path; the scheme is mapped from `ws`/`wss` to `http`/`https` as RFC 8441 requires.
pub(super) fn build_request(
    url: &Url,
    options: &Options,
    builder: HttpRequestBuilder,
) -> Result<Request<Empty<Bytes>>> {
    let scheme = match url.scheme() {
        "ws" | "http" => "http",
        "wss" | "https" => "https",
        _ => return Err(WebSocketError::InvalidHttpScheme),
    };

    let host = url.host().expect("hostname").to_string();
    let authority = if let Some(port) = url.port() {
        format!("{host}:{port}")
    } else {
        host
    };

    let path = &url[url::Position::BeforePath..];
    let uri = format!("{scheme}://{authority}{path}");

    // RFC 8441 section 5: no Sec-WebSocket-Key, no Upgrade, no Connection.
    let mut request = builder
        .method(Method::CONNECT)
        .uri(uri)
        .body(Empty::<Bytes>::new())
        .expect("request build");

    // Inserted rather than appended: 13 is the only version this speaks, and a caller
    // that set the header on `builder` would otherwise leave the request carrying two
    // conflicting values.
    request.headers_mut().insert(
        header::SEC_WEBSOCKET_VERSION,
        HeaderValue::from_static("13"),
    );

    request
        .extensions_mut()
        .insert(Protocol::from_static(WEBSOCKET_PROTOCOL));

    if let Some(compression) = options.compression.as_ref() {
        let extensions = WebSocketExtensions::from(compression);
        let header_value = extensions.to_string().parse().expect("extensions header");
        request
            .headers_mut()
            .insert(header::SEC_WEBSOCKET_EXTENSIONS, header_value);
    }

    Ok(request)
}

/// Checks the response to an extended CONNECT and resolves the negotiated settings.
///
/// Unlike the HTTP/1.1 handshake this accepts `200` and has no `Sec-WebSocket-Accept`,
/// `Upgrade` or `Connection` headers to validate.
pub(super) fn verify<B>(response: &Response<B>, options: Options) -> Result<Negotiation> {
    if response.status() != StatusCode::OK {
        return Err(WebSocketError::InvalidStatusCode(
            response.status().as_u16(),
        ));
    }

    let extensions = WebSocketExtensions::from_headers(response.headers());

    Negotiation::new(extensions, &options, Role::Client)
}

/// Performs an RFC 8441 handshake over an established HTTP/2-capable connection.
///
/// `io` must be a stream the peer will speak HTTP/2 on: either a TLS stream that
/// negotiated the `h2` ALPN protocol, or a plaintext stream where HTTP/2 prior knowledge
/// applies. Prefer [`WebSocket::connect`] with
/// [`http_version`](super::WebSocketBuilder::http_version), which sets ALPN up for you.
///
/// The HTTP/2 connection is driven by a spawned task and is closed once the returned
/// WebSocket is dropped.
///
/// [`WebSocket::connect`]: super::WebSocket::connect
pub async fn handshake<S>(
    url: Url,
    io: S,
    options: Options,
    builder: HttpRequestBuilder,
) -> Result<HttpWebSocket>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let request = build_request(&url, &options, builder)?;

    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;

    super::spawn_connection(async move {
        if let Err(err) = conn.await {
            log::debug!("http2 connection closed: {err:?}");
        }
    });

    let mut response = sender.send_request(request).await.map_err(connect_error)?;
    let negotiated = verify(&response, options)?;

    let upgraded = hyper::upgrade::on(&mut response)
        .await
        .map_err(connect_error)?;

    Ok(HttpWebSocket::new(
        Role::Client,
        HttpStream::from(TokioIo::new(upgraded)),
        Bytes::new(),
        negotiated,
    ))
}

/// Maps a failed extended CONNECT to a diagnosable error.
///
/// hyper does not expose the peer's settings to the client, so there is no way to check
/// `SETTINGS_ENABLE_CONNECT_PROTOCOL` up front. A server without it treats the
/// `:protocol` pseudo-header as malformed and resets the stream with `PROTOCOL_ERROR`,
/// which is specific enough to report as a missing RFC 8441 implementation rather than
/// an opaque HTTP/2 failure.
fn connect_error(err: hyper::Error) -> WebSocketError {
    use std::error::Error;

    let mut source: Option<&(dyn Error + 'static)> = Some(&err);
    while let Some(cause) = source {
        if let Some(h2_err) = cause.downcast_ref::<h2::Error>() {
            if h2_err.reason() == Some(h2::Reason::PROTOCOL_ERROR) {
                return WebSocketError::ExtendedConnectNotSupported;
            }
        }
        source = cause.source();
    }

    WebSocketError::from(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(url: &str, options: Options) -> Request<Empty<Bytes>> {
        build_request(&url.parse().unwrap(), &options, hyper::Request::builder()).unwrap()
    }

    #[test]
    fn request_uses_extended_connect() {
        let req = request("wss://example.com/chat", Options::default());

        assert_eq!(req.method(), Method::CONNECT);
        assert_eq!(
            req.extensions().get::<Protocol>().map(Protocol::as_str),
            Some(WEBSOCKET_PROTOCOL)
        );
        assert_eq!(req.uri().scheme_str(), Some("https"));
        assert_eq!(req.uri().authority().unwrap().as_str(), "example.com");
        assert_eq!(req.uri().path(), "/chat");
        assert_eq!(
            req.headers().get(header::SEC_WEBSOCKET_VERSION).unwrap(),
            "13"
        );
    }

    #[test]
    fn request_omits_http1_handshake_headers() {
        let req = request("wss://example.com/chat", Options::default());

        // RFC 8441 section 5 forbids all three over HTTP/2.
        assert!(req.headers().get(header::SEC_WEBSOCKET_KEY).is_none());
        assert!(req.headers().get(header::UPGRADE).is_none());
        assert!(req.headers().get(header::CONNECTION).is_none());
    }

    #[test]
    fn plaintext_url_maps_to_http_scheme() {
        let req = request("ws://example.com:8080/chat", Options::default());

        assert_eq!(req.uri().scheme_str(), Some("http"));
        assert_eq!(req.uri().authority().unwrap().as_str(), "example.com:8080");
    }

    #[test]
    fn caller_cannot_override_the_websocket_version() {
        // A caller-supplied version must be replaced, not appended, or the request goes
        // out with two conflicting values and the server picks whichever it reads first.
        let req = build_request(
            &"wss://example.com/chat".parse().unwrap(),
            &Options::default(),
            hyper::Request::builder().header(header::SEC_WEBSOCKET_VERSION, "8"),
        )
        .unwrap();

        let versions: Vec<_> = req
            .headers()
            .get_all(header::SEC_WEBSOCKET_VERSION)
            .iter()
            .collect();

        assert_eq!(versions, vec!["13"]);
    }

    #[test]
    fn caller_headers_are_preserved() {
        let req = build_request(
            &"wss://example.com/chat".parse().unwrap(),
            &Options::default(),
            hyper::Request::builder().header("authorization", "Bearer token"),
        )
        .unwrap();

        assert_eq!(req.headers().get("authorization").unwrap(), "Bearer token");
    }

    #[test]
    fn request_offers_compression_when_enabled() {
        let req = request(
            "wss://example.com/chat",
            Options::default().with_balanced_compression(),
        );

        let offer = req
            .headers()
            .get(header::SEC_WEBSOCKET_EXTENSIONS)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(offer.contains("permessage-deflate"));
    }

    #[test]
    fn rejects_non_websocket_scheme() {
        let err = build_request(
            &"ftp://example.com/chat".parse().unwrap(),
            &Options::default(),
            hyper::Request::builder(),
        )
        .unwrap_err();

        assert!(matches!(err, WebSocketError::InvalidHttpScheme));
    }

    #[test]
    fn verify_accepts_200_not_101() {
        let ok = Response::builder().status(200).body(()).unwrap();
        assert!(verify(&ok, Options::default()).is_ok());

        let switching = Response::builder().status(101).body(()).unwrap();
        let err = verify(&switching, Options::default()).unwrap_err();
        assert!(matches!(err, WebSocketError::InvalidStatusCode(101)));
    }

    #[test]
    fn is_extended_connect_ignores_plain_connect() {
        let mut tunnel = Request::builder()
            .method(Method::CONNECT)
            .uri("https://example.com/")
            .body(())
            .unwrap();
        assert!(!is_extended_connect(&tunnel));

        tunnel
            .extensions_mut()
            .insert(Protocol::from_static(WEBSOCKET_PROTOCOL));
        assert!(is_extended_connect(&tunnel));
    }

    #[test]
    fn is_extended_connect_ignores_http1_upgrade() {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/chat")
            .header(header::UPGRADE, "websocket")
            .body(())
            .unwrap();

        assert!(!is_extended_connect(&req));
    }

    #[test]
    fn upgrade_rejects_wrong_version() {
        let mut req = Request::builder()
            .method(Method::CONNECT)
            .uri("https://example.com/chat")
            .header(header::SEC_WEBSOCKET_VERSION, "8")
            .body(())
            .unwrap();
        req.extensions_mut()
            .insert(Protocol::from_static(WEBSOCKET_PROTOCOL));

        let err = crate::WebSocket::upgrade_with_options(&mut req, Options::default()).unwrap_err();
        assert!(matches!(err, WebSocketError::InvalidSecWebsocketVersion));
    }

    #[test]
    fn upgrade_answers_200_without_accept_header() {
        let mut req = Request::builder()
            .method(Method::CONNECT)
            .uri("https://example.com/chat")
            .header(header::SEC_WEBSOCKET_VERSION, "13")
            .body(())
            .unwrap();
        req.extensions_mut()
            .insert(Protocol::from_static(WEBSOCKET_PROTOCOL));

        let (response, _fut) =
            crate::WebSocket::upgrade_with_options(&mut req, Options::default()).unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .headers()
            .get(header::SEC_WEBSOCKET_ACCEPT)
            .is_none());
        assert!(response.headers().get(header::UPGRADE).is_none());
        assert!(response.headers().get(header::CONNECTION).is_none());
    }

    #[test]
    fn upgrade_negotiates_compression() {
        let mut req = Request::builder()
            .method(Method::CONNECT)
            .uri("https://example.com/chat")
            .header(header::SEC_WEBSOCKET_VERSION, "13")
            .header(header::SEC_WEBSOCKET_EXTENSIONS, "permessage-deflate")
            .body(())
            .unwrap();
        req.extensions_mut()
            .insert(Protocol::from_static(WEBSOCKET_PROTOCOL));

        let (response, _fut) = crate::WebSocket::upgrade_with_options(
            &mut req,
            Options::default().with_balanced_compression(),
        )
        .unwrap();

        let agreed = response
            .headers()
            .get(header::SEC_WEBSOCKET_EXTENSIONS)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(agreed.contains("permessage-deflate"));
    }
}
