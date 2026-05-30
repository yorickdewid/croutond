use std::fmt;
use std::io;
use std::path::Path;

use axum::http::Method;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Request, body::Incoming, client::conn::http1, header};
use hyper_util::rt::TokioIo;
use tokio::net::UnixStream;
use tracing::warn;

use crate::pool::ProxyResponse;

pub(crate) async fn send_unix_http_request(
    socket_path: &Path,
    method: Method,
    path: &str,
    body: Vec<u8>,
    content_type: Option<&str>,
) -> io::Result<ProxyResponse> {
    let stream = UnixStream::connect(socket_path).await?;
    let io = TokioIo::new(stream);

    let (mut sender, connection) = http1::handshake(io).await.map_err(io_other)?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            warn!(%error, "hyper client connection error");
        }
    });

    let uri = if path.is_empty() { "/" } else { path };
    let mut request_builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::HOST, "localhost")
        .header(header::CONNECTION, "close");

    if let Some(value) = content_type {
        request_builder = request_builder.header(header::CONTENT_TYPE, value);
    }

    let request = request_builder
        .body(Full::new(Bytes::from(body)))
        .map_err(io_invalid_input)?;

    let response = sender.send_request(request).await.map_err(io_other)?;
    response_to_proxy_response(response).await
}

async fn response_to_proxy_response(
    response: hyper::Response<Incoming>,
) -> io::Result<ProxyResponse> {
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());

    let body = response
        .into_body()
        .collect()
        .await
        .map_err(io_other)?
        .to_bytes()
        .to_vec();

    Ok(ProxyResponse {
        status,
        content_type,
        body,
    })
}

fn io_other(error: impl fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

fn io_invalid_input(error: impl fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}
