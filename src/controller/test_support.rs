use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

enum Route {
    Fixed { body: Vec<u8>, failures: usize },
    Sequence(VecDeque<Vec<u8>>),
}

pub(crate) struct TestServer {
    base: String,
    routes: Arc<Mutex<HashMap<String, Route>>>,
    hits: Arc<Mutex<HashMap<String, usize>>>,
}

impl TestServer {
    pub(crate) async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let routes: Arc<Mutex<HashMap<String, Route>>> = Default::default();
        let hits: Arc<Mutex<HashMap<String, usize>>> = Default::default();
        let (served, counted) = (Arc::clone(&routes), Arc::clone(&hits));

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let (served, counted) = (Arc::clone(&served), Arc::clone(&counted));
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let read = socket.read(&mut buf).await.unwrap_or(0);
                    let request = String::from_utf8_lossy(&buf[..read]);
                    let path = request.split_whitespace().nth(1).unwrap_or("/").to_owned();
                    *counted.lock().unwrap().entry(path.clone()).or_default() += 1;

                    let body = match served.lock().unwrap().get_mut(&path) {
                        Some(Route::Fixed { failures, .. }) if *failures > 0 => {
                            *failures -= 1;
                            None
                        }
                        Some(Route::Fixed { body, .. }) => Some(body.clone()),
                        Some(Route::Sequence(responses)) => responses.pop_front(),
                        None => None,
                    };
                    let response = match body {
                        Some(body) => ok_response(body),
                        None => b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                            .to_vec(),
                    };
                    let _ = socket.write_all(&response).await;
                });
            }
        });

        Self { base, routes, hits }
    }

    pub(crate) async fn serve_once(body: Vec<u8>) -> String {
        let server = Self::start().await;
        server.serve("/", body, 0)
    }

    pub(crate) async fn serve_sequence(responses: Vec<Vec<u8>>) -> String {
        let server = Self::start().await;
        server
            .routes
            .lock()
            .unwrap()
            .insert("/".to_owned(), Route::Sequence(responses.into()));
        server.url("/")
    }

    pub(crate) fn serve(&self, path: &str, body: Vec<u8>, failures: usize) -> String {
        self.routes
            .lock()
            .unwrap()
            .insert(path.to_owned(), Route::Fixed { body, failures });
        self.url(path)
    }

    pub(crate) fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    pub(crate) fn hits(&self, path: &str) -> usize {
        self.hits.lock().unwrap().get(path).copied().unwrap_or(0)
    }
}

pub(crate) fn gzip(bytes: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

pub(crate) fn zstd(bytes: &[u8]) -> Vec<u8> {
    zstd::encode_all(bytes, 0).unwrap()
}

fn ok_response(body: Vec<u8>) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    response.extend(body);
    response
}
