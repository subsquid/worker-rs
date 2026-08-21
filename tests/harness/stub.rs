//! Shared HTTP stub: static content, a per-path fault injector, and a request ledger.
//!
//! HC-1 (IB-40/41), HC-2 (IB-42) and the schema registry (IB-44) are instances of this. The
//! worker reaches all three over real HTTP with its production clients, not at a Rust seam.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use parking_lot::Mutex;

/// What the stub does with a request for a given path.
#[derive(Clone, Debug)]
pub enum Fault {
    /// Serve the registered bytes.
    None,
    /// Answer with this status and an empty body (FM-10, FM-20/23).
    Status(u16),
    /// Serve `n` bytes then end the body — a power-loss-shaped short read (CN-4).
    Truncate(usize),
    /// Serve the bytes with `byte[i] ^= 0xff` at the given offset (INV-13, GAP-5).
    Corrupt(usize),
    /// Sleep before answering — exercises P-DL-FILE-TIMEOUT.
    Delay(Duration),
    /// Send headers, then never send the body — exercises P-DL-STALL-TIMEOUT.
    Stall,
}

/// One served request, as observed by the stub. The bytes are the provenance oracle for
/// INV-13/21: what the worker committed must equal what the stub says it sent.
#[derive(Clone, Debug)]
pub struct Served {
    pub path: String,
    pub status: u16,
    pub bytes: Vec<u8>,
    /// Headers the worker attached — the decrypted assignment headers on the origin (IB-42).
    pub request_headers: Vec<(String, String)>,
}

#[derive(Default)]
struct Inner {
    content: HashMap<String, Vec<u8>>,
    faults: HashMap<String, Fault>,
    ledger: Vec<Served>,
}

/// A running HTTP stub. Dropping it shuts the server down.
pub struct HttpStub {
    addr: SocketAddr,
    inner: Arc<Mutex<Inner>>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl HttpStub {
    pub async fn start() -> Self {
        let inner = Arc::new(Mutex::new(Inner::default()));
        let app = axum::Router::new()
            .fallback(serve)
            .with_state(inner.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stub couldn't bind a loopback port");
        let addr = listener.local_addr().expect("stub has no local address");
        let (shutdown, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });

        Self {
            addr,
            inner,
            shutdown: Some(shutdown),
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Absolute URL for a stub-relative path (`"/a/b"`).
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }

    pub fn put(&self, path: impl Into<String>, bytes: impl Into<Vec<u8>>) {
        self.inner.lock().content.insert(path.into(), bytes.into());
    }

    pub fn inject(&self, path: impl Into<String>, fault: Fault) {
        self.inner.lock().faults.insert(path.into(), fault);
    }

    pub fn clear_faults(&self) {
        self.inner.lock().faults.clear();
    }

    /// Everything served so far, in order.
    pub fn ledger(&self) -> Vec<Served> {
        self.inner.lock().ledger.clone()
    }

    /// Bytes this stub actually sent for `path` on its last successful serve — the
    /// provenance oracle a committed chunk is compared against.
    pub fn last_served(&self, path: &str) -> Option<Vec<u8>> {
        self.inner
            .lock()
            .ledger
            .iter()
            .rev()
            .find(|s| s.path == path && s.status == 200)
            .map(|s| s.bytes.clone())
    }

    pub fn request_count(&self, path: &str) -> usize {
        self.inner
            .lock()
            .ledger
            .iter()
            .filter(|s| s.path == path)
            .count()
    }
}

impl Drop for HttpStub {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

fn header_pairs(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(k, v)| {
            (
                k.as_str().to_owned(),
                v.to_str().unwrap_or("<non-utf8>").to_owned(),
            )
        })
        .collect()
}

async fn serve(State(inner): State<Arc<Mutex<Inner>>>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let request_headers = header_pairs(req.headers());

    let (content, fault) = {
        let guard = inner.lock();
        (
            guard.content.get(&path).cloned(),
            guard.faults.get(&path).cloned().unwrap_or(Fault::None),
        )
    };

    let record = |status: u16, bytes: Vec<u8>| {
        inner.lock().ledger.push(Served {
            path: path.clone(),
            status,
            bytes,
            request_headers: request_headers.clone(),
        });
    };

    if let Fault::Delay(d) = fault {
        tokio::time::sleep(d).await;
    }
    if matches!(fault, Fault::Stall) {
        record(200, Vec::new());
        // Headers go out, the body never does; the client must hit its read-stall bound.
        let (mut tx, body) = futures::channel::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(1);
        tokio::spawn(async move {
            std::future::pending::<()>().await;
            let _ = futures::SinkExt::send(&mut tx, Ok(Vec::new())).await;
        });
        return Response::builder()
            .status(StatusCode::OK)
            .body(Body::from_stream(body))
            .expect("stall response is well-formed");
    }
    if let Fault::Status(code) = fault {
        record(code, Vec::new());
        return Response::builder()
            .status(StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
            .body(Body::empty())
            .expect("status response is well-formed");
    }

    let Some(mut bytes) = content else {
        record(404, Vec::new());
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .expect("404 response is well-formed");
    };

    match fault {
        Fault::Truncate(n) => bytes.truncate(n),
        Fault::Corrupt(at) => {
            if let Some(b) = bytes.get_mut(at) {
                *b ^= 0xff;
            }
        }
        _ => {}
    }

    record(200, bytes.clone());
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(bytes))
        .expect("content response is well-formed")
}
