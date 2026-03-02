/// Benchmark: libp2p request-response vs stream-based query transport.
///
/// Architecture comparison:
///
/// OLD (request-response):
///   client.send_request() -> protocol codec -> server swarm event ->
///   bounded event queue (LOSSY: try_send, drops if full) ->
///   worker task processes -> send_response() via swarm -> protocol codec -> client
///
/// NEW (stream):
///   client.open_stream() -> write request bytes -> server IncomingStreams ->
///   spawn task per stream -> read request -> process ->
///   write response directly to stream -> client reads response
///
/// Key differences tested:
///   1. Queue saturation: req-resp DROPS events under load; stream has backpressure
///   2. Large payload efficiency: stream writes directly, no event loop relay
///   3. Sequential latency: stream avoids event dispatch overhead
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::prelude::*;
use libp2p::swarm::SwarmEvent;
use libp2p::{PeerId, StreamProtocol};

const STREAM_PROTOCOL: StreamProtocol = StreamProtocol::new("/bench/stream/1.0.0");
const REQ_RESP_PROTOCOL: &str = "/bench/reqresp/1.0.0";

fn make_payload(size: usize) -> Vec<u8> {
    vec![0xAB; size]
}

// --- Request-Response Codec ---

#[derive(Debug, Clone)]
struct BenchCodec {
    max_req: u64,
    max_resp: u64,
}

#[async_trait::async_trait]
impl libp2p::request_response::Codec for BenchCodec {
    type Protocol = &'static str;
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> std::io::Result<Self::Request>
    where T: futures::AsyncRead + Unpin + Send {
        let mut buf = Vec::new();
        io.take(self.max_req + 1).read_to_end(&mut buf).await?;
        Ok(buf)
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T) -> std::io::Result<Self::Response>
    where T: futures::AsyncRead + Unpin + Send {
        let mut buf = Vec::new();
        io.take(self.max_resp + 1).read_to_end(&mut buf).await?;
        Ok(buf)
    }

    async fn write_request<T>(&mut self, _: &Self::Protocol, io: &mut T, req: Self::Request) -> std::io::Result<()>
    where T: futures::AsyncWrite + Unpin + Send {
        io.write_all(&req).await
    }

    async fn write_response<T>(&mut self, _: &Self::Protocol, io: &mut T, res: Self::Response) -> std::io::Result<()>
    where T: futures::AsyncWrite + Unpin + Send {
        io.write_all(&res).await
    }
}

// --- Result ---

#[derive(Clone)]
struct BenchResult {
    total: usize,
    succeeded: usize,
    dropped: usize,
    errors: usize,
    elapsed: Duration,
}

impl std::fmt::Display for BenchResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let qps = self.succeeded as f64 / self.elapsed.as_secs_f64();
        let avg = if self.succeeded > 0 {
            self.elapsed / self.succeeded as u32
        } else {
            Duration::ZERO
        };
        let loss = (self.dropped + self.errors) as f64 / self.total.max(1) as f64 * 100.0;
        write!(
            f,
            "ok={:<5} drop={:<4} err={:<3} | qps={:>8.0}  avg_lat={:>8.2?}  loss={:>5.1}%",
            self.succeeded, self.dropped, self.errors, qps, avg, loss,
        )
    }
}

// ============================================================
// Stream testbed
// ============================================================

struct StreamTestbed {
    peer: PeerId,
    control: libp2p_stream::Control,
    resp_size: Arc<AtomicUsize>,
    processing_us: Arc<AtomicUsize>, // simulated query processing time in µs
}

async fn setup_stream() -> StreamTestbed {
    let beh = libp2p_stream::Behaviour::new();
    let mut ctl = beh.new_control();
    let mut incoming = ctl.accept(STREAM_PROTOCOL).unwrap();

    let mut server = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|_| beh)
        .unwrap()
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();
    server.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap()).unwrap();
    let peer = *server.local_peer_id();
    let addr = loop {
        if let SwarmEvent::NewListenAddr { address, .. } = server.select_next_some().await { break address; }
    };

    let resp_size = Arc::new(AtomicUsize::new(1024));
    let rs = resp_size.clone();
    let processing_us = Arc::new(AtomicUsize::new(0));
    let pu = processing_us.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = server.select_next_some() => {}
                Some((_p, mut s)) = incoming.next() => {
                    let sz = rs.load(Ordering::Relaxed);
                    let delay = pu.load(Ordering::Relaxed);
                    tokio::spawn(async move {
                        let mut buf = Vec::new();
                        let _ = s.read_to_end(&mut buf).await;
                        if delay > 0 {
                            tokio::time::sleep(Duration::from_micros(delay as u64)).await;
                        }
                        let _ = s.write_all(&make_payload(sz)).await;
                        let _ = s.close().await;
                    });
                }
            }
        }
    });

    let cb = libp2p_stream::Behaviour::new();
    let control = cb.new_control();
    let mut client = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|_| cb)
        .unwrap()
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();
    client.dial(addr.with(libp2p::multiaddr::Protocol::P2p(peer))).unwrap();
    loop {
        if let SwarmEvent::ConnectionEstablished { .. } = client.select_next_some().await { break; }
    }
    tokio::spawn(async move { loop { let _ = client.select_next_some().await; } });

    StreamTestbed { peer, control, resp_size, processing_us }
}

async fn run_stream(
    tb: &StreamTestbed, req_size: usize, max_concurrent: usize, total: usize,
) -> BenchResult {
    let ok = Arc::new(AtomicUsize::new(0));
    let err = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(total);
    for _ in 0..total {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let mut ctl = tb.control.clone();
        let peer = tb.peer;
        let ok = ok.clone();
        let err = err.clone();
        handles.push(tokio::spawn(async move {
            let _p = permit;
            let r: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
                let mut s = ctl.open_stream(peer, STREAM_PROTOCOL).await?;
                s.write_all(&make_payload(req_size)).await?;
                s.close().await?;
                let mut buf = Vec::new();
                s.read_to_end(&mut buf).await?;
                Ok(())
            }.await;
            match r {
                Ok(_) => ok.fetch_add(1, Ordering::Relaxed),
                Err(_) => err.fetch_add(1, Ordering::Relaxed),
            };
        }));
    }
    futures::future::join_all(handles).await;

    BenchResult {
        total,
        succeeded: ok.load(Ordering::Relaxed),
        dropped: 0,
        errors: err.load(Ordering::Relaxed),
        elapsed: start.elapsed(),
    }
}

// ============================================================
// Request-Response testbed
//
// Server architecture mirrors real WorkerTransport:
//   swarm event loop -> try_send(bounded_queue) -> worker reads -> send_response via channel
// ============================================================

struct ReqRespTestbed {
    tx: tokio::sync::mpsc::UnboundedSender<(Vec<u8>, tokio::sync::oneshot::Sender<Result<Vec<u8>, ()>>)>,
    resp_size: Arc<AtomicUsize>,
    server_drops: Arc<AtomicUsize>,
    processing_us: Arc<AtomicUsize>,
}

async fn setup_reqresp(queue_size: usize) -> ReqRespTestbed {
    let codec = BenchCodec { max_req: 1 << 20, max_resp: 100 << 20 };
    let beh = libp2p::request_response::Behaviour::with_codec(
        codec.clone(),
        vec![(REQ_RESP_PROTOCOL, libp2p::request_response::ProtocolSupport::Full)],
        libp2p::request_response::Config::default(),
    );
    let mut server = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|_| beh)
        .unwrap()
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();
    server.listen_on("/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap()).unwrap();
    let peer = *server.local_peer_id();
    let addr = loop {
        if let SwarmEvent::NewListenAddr { address, .. } = server.select_next_some().await { break address; }
    };

    let resp_size = Arc::new(AtomicUsize::new(1024));
    let rs = resp_size.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let drops2 = drops.clone();
    let processing_us = Arc::new(AtomicUsize::new(0));
    let pu = processing_us.clone();

    // Bounded queue (lossy, like real WorkerTransport)
    type Item = (Vec<u8>, libp2p::request_response::ResponseChannel<Vec<u8>>);
    let (qtx, mut qrx) = tokio::sync::mpsc::channel::<Item>(queue_size);

    // Worker: process from queue, send responses back via channel
    let (rtx, mut rrx) = tokio::sync::mpsc::unbounded_channel::<(
        libp2p::request_response::ResponseChannel<Vec<u8>>, Vec<u8>,
    )>();
    tokio::spawn(async move {
        while let Some((_req, ch)) = qrx.recv().await {
            let delay = pu.load(Ordering::Relaxed);
            if delay > 0 {
                tokio::time::sleep(Duration::from_micros(delay as u64)).await;
            }
            let _ = rtx.send((ch, make_payload(rs.load(Ordering::Relaxed))));
        }
    });

    // Server swarm loop
    tokio::spawn(async move {
        loop {
            tokio::select! {
                event = server.select_next_some() => {
                    if let SwarmEvent::Behaviour(libp2p::request_response::Event::Message {
                        message: libp2p::request_response::Message::Request { request, channel, .. }, ..
                    }) = event {
                        if qtx.try_send((request, channel)).is_err() {
                            drops2.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Some((ch, resp)) = rrx.recv() => {
                    let _ = server.behaviour_mut().send_response(ch, resp);
                }
            }
        }
    });

    // Client
    let cb = libp2p::request_response::Behaviour::with_codec(
        codec,
        vec![(REQ_RESP_PROTOCOL, libp2p::request_response::ProtocolSupport::Full)],
        libp2p::request_response::Config::default(),
    );
    let mut client = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_quic()
        .with_behaviour(|_| cb)
        .unwrap()
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(120)))
        .build();
    client.dial(addr.with(libp2p::multiaddr::Protocol::P2p(peer))).unwrap();
    loop {
        if let SwarmEvent::ConnectionEstablished { .. } = client.select_next_some().await { break; }
    }

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<(
        Vec<u8>, tokio::sync::oneshot::Sender<Result<Vec<u8>, ()>>,
    )>();
    tokio::spawn(async move {
        use std::collections::HashMap;
        let mut pending = HashMap::<
            libp2p::request_response::OutboundRequestId,
            tokio::sync::oneshot::Sender<Result<Vec<u8>, ()>>,
        >::new();
        loop {
            tokio::select! {
                Some((req, resp_tx)) = rx.recv() => {
                    let id = client.behaviour_mut().send_request(&peer, req);
                    pending.insert(id, resp_tx);
                }
                event = client.select_next_some() => {
                    match event {
                        SwarmEvent::Behaviour(libp2p::request_response::Event::Message {
                            message: libp2p::request_response::Message::Response { request_id, response, .. }, ..
                        }) => { if let Some(tx) = pending.remove(&request_id) { let _ = tx.send(Ok(response)); } }
                        SwarmEvent::Behaviour(libp2p::request_response::Event::OutboundFailure { request_id, .. }) => {
                            if let Some(tx) = pending.remove(&request_id) { let _ = tx.send(Err(())); }
                        }
                        _ => {}
                    }
                }
            }
        }
    });

    ReqRespTestbed { tx, resp_size, server_drops: drops, processing_us }
}

async fn run_reqresp(
    tb: &ReqRespTestbed, req_size: usize, concurrency: usize, total: usize,
) -> BenchResult {
    tb.server_drops.store(0, Ordering::Relaxed);
    let ok = Arc::new(AtomicUsize::new(0));
    let err = Arc::new(AtomicUsize::new(0));
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(total);
    for _ in 0..total {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let tx = tb.tx.clone();
        let ok = ok.clone();
        let err = err.clone();
        handles.push(tokio::spawn(async move {
            let _p = permit;
            let (rtx, rrx) = tokio::sync::oneshot::channel();
            if tx.send((make_payload(req_size), rtx)).is_err() {
                err.fetch_add(1, Ordering::Relaxed);
                return;
            }
            match tokio::time::timeout(Duration::from_secs(10), rrx).await {
                Ok(Ok(Ok(_))) => ok.fetch_add(1, Ordering::Relaxed),
                _ => err.fetch_add(1, Ordering::Relaxed),
            };
        }));
    }
    futures::future::join_all(handles).await;

    BenchResult {
        total,
        succeeded: ok.load(Ordering::Relaxed),
        dropped: tb.server_drops.load(Ordering::Relaxed),
        errors: err.load(Ordering::Relaxed),
        elapsed: start.elapsed(),
    }
}

// ============================================================

fn print_comparison(label: &str, stream: &BenchResult, reqresp: &BenchResult) {
    let sq = stream.succeeded as f64 / stream.elapsed.as_secs_f64();
    let rq = reqresp.succeeded as f64 / reqresp.elapsed.as_secs_f64();
    let factor = sq / rq;
    println!("  {:<24} stream:     {}", label, stream);
    println!("  {:<24} req-resp:   {}", "", reqresp);
    if factor >= 1.0 {
        println!("  {:<24} => stream {:.1}x FASTER, {} fewer drops\n", "", factor,
            if reqresp.dropped > stream.dropped { reqresp.dropped - stream.dropped } else { 0 });
    } else {
        println!("  {:<24} => req-resp {:.1}x faster, BUT {} server drops\n", "", 1.0/factor, reqresp.dropped);
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    println!("=== P2P Transport Protocol Benchmark ===\n");
    println!("  NEW (stream):   client -> open_stream -> direct I/O -> close");
    println!("  OLD (req-resp): client -> send_request -> swarm event loop -> bounded queue (lossy!) -> worker -> send_response\n");

    let stream_tb = rt.block_on(setup_stream());

    // Warm up stream
    rt.block_on(run_stream(&stream_tb, 256, 1, 50));

    let request_size = 256;

    // ============================================================
    // Test 1: Sequential latency (no contention)
    // ============================================================
    println!("== Test 1: Sequential Latency (1 query at a time, 500 queries) ==\n");
    for (resp_size, label) in [(1024, "1KB"), (10240, "10KB"), (102400, "100KB"), (1048576, "1MB")] {
        stream_tb.resp_size.store(resp_size, Ordering::Relaxed);
        let rr_tb = rt.block_on(setup_reqresp(100));
        rr_tb.resp_size.store(resp_size, Ordering::Relaxed);
        rt.block_on(run_reqresp(&rr_tb, request_size, 1, 20));

        let sr = rt.block_on(run_stream(&stream_tb, request_size, 1, 500));
        let rr = rt.block_on(run_reqresp(&rr_tb, request_size, 1, 500));
        print_comparison(&format!("resp={}", label), &sr, &rr);
    }

    // ============================================================
    // Test 2: With realistic processing delay (THIS IS THE KEY TEST)
    //
    // Real queries take 1-100ms to execute. During processing,
    // the req-resp bounded queue fills up and DROPS requests.
    // Stream approach: each query gets its own task, no queue.
    // ============================================================
    println!("== Test 2: With Processing Delay (REALISTIC — shows queue drops) ==\n");
    println!("  req-resp queue size: 10 (realistic WorkerTransport config)\n");

    stream_tb.resp_size.store(10 * 1024, Ordering::Relaxed);

    for (delay_us, delay_label) in [(500, "0.5ms"), (1000, "1ms"), (5000, "5ms"), (10000, "10ms")] {
        stream_tb.processing_us.store(delay_us, Ordering::Relaxed);

        for conc in [10, 50] {
            let n = 500;
            let sr = rt.block_on(run_stream(&stream_tb, request_size, conc, n));
            let rr_tb = rt.block_on(setup_reqresp(10));
            rr_tb.resp_size.store(10 * 1024, Ordering::Relaxed);
            rr_tb.processing_us.store(delay_us, Ordering::Relaxed);
            rt.block_on(run_reqresp(&rr_tb, request_size, 1, 10));
            let rr = rt.block_on(run_reqresp(&rr_tb, request_size, conc, n));
            print_comparison(
                &format!("delay={} conc={}", delay_label, conc),
                &sr,
                &rr,
            );
        }
    }

    // Reset processing delay
    stream_tb.processing_us.store(0, Ordering::Relaxed);

    // ============================================================
    // Test 3: Large payloads (stream writes directly, no event relay)
    // ============================================================
    println!("== Test 3: Large Payloads (stream avoids event loop relay) ==\n");

    for (resp_size, label) in [(102400, "100KB"), (1048576, "1MB"), (5242880, "5MB")] {
        stream_tb.resp_size.store(resp_size, Ordering::Relaxed);
        let rr_tb = rt.block_on(setup_reqresp(100));
        rr_tb.resp_size.store(resp_size, Ordering::Relaxed);
        rt.block_on(run_reqresp(&rr_tb, request_size, 1, 10));

        let sr = rt.block_on(run_stream(&stream_tb, request_size, 5, 200));
        let rr = rt.block_on(run_reqresp(&rr_tb, request_size, 5, 200));
        print_comparison(&format!("conc=5 resp={}", label), &sr, &rr);
    }

    // ============================================================
    // Test 4: Sustained throughput with processing delay
    // ============================================================
    println!("== Test 4: Sustained Load with 1ms Processing (3 second runs) ==\n");
    println!("  This simulates a real worker processing queries.\n");

    stream_tb.resp_size.store(10 * 1024, Ordering::Relaxed);
    stream_tb.processing_us.store(1000, Ordering::Relaxed);

    for conc in [5, 10, 20, 50] {
        let rr_tb = rt.block_on(setup_reqresp(10));
        rr_tb.resp_size.store(10 * 1024, Ordering::Relaxed);
        rr_tb.processing_us.store(1000, Ordering::Relaxed);
        rt.block_on(run_reqresp(&rr_tb, request_size, 1, 10));

        let duration = Duration::from_secs(3);
        let sr = rt.block_on(sustained_stream(&stream_tb, request_size, conc, duration));
        let rr = rt.block_on(sustained_reqresp(&rr_tb, request_size, conc, duration));
        print_comparison(&format!("conc={}", conc), &sr, &rr);
    }
}

async fn sustained_stream(
    tb: &StreamTestbed, req_size: usize, concurrency: usize, duration: Duration,
) -> BenchResult {
    let ok = Arc::new(AtomicUsize::new(0));
    let err = Arc::new(AtomicUsize::new(0));
    let total = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let deadline = start + duration;

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let mut ctl = tb.control.clone();
        let peer = tb.peer;
        let ok = ok.clone();
        let err = err.clone();
        let total = total.clone();
        handles.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                total.fetch_add(1, Ordering::Relaxed);
                let r: Result<(), Box<dyn std::error::Error + Send + Sync>> = async {
                    let mut s = ctl.open_stream(peer, STREAM_PROTOCOL).await?;
                    s.write_all(&make_payload(req_size)).await?;
                    s.close().await?;
                    let mut buf = Vec::new();
                    s.read_to_end(&mut buf).await?;
                    Ok(())
                }.await;
                match r { Ok(_) => { ok.fetch_add(1, Ordering::Relaxed); } Err(_) => { err.fetch_add(1, Ordering::Relaxed); } };
            }
        }));
    }
    futures::future::join_all(handles).await;
    BenchResult {
        total: total.load(Ordering::Relaxed),
        succeeded: ok.load(Ordering::Relaxed),
        dropped: 0,
        errors: err.load(Ordering::Relaxed),
        elapsed: start.elapsed(),
    }
}

async fn sustained_reqresp(
    tb: &ReqRespTestbed, req_size: usize, concurrency: usize, duration: Duration,
) -> BenchResult {
    tb.server_drops.store(0, Ordering::Relaxed);
    let ok = Arc::new(AtomicUsize::new(0));
    let err = Arc::new(AtomicUsize::new(0));
    let total = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    let deadline = start + duration;

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let tx = tb.tx.clone();
        let ok = ok.clone();
        let err = err.clone();
        let total = total.clone();
        handles.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                total.fetch_add(1, Ordering::Relaxed);
                let (rtx, rrx) = tokio::sync::oneshot::channel();
                if tx.send((make_payload(req_size), rtx)).is_err() {
                    err.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                match tokio::time::timeout(Duration::from_secs(10), rrx).await {
                    Ok(Ok(Ok(_))) => { ok.fetch_add(1, Ordering::Relaxed); }
                    _ => { err.fetch_add(1, Ordering::Relaxed); }
                };
            }
        }));
    }
    futures::future::join_all(handles).await;
    BenchResult {
        total: total.load(Ordering::Relaxed),
        succeeded: ok.load(Ordering::Relaxed),
        dropped: tb.server_drops.load(Ordering::Relaxed),
        errors: err.load(Ordering::Relaxed),
        elapsed: start.elapsed(),
    }
}
