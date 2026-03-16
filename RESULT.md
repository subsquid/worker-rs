# Benchmark Results: libp2p-stream vs request-response

## Summary

The stream-based approach is **3-9x faster** for realistic workloads where
queries take time to process. The key advantage is parallel task execution:
each stream gets its own task, while request-response serializes through a
bounded event queue that drops requests under load.

## Test Environment

- Transport: QUIC over localhost (libp2p fork `c0ed330`)
- Architecture comparison:
  - **NEW (stream):** client -> open_stream -> direct read/write -> close
  - **OLD (req-resp):** client -> send_request -> swarm event loop -> bounded queue (lossy!) -> worker -> send_response

---

## Test 1: Sequential Latency (no contention)

Stream is consistently 10-20% faster due to no event loop dispatch overhead.

| Response Size | Stream QPS | Req-Resp QPS | Speedup |
|--------------|-----------|-------------|---------|
| 1 KB         | 8,569     | 7,930       | 1.1x    |
| 10 KB        | 6,489     | 5,353       | 1.2x    |
| 100 KB       | 1,803     | 1,474       | 1.2x    |
| 1 MB         | 178       | 175         | 1.0x    |

---

## Test 2: With Processing Delay (REALISTIC)

**This is the critical test.** Real queries take 1-10ms to process.
The request-response bounded queue (size=10) serializes processing through
a single worker task, while streams handle each query in its own task.

### Concurrency=10 (moderate load)

| Processing Delay | Stream QPS | Req-Resp QPS | Stream Speedup |
|-----------------|-----------|-------------|----------------|
| 0.5 ms          | 4,035     | 603         | **6.7x**       |
| 1 ms            | 3,761     | 455         | **8.3x**       |
| 5 ms            | 1,358     | 150         | **9.1x**       |
| 10 ms           | 796       | 84          | **9.4x**       |

### Concurrency=50 (high load)

| Processing Delay | Stream QPS | Req-Resp QPS | Req-Resp Drops |
|-----------------|-----------|-------------|----------------|
| 0.5 ms          | 7,034     | 17,829*     | **96.0%**      |
| 1 ms            | 7,172     | 14,557*     | **96.8%**      |
| 5 ms            | 5,981     | 6,194*      | **97.6%**      |
| 10 ms           | 3,665     | 3,811*      | **97.8%**      |

*\* QPS counts only successfully delivered responses. At conc=50, request-response
DROPS 96-98% of requests silently because the bounded queue overflows via `try_send`.*

---

## Test 3: Large Payloads

Stream writes response directly to the QUIC stream. Request-response must
relay through the swarm event loop.

| Response Size | Stream QPS | Req-Resp QPS | Speedup |
|--------------|-----------|-------------|---------|
| 100 KB       | 1,708     | 1,688       | 1.0x    |
| 1 MB         | 213       | 179         | 1.2x    |
| 5 MB         | 40        | 30          | **1.4x** |

---

## Test 4: Sustained Load with 1ms Processing

3-second continuous load. Processing delay = 1ms per query, response = 10KB.

| Concurrency | Stream QPS | Req-Resp QPS | Req-Resp Drops | Speedup   |
|------------|-----------|-------------|----------------|-----------|
| 5          | 1,597     | 457         | 0              | **3.5x**  |
| 10         | 3,618     | 460         | 0              | **7.9x**  |
| 20         | 5,623     | 30,037*     | 98.3% dropped  | -         |
| 50         | 7,783     | 55,703*     | 99.1% dropped  | -         |

*\* At conc >= 20, request-response appears fast but drops almost all requests.
Stream processes every request, scaling linearly with concurrency.*

---

## Key Takeaways

1. **Stream is 3-9x faster for realistic workloads** (concurrent queries with
   processing time), because each query runs in its own task instead of
   serializing through a single-threaded bounded queue.

2. **Request-response silently drops 96-99% of queries** under concurrent load
   with realistic processing delays. The bounded event queue (`try_send`) overflows
   because the single worker task can't drain fast enough.

3. **Stream provides proper backpressure.** When the server is at capacity,
   clients wait (via QUIC flow control) instead of having their requests
   silently dropped.

4. **For sequential queries**, stream is 10-20% faster due to no event loop overhead.

5. **For large payloads (1-5 MB)**, stream is 20-40% faster because responses
   go directly from worker task to QUIC stream, bypassing the swarm event loop relay.
