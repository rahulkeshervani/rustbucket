# RustBucket 🦀🪣

**RustBucket** is a high-performance, multi-threaded, Redis-compatible key-value store written in 100% safe Rust. It is designed to be a drop-in replacement for Redis in many scenarios, offering superior read throughput and comparable write performance while guaranteeing memory safety.

---

## 🚀 Key Features

- **Extreme Performance**: Outperforms Redis in read operations by **~1.6x** (2.08M ops/sec vs 1.26M ops/sec) and matches write throughput (95%).
- **Sharded Architecture**: Uses **64 concurrent shards** to minimize lock contention, allowing it to scale effectively on multi-core systems.
- **Zero-Copy Optimization**: Core engine operates on `bytes::Bytes` to eliminate heap allocations and UTF-8 validation overhead for keys and values.
- **Smart Pipelining**: Intelligent I/O buffering strategy that batches writes, dramatically increasing throughput for pipelined workloads.
- **Memory Safe**: Built entirely in safe Rust, eliminating entire classes of bugs like buffer overflows and use-after-free vulnerabilities common in C/C++.
- **Async I/O**: Powered by `tokio` for efficient, non-blocking network operations.

---

## 📊 Performance Benchmarks

Benchmarks were conducted on a MacBook Pro M4 Pro using `redis-benchmark` with 16-command pipelining against both RustBucket and Redis 7.2 (reference).

| Metric | RustBucket (v2.0) | Redis (Ref) | vs Redis |
| :--- | :--- | :--- | :--- |
| **GET (Pipeline P=16)** | **2,083,333 req/sec** | 1,265,822 req/sec | **🚀 164% (1.6x Faster)** |
| **SET (Pipeline P=16)** | **1,333,333 req/sec** | 1,408,450 req/sec | ~95% (Parity) |
| **LPUSH** | 165,289 req/sec | 186,219 req/sec | ~89% |
| **PING** | 156,739 req/sec | 169,779 req/sec | ~92% |

> **Note**: RustBucket achieves >2M reads/sec on a single node, significantly outperforming standard Redis.

---

## ✅ Implemented Commands

RustBucket supports a wide range of Redis commands, including advanced data types and JSON support.

### 🔑 Keys & Strings
- `GET`, `SET`, `DEL`
- `EXISTS`, `TYPE`
- `KEYS` (Pattern matching), `SCAN` (Cursor-based flow)
- `TTL`, `PTTL` (Time-to-Live, Milliseconds)
- `DBSIZE`, `FLUSHDB`

### 📦 Hashes
- `HSET`, `HGET`, `HDEL`
- `HEXISTS`, `HGETALL`
- `HKEYS`, `HVALS`
- `HLEN`, `HSCAN`

### 📝 Lists
- `LPUSH`, `RPUSH` (O(1) with `VecDeque`)
- `LPOP`, `RPOP`
- `LRANGE`

### 🧊 Sets
- `SADD`, `SREM`
- `SMEMBERS`

### 📊 Sorted Sets
- `ZADD`, `ZRANGE` (with strict ordering)

### 📄 JSON (ReJSON Compatible)
- `JSON.SET`
- `JSON.GET`

### 🔌 Connection & Server
- `PING`, `AUTH`
- `SELECT`
- `INFO` (Server stats)

---

## 🔮 Coming Soon (Roadmap)
The following Redis features are currently **not implemented** but are planned for future releases:

- **Persistence**: AOF (Append Only File) and RDB snapshotting.
- **Pub/Sub**: `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`.
- **Transactions**: `MULTI`, `EXEC`, `DISCARD`, `WATCH`.
- **Scripting**: Lua scripting support (`EVAL`).
- **Cluster Support**: Native clustering for horizontal scaling.
- **Eviction Policies**: LRU/LFU memory eviction (currently unbounded).
- **Advanced Types**: Streams, HyperLogLog, Geo, Bitmaps.
- **ACLs**: Granular user permissions (currently simple password auth).
- **Modules API**: Support for loading external modules.

---

## 🛠️ Usage

### Prerequisites
- Rust (latest stable)
- Cargo

### Building and Running
```bash
# Build for maximum performance
cargo build --release

# Run the server on port 6379
./target/release/rustbucket
```

### Running Benchmarks
We include a benchmark suite to verify performance against a local Redis instance:
```bash
# Run benchmarks against RustBucket and reference Redis
./benchmark_suite.sh --target all
```

---

## 📄 License
This project is licensed under the MIT License.
