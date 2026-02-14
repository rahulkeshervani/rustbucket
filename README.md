# RustBucket 🦀🪣

**RustBucket** is a high-performance, lightweight Redis-compatible server implementation written in Rust. It leverages the Tokio async runtime to handle concurrent connections efficiently and implements the Redis Serialization Protocol (RESP) for compatibility with standard Redis clients (like `redis-cli`, Redis Insight, etc.).

## 🚀 Features

- **Blazing Fast**: Built on top of Rust's zero-cost abstractions and Tokio's asynchronous I/O.
- **Redis Compatible**: Speaks the standard RESP protocol. Works with existing Redis tools.
- **Multi-Type Support**: Not just strings! Supports Hashes, Lists, Sets, Sorted Sets, and JSON.
- **Memory Efficient**: Uses `Bytes` for zero-copy parsing and storage where possible.

## 📦 Supported Commands

RustBucket currently supports a subset of the Redis command usage, focusing on the most common operations:

### Strings & Generic
- `GET key` / `SET key value`
- `DEL key`
- `EXISTS key`
- `TYPE key`
- `TTL key` / `PTTL key` (Returns -1 for now, persistent compatibility)
- `KEYS pattern` (Full scan)
- `SCAN cursor [MATCH pattern] [COUNT count]`

### Hashes
- `HSET key field value`
- `HGET key field`
- `HGETALL key`
- `HDEL key field`
- `HEXISTS key field`
- `HLEN key`
- `HKEYS key`
- `HVALS key`
- `HSCAN key cursor [MATCH pattern] [COUNT count]`

### Lists
- `LPUSH key value` / `RPUSH key value`
- `LPOP key` / `RPOP key`
- `LRANGE key start stop`

### Sets
- `SADD key member`
- `SMEMBERS key`
- `SREM key member`

### Sorted Sets
- `ZADD key score member`
- `ZRANGE key start stop`

### JSON (RedisJSON compatible)
- `JSON.SET key path value` (Supports root `$` or `.`)
- `JSON.GET key [path]`

### Server & Connection
- `PING [message]`
- `AUTH username password` (Accepts default user)
- `SELECT index`
- `INFO [section]`
- `DBSIZE`
- `FLUSHDB`

## 🛠️ Usage

### Prerequisites
- [Rust](https://www.rust-lang.org/tools/install) (latest stable)

### Installation

Clone the repository:
```bash
git clone https://github.com/rahulkeshervani/rustbucket.git
cd rustbucket
```

### Running the Server

Run with Cargo:
```bash
cargo run --release
```
The server listens on `127.0.0.1:6379` by default.

### Connecting with Redis CLI

You can use the standard `redis-cli` to interact with RustBucket:

```bash
redis-cli -p 6379
```

**Examples:**
```bash
127.0.0.1:6379> SET mykey "Hello Rust"
OK
127.0.0.1:6379> GET mykey
"Hello Rust"
127.0.0.1:6379> HSET user:1 name "Rahul" age "30"
(integer) 2
127.0.0.1:6379> HGETALL user:1
1) "name"
2) "Rahul"
3) "age"
4) "30"
```

## 🏗️ Architecture

- **`src/main.rs`**: Entry point, sets up the Tokio runtime and TCP listener.
- **`src/server.rs`**: Manages connection handling and the main command processing loop.
- **`src/connection.rs`**: Low-level frame reading/writing (RESP parsing).
- **`src/db.rs`**: In-memory database storage using `Arc<RwLock<HashMap<String, DataType>>>`.
- **`src/cmd.rs`**: Command definitions, parsing logic, and execution handlers.
- **`src/frame.rs`**: Definition of Redis protocol frames (`Simple`, `Integer`, `Bulk`, `Array`, `Error`).

## 🔮 Coming Soon / Roadmap

The following features are part of the standard Redis specification but are **not yet implemented** in RustBucket:

- **Persistence**: RDB / AOF (Currently in-memory only)
- **Expiration**: Active TTL expiry (Keys currently do not expire)
- **Pub/Sub**: `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`
- **Transactions**: `MULTI`, `EXEC`, `WATCH`
- **Streams**: `XADD`, `XREAD`, etc.
- **Lua Scripting**: `EVAL`
- **Cluster & Sentinel**: Distributed support
- **Eviction Policies**: `maxmemory` handling (LRU/LFU)
- **Blocking Commands**: `BLPOP`, `BRPOP`
- **ACLs**: Fine-grained user permissions

## 🤝 Contributing

Contributions are welcome! Feel free to submit a Pull Request or open an Issue if you find bugs or want to implement missing Redis commands.

## 📄 License

This project is open-source and available under the simple [MIT License](LICENSE).
