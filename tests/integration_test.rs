use bytes::Bytes;
use rustbucket::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};

async fn get_client() -> Connection {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        rustbucket::run(listener).await.unwrap();
    });

    let stream = TcpStream::connect(format!("127.0.0.1:{}", port)).await.unwrap();
    Connection::new(stream)
}

#[tokio::test]
async fn test_ping_auth() {
    let mut client = get_client().await;

    // PING
    let cmd = Frame::Array(vec![Frame::Simple("ping".to_string())]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Simple(s) => assert_eq!(s, "PONG"),
        _ => panic!("Expected SimpleString PONG"),
    }

    // PING hello
    let cmd = Frame::Array(vec![
        Frame::Simple("ping".to_string()),
        Frame::Bulk(Bytes::from("hello")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Bulk(b) => assert_eq!(b, "hello"),
        _ => panic!("Expected BulkString hello"),
    }

    // AUTH
    let cmd = Frame::Array(vec![
        Frame::Simple("auth".to_string()),
        Frame::Bulk(Bytes::from("pass")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Simple(s) => assert_eq!(s, "OK"),
        _ => panic!("Expected SimpleString OK"),
    }
}

#[tokio::test]
async fn test_set_get_del() {
    let mut client = get_client().await;

    // SET
    let cmd = Frame::Array(vec![
        Frame::Simple("set".to_string()),
        Frame::Bulk(Bytes::from("foo")),
        Frame::Bulk(Bytes::from("bar")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Simple(s) => assert_eq!(s, "OK"),
        _ => panic!("Expected SimpleString OK"),
    }

    // GET
    let cmd = Frame::Array(vec![
        Frame::Simple("get".to_string()),
        Frame::Bulk(Bytes::from("foo")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Bulk(b) => assert_eq!(b, "bar"),
        _ => panic!("Expected BulkString bar"),
    }

    // DEL
    let cmd = Frame::Array(vec![
        Frame::Simple("del".to_string()),
        Frame::Bulk(Bytes::from("foo")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer 1"),
    }
}

#[tokio::test]
async fn test_info() {
    let mut client = get_client().await;

    // INFO
    let cmd = Frame::Array(vec![Frame::Simple("info".to_string())]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Bulk(b) => {
            let s = std::str::from_utf8(&b).unwrap();
            assert!(s.contains("role:master"));
        }
        _ => panic!("Expected BulkString"),
    }
}

#[tokio::test]
async fn test_discovery_commands() {
    let mut client = get_client().await;

    // FLUSHDB
    let cmd = Frame::Array(vec![Frame::Simple("flushdb".to_string())]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Simple(s) => assert_eq!(s, "OK"),
        _ => panic!("Expected OK"),
    }

    // DBSIZE -> 0
    let cmd = Frame::Array(vec![Frame::Simple("dbsize".to_string())]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected 0"),
    }

    // SET k1 v1
    let cmd = Frame::Array(vec![
        Frame::Simple("set".to_string()),
        Frame::Bulk(Bytes::from("k1")),
        Frame::Bulk(Bytes::from("v1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // SET k2 v2
    let cmd = Frame::Array(vec![
        Frame::Simple("set".to_string()),
        Frame::Bulk(Bytes::from("k2")),
        Frame::Bulk(Bytes::from("v2")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // DBSIZE -> 2
    let cmd = Frame::Array(vec![Frame::Simple("dbsize".to_string())]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 2),
        other => panic!("Expected 2, got {:?}", other),
    }

    // KEYS *
    let cmd = Frame::Array(vec![
        Frame::Simple("keys".to_string()),
        Frame::Simple("*".to_string()),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Array(arr) => {
            assert_eq!(arr.len(), 2);
            let mut keys: Vec<String> = arr.iter().map(|f| match f {
                Frame::Bulk(b) => std::str::from_utf8(b).unwrap().to_string(),
                _ => panic!("Expected Bulk"),
            }).collect();
            keys.sort();
            assert_eq!(keys, vec!["k1", "k2"]);
        }
        _ => panic!("Expected Array"),
    }

    // TYPE k1
    let cmd = Frame::Array(vec![
        Frame::Simple("type".to_string()),
        Frame::Bulk(Bytes::from("k1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Simple(s) => assert_eq!(s, "string"),
        _ => panic!("Expected string"),
    }
}

#[tokio::test]
async fn test_advanced_types() {
    let mut client = get_client().await;

    // EXISTS key1 -> 0
    let cmd = Frame::Array(vec![
        Frame::Simple("exists".to_string()),
        Frame::Bulk(Bytes::from("key1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected Integer 0"),
    }

    // HSET myhash field1 value1
    let cmd = Frame::Array(vec![
        Frame::Simple("hset".to_string()),
        Frame::Bulk(Bytes::from("myhash")),
        Frame::Bulk(Bytes::from("field1")),
        Frame::Bulk(Bytes::from("value1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap(); // Expect 1

    // HGET myhash field1
    let cmd = Frame::Array(vec![
        Frame::Simple("hget".to_string()),
        Frame::Bulk(Bytes::from("myhash")),
        Frame::Bulk(Bytes::from("field1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Bulk(b) => assert_eq!(b, "value1"),
        _ => panic!("Expected value1"),
    }

    // LPUSH mylist v1
    let cmd = Frame::Array(vec![
        Frame::Simple("lpush".to_string()),
        Frame::Bulk(Bytes::from("mylist")),
        Frame::Bulk(Bytes::from("v1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // RPOP mylist -> v1
    let cmd = Frame::Array(vec![
        Frame::Simple("rpop".to_string()),
        Frame::Bulk(Bytes::from("mylist")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Bulk(b) => assert_eq!(b, "v1"),
        _ => panic!("Expected v1"),
    }

    // SADD myset m1
    let cmd = Frame::Array(vec![
        Frame::Simple("sadd".to_string()),
        Frame::Bulk(Bytes::from("myset")),
        Frame::Bulk(Bytes::from("m1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // SMEMBERS myset -> [m1]
    let cmd = Frame::Array(vec![
        Frame::Simple("smembers".to_string()),
        Frame::Bulk(Bytes::from("myset")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Array(arr) => assert_eq!(arr.len(), 1),
        _ => panic!("Expected Array"),
    }

    // ZADD myzset 1.0 m1
    let cmd = Frame::Array(vec![
        Frame::Simple("zadd".to_string()),
        Frame::Bulk(Bytes::from("myzset")),
        Frame::Bulk(Bytes::from("1.0")),
        Frame::Bulk(Bytes::from("m1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 1),
        other => panic!("ZADD expected Integer(1), got {:?}", other),
    }

    // ZRANGE myzset 0 -1
    let cmd = Frame::Array(vec![
        Frame::Simple("zrange".to_string()),
        Frame::Bulk(Bytes::from("myzset")),
        Frame::Bulk(Bytes::from("0")),
        Frame::Bulk(Bytes::from("-1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Array(arr) => {
             assert_eq!(arr.len(), 1);
             match &arr[0] {
                 Frame::Bulk(b) => assert_eq!(b, "m1"),
                 _ => panic!("Expected Bulk"),
             }
        }
        other => panic!("ZRANGE Expected Array, got {:?}", other),
    }

    // JSON.SET myjson $ '{"a":1}'
    let cmd = Frame::Array(vec![
        Frame::Simple("json.set".to_string()),
        Frame::Bulk(Bytes::from("myjson")),
        Frame::Bulk(Bytes::from("$")),
        Frame::Bulk(Bytes::from("{\"a\":1}")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // JSON.GET myjson
    let cmd = Frame::Array(vec![
        Frame::Simple("json.get".to_string()),
        Frame::Bulk(Bytes::from("myjson")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Bulk(b) => {
             let s = std::str::from_utf8(&b).unwrap();
             assert!(s.contains("\"a\":1"));
        }
        _ => panic!("Expected Bulk"),
    }
}

#[tokio::test]
async fn test_redis_insight_compatibility() {
    let mut client = get_client().await;

    // SELECT 0
    let cmd = Frame::Array(vec![
        Frame::Simple("select".to_string()),
        Frame::Bulk(Bytes::from("0")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Simple(s) => assert_eq!(s, "OK"),
        _ => panic!("Expected OK"),
    }

    // SET foo bar
    let cmd = Frame::Array(vec![
        Frame::Simple("set".to_string()),
        Frame::Bulk(Bytes::from("foo")),
        Frame::Bulk(Bytes::from("bar")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // TTL foo -> -1
    let cmd = Frame::Array(vec![
        Frame::Simple("ttl".to_string()),
        Frame::Bulk(Bytes::from("foo")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, -1),
        _ => panic!("Expected Integer -1"),
    }

    // PTTL foo -> -1
    let cmd = Frame::Array(vec![
        Frame::Simple("pttl".to_string()),
        Frame::Bulk(Bytes::from("foo")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, -1),
        _ => panic!("Expected Integer -1"),
    }

    // TYPE checks
    // Hash
    let cmd = Frame::Array(vec![
        Frame::Simple("hset".to_string()),
        Frame::Bulk(Bytes::from("myhash_type")),
        Frame::Bulk(Bytes::from("f")),
        Frame::Bulk(Bytes::from("v")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    let cmd = Frame::Array(vec![
        Frame::Simple("type".to_string()),
        Frame::Bulk(Bytes::from("myhash_type")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Simple(s) => assert_eq!(s, "hash"),
        _ => panic!("Expected hash"),
    }
}

#[tokio::test]
async fn test_hlen() {
    let mut client = get_client().await;

    // HSET myhash field1 "Hello"
    let cmd = Frame::Array(vec![
        Frame::Simple("hset".to_string()),
        Frame::Bulk(Bytes::from("myhash_hlen")),
        Frame::Bulk(Bytes::from("field1")),
        Frame::Bulk(Bytes::from("Hello")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // HLEN myhash -> 1
    let cmd = Frame::Array(vec![
        Frame::Simple("hlen".to_string()),
        Frame::Bulk(Bytes::from("myhash_hlen")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer 1"),
    }

    // HSET myhash field2 "World"
    let cmd = Frame::Array(vec![
        Frame::Simple("hset".to_string()),
        Frame::Bulk(Bytes::from("myhash_hlen")),
        Frame::Bulk(Bytes::from("field2")),
        Frame::Bulk(Bytes::from("World")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // HLEN myhash -> 2
    let cmd = Frame::Array(vec![
        Frame::Simple("hlen".to_string()),
        Frame::Bulk(Bytes::from("myhash_hlen")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 2),
        _ => panic!("Expected Integer 2"),
    }
}

#[tokio::test]
async fn test_hash_advanced() {
    let mut client = get_client().await;

    // HSET myhash field1 "v1"
    let cmd = Frame::Array(vec![
        Frame::Simple("hset".to_string()),
        Frame::Bulk(Bytes::from("myhash_adv")),
        Frame::Bulk(Bytes::from("field1")),
        Frame::Bulk(Bytes::from("v1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // HSET myhash field2 "v2"
    let cmd = Frame::Array(vec![
        Frame::Simple("hset".to_string()),
        Frame::Bulk(Bytes::from("myhash_adv")),
        Frame::Bulk(Bytes::from("field2")),
        Frame::Bulk(Bytes::from("v2")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    client.read_frame().await.unwrap();

    // HEXISTS myhash field1 -> 1
    let cmd = Frame::Array(vec![
        Frame::Simple("hexists".to_string()),
        Frame::Bulk(Bytes::from("myhash_adv")),
        Frame::Bulk(Bytes::from("field1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer 1"),
    }

    // HDEL myhash field1 -> 1
    let cmd = Frame::Array(vec![
        Frame::Simple("hdel".to_string()),
        Frame::Bulk(Bytes::from("myhash_adv")),
        Frame::Bulk(Bytes::from("field1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 1),
        _ => panic!("Expected Integer 1"),
    }

    // HEXISTS myhash field1 -> 0
    let cmd = Frame::Array(vec![
        Frame::Simple("hexists".to_string()),
        Frame::Bulk(Bytes::from("myhash_adv")),
        Frame::Bulk(Bytes::from("field1")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Integer(n) => assert_eq!(n, 0),
        _ => panic!("Expected Integer 0"),
    }

    // HKEYS myhash -> ["field2"]
    let cmd = Frame::Array(vec![
        Frame::Simple("hkeys".to_string()),
        Frame::Bulk(Bytes::from("myhash_adv")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Array(arr) => {
             assert_eq!(arr.len(), 1);
             match &arr[0] {
                 Frame::Bulk(b) => assert_eq!(std::str::from_utf8(b).unwrap(), "field2"),
                 _ => panic!("Expected Bulk"),
             }
        }
        _ => panic!("Expected Array"),
    }

    // HVALS myhash -> ["v2"]
    let cmd = Frame::Array(vec![
        Frame::Simple("hvals".to_string()),
        Frame::Bulk(Bytes::from("myhash_adv")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Array(arr) => {
             assert_eq!(arr.len(), 1);
             match &arr[0] {
                 Frame::Bulk(b) => assert_eq!(std::str::from_utf8(b).unwrap(), "v2"),
                 _ => panic!("Expected Bulk"),
             }
        }
        _ => panic!("Expected Array"),
    }

    // HSCAN myhash 0 -> [0, ["field2", "v2"]]
    let cmd = Frame::Array(vec![
        Frame::Simple("hscan".to_string()),
        Frame::Bulk(Bytes::from("myhash_adv")),
        Frame::Bulk(Bytes::from("0")),
    ]);
    client.write_frame(&cmd).await.unwrap();
    match client.read_frame().await.unwrap().unwrap() {
        Frame::Array(arr) => {
             assert_eq!(arr.len(), 2); // Cursor + Array
             // Check keys/values in second element
             match &arr[1] {
                 Frame::Array(kv) => {
                      assert_eq!(kv.len(), 2);
                 }
                 _ => panic!("Expected Array of KV"),
             }
        }
        _ => panic!("Expected Array [Cursor, Array]"),
    }
}
