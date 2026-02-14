use crate::{Connection, Db, Frame, Error};
use crate::db::DataType;
use serde_json;
use bytes::Bytes;
use std::str;
use tracing::{debug, instrument, warn};
use std::collections::{HashMap, HashSet};

/// Enumeration of supported Redis commands.
///
/// Methods called on `Command` are delegated to the command implementation.
#[derive(Debug)]
pub enum Command {
    Get(Get),
    Set(Set),
    Del(Del),
    Ping(Ping),
    Auth(Auth),
    Info(Info),
    Scan(Scan),
    Keys(Keys),
    Type(Type),
    DbSize(DbSize),
    FlushDb(FlushDb),
    Exists(Exists),
    HSet(HSet),
    HGet(HGet),
    HDel(HDel),
    HExists(HExists),
    HGetAll(HGetAll),
    HKeys(HKeys),
    HVals(HVals),
    HScan(HScan),
    HLen(HLen),
    LPush(LPush),
    RPush(RPush),
    LPop(LPop),
    RPop(RPop),
    LRange(LRange),
    SAdd(SAdd),
    SMembers(SMembers),
    SRem(SRem),
    JsonSet(JsonSet),
    JsonGet(JsonGet),
    ZAdd(ZAdd),
    ZRange(ZRange),
    Ttl(Ttl),
    Pttl(Pttl),
    Select(Select),
    Unknown(Unknown),
}

impl Command {
    /// Parse a `Command` from a received frame.
    ///
    /// The `Frame` must represent a Redis command supported by `mini-redis` and
    /// be an array frame.
    ///
    /// # Returns
    ///
    /// On success, the command value is returned.
    ///
    /// # Errors
    ///
    /// If the frame is not an array frame, or if the command is not supported,
    /// an error is returned.
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?;

        // The first frame in the array is the command name.
        let command_name = parse.next_string()?.to_lowercase();

        // Match the command name and delegate to the specific command parser.
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "del" => Command::Del(Del::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "auth" => Command::Auth(Auth::parse_frames(&mut parse)?),
            "info" => Command::Info(Info::parse_frames(&mut parse)?),
            "scan" => Command::Scan(Scan::parse_frames(&mut parse)?),
            "keys" => Command::Keys(Keys::parse_frames(&mut parse)?),
            "type" => Command::Type(Type::parse_frames(&mut parse)?),
            "dbsize" => Command::DbSize(DbSize::parse_frames(&mut parse)?),
            "flushdb" => Command::FlushDb(FlushDb::parse_frames(&mut parse)?),
            "exists" => Command::Exists(Exists::parse_frames(&mut parse)?),
            "hset" => Command::HSet(HSet::parse_frames(&mut parse)?),
            "hget" => Command::HGet(HGet::parse_frames(&mut parse)?),
            "hdel" => Command::HDel(HDel::parse_frames(&mut parse)?),
            "hexists" => Command::HExists(HExists::parse_frames(&mut parse)?),
            "hgetall" => Command::HGetAll(HGetAll::parse_frames(&mut parse)?),
            "hkeys" => Command::HKeys(HKeys::parse_frames(&mut parse)?),
            "hvals" => Command::HVals(HVals::parse_frames(&mut parse)?),
            "hscan" => Command::HScan(HScan::parse_frames(&mut parse)?),
            "hlen" => Command::HLen(HLen::parse_frames(&mut parse)?),
            "lpush" => Command::LPush(LPush::parse_frames(&mut parse)?),
            "rpush" => Command::RPush(RPush::parse_frames(&mut parse)?),
            "lpop" => Command::LPop(LPop::parse_frames(&mut parse)?),
            "rpop" => Command::RPop(RPop::parse_frames(&mut parse)?),
            "lrange" => Command::LRange(LRange::parse_frames(&mut parse)?),
            "sadd" => Command::SAdd(SAdd::parse_frames(&mut parse)?),
            "smembers" => Command::SMembers(SMembers::parse_frames(&mut parse)?),
            "srem" => Command::SRem(SRem::parse_frames(&mut parse)?),
            "json.set" => Command::JsonSet(JsonSet::parse_frames(&mut parse)?),
            "json.get" => Command::JsonGet(JsonGet::parse_frames(&mut parse)?),
            "zadd" => Command::ZAdd(ZAdd::parse_frames(&mut parse)?),
            "zrange" => Command::ZRange(ZRange::parse_frames(&mut parse)?),
            "ttl" => Command::Ttl(Ttl::parse_frames(&mut parse)?),
            "pttl" => Command::Pttl(Pttl::parse_frames(&mut parse)?),
            "select" => Command::Select(Select::parse_frames(&mut parse)?),
            _ => {
                // The command is not recognized, return an Unknown command.
                //
                // We return an `Unknown` command rather than an error to allow
                // the server to continue function and simply return an error
                // to the client.
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        // Check if there is any remaining data in the frame. If so, return an
        // error.
        parse.finish()?;

        Ok(command)
    }

    /// Apply the command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Del(cmd) => cmd.apply(db, dst).await,
            Ping(cmd) => cmd.apply(dst).await,
            Auth(cmd) => cmd.apply(dst).await,
            Info(cmd) => cmd.apply(dst).await,
            Scan(cmd) => cmd.apply(db, dst).await,
            Keys(cmd) => cmd.apply(db, dst).await,
            Type(cmd) => cmd.apply(db, dst).await,
            DbSize(cmd) => cmd.apply(db, dst).await,
            FlushDb(cmd) => cmd.apply(db, dst).await,
            Exists(cmd) => cmd.apply(db, dst).await,
            HSet(cmd) => cmd.apply(db, dst).await,
            HGet(cmd) => cmd.apply(db, dst).await,
            HDel(cmd) => cmd.apply(db, dst).await,
            HExists(cmd) => cmd.apply(db, dst).await,
            HGetAll(cmd) => cmd.apply(db, dst).await,
            HKeys(cmd) => cmd.apply(db, dst).await,
            HVals(cmd) => cmd.apply(db, dst).await,
            HScan(cmd) => cmd.apply(db, dst).await,
            HLen(cmd) => cmd.apply(db, dst).await,
            LPush(cmd) => cmd.apply(db, dst).await,
            RPush(cmd) => cmd.apply(db, dst).await,
            LPop(cmd) => cmd.apply(db, dst).await,
            RPop(cmd) => cmd.apply(db, dst).await,
            LRange(cmd) => cmd.apply(db, dst).await,
            SAdd(cmd) => cmd.apply(db, dst).await,
            SMembers(cmd) => cmd.apply(db, dst).await,
            SRem(cmd) => cmd.apply(db, dst).await,
            JsonSet(cmd) => cmd.apply(db, dst).await,
            JsonGet(cmd) => cmd.apply(db, dst).await,
            ZAdd(cmd) => cmd.apply(db, dst).await,
            ZRange(cmd) => cmd.apply(db, dst).await,
            Ttl(cmd) => cmd.apply(db, dst).await, // Ttl needs db to check key
            Pttl(cmd) => cmd.apply(db, dst).await, // Pttl needs db to check key
            Select(cmd) => cmd.apply(dst).await,
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }

    /// Returns the command name
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Set(_) => "set",
            Command::Del(_) => "del",
            Command::Ping(_) => "ping",
            Command::Auth(_) => "auth",
            Command::Info(_) => "info",
            Command::Scan(_) => "scan",
            Command::Keys(_) => "keys",
            Command::Type(_) => "type",
            Command::DbSize(_) => "dbsize",
            Command::FlushDb(_) => "flushdb",
            Command::Exists(_) => "exists",
            Command::HSet(_) => "hset",
            Command::HGet(_) => "hget",
            Command::HDel(_) => "hdel",
            Command::HExists(_) => "hexists",
            Command::HGetAll(_) => "hgetall",
            Command::HKeys(_) => "hkeys",
            Command::HVals(_) => "hvals",
            Command::HScan(_) => "hscan",
            Command::HLen(_) => "hlen",
            Command::LPush(_) => "lpush",
            Command::RPush(_) => "rpush",
            Command::LPop(_) => "lpop",
            Command::RPop(_) => "rpop",
            Command::LRange(_) => "lrange",
            Command::SAdd(_) => "sadd",
            Command::SMembers(_) => "smembers",
            Command::SRem(_) => "srem",
            Command::JsonSet(_) => "json.set",
            Command::JsonGet(_) => "json.get",
            Command::ZAdd(_) => "zadd",
            Command::ZRange(_) => "zrange",
            Command::Ttl(_) => "ttl",
            Command::Pttl(_) => "pttl",
            Command::Select(_) => "select",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}

#[derive(Debug)]
pub struct Exists {
    key: String,
}

impl Exists {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Exists> {
        let key = parse.next_string()?;
        Ok(Exists { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if db.exists(&self.key) {
            Frame::Integer(1)
        } else {
            Frame::Integer(0)
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HSet {
    key: String,
    field: String,
    value: Bytes,
}

impl HSet {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HSet> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        let value = parse.next_bytes()?;
        Ok(HSet { key, field, value })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut hash_map = match db.get_value(&self.key) {
            Some(DataType::Hash(h)) => h,
            None => HashMap::new(),
            _ => {
                dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                return Ok(());
            }
        };

        hash_map.insert(self.field, self.value);
        db.set_value(self.key, DataType::Hash(hash_map));

        dst.write_frame(&Frame::Integer(1)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HGet {
    key: String,
    field: String,
}

impl HGet {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HGet> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        Ok(HGet { key, field })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.get_value(&self.key) {
            Some(DataType::Hash(h)) => {
                if let Some(val) = h.get(&self.field) {
                    Frame::Bulk(val.clone())
                } else {
                    Frame::Null
                }
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Frame::Null,
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HDel {
    key: String,
    field: String,
}

impl HDel {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HDel> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        Ok(HDel { key, field })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.get_value(&self.key) {
            Some(DataType::Hash(mut h)) => {
                let removed = if h.remove(&self.field).is_some() { 1 } else { 0 };
                db.set_value(self.key, DataType::Hash(h)); // Build-back
                Frame::Integer(removed)
            },
            None => Frame::Integer(0),
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HExists {
    key: String,
    field: String,
}

impl HExists {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HExists> {
        let key = parse.next_string()?;
        let field = parse.next_string()?;
        Ok(HExists { key, field })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
         let response = match db.get_value(&self.key) {
            Some(DataType::Hash(h)) => {
                let exists = if h.contains_key(&self.field) { 1 } else { 0 };
                Frame::Integer(exists)
            },
            None => Frame::Integer(0),
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HGetAll {
    key: String,
}

impl HGetAll {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HGetAll> {
        let key = parse.next_string()?;
        Ok(HGetAll { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.get_value(&self.key) {
            Some(DataType::Hash(h)) => {
                let mut frames = Vec::new();
                for (k, v) in h {
                    frames.push(Frame::Bulk(Bytes::from(k)));
                    frames.push(Frame::Bulk(v));
                }
                Frame::Array(frames)
            }
            Some(_) => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
            None => Frame::Array(vec![]),
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HKeys {
    key: String,
}

impl HKeys {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HKeys> {
        let key = parse.next_string()?;
        Ok(HKeys { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.get_value(&self.key) {
            Some(DataType::Hash(h)) => {
                 let mut frames = Vec::new();
                 for k in h.keys() {
                     frames.push(Frame::Bulk(Bytes::from(k.clone())));
                 }
                 Frame::Array(frames)
            },
            None => Frame::Array(vec![]),
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HVals {
    key: String,
}

impl HVals {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HVals> {
        let key = parse.next_string()?;
        Ok(HVals { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.get_value(&self.key) {
            Some(DataType::Hash(h)) => {
                 let mut frames = Vec::new();
                 for v in h.values() {
                     frames.push(Frame::Bulk(v.clone()));
                 }
                 Frame::Array(frames)
            },
            None => Frame::Array(vec![]),
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HScan {
    key: String,
    cursor: u64,
    match_pattern: Option<String>,
    count: Option<usize>,
}

impl HScan {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HScan> {
        let key = parse.next_string()?;
        let cursor_str = parse.next_string()?;
        let cursor = cursor_str.parse::<u64>().unwrap_or(0);
        
        let mut match_pattern = None;
        let mut count = None;

        while let Ok(arg) = parse.next_string() {
            match arg.to_lowercase().as_str() {
                "match" => {
                    match_pattern = Some(parse.next_string()?);
                }
                "count" => {
                    let count_str = parse.next_string()?;
                    count = Some(count_str.parse::<usize>().unwrap_or(10));
                }
                _ => {}
            }
        }

        Ok(HScan {
            key,
            cursor,
            match_pattern,
            count,
        })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.get_value(&self.key) {
            Some(DataType::Hash(h)) => {
                let mut keys: Vec<String> = h.keys().cloned().collect();
                 // Filter keys if match pattern is provided
                if let Some(pattern) = &self.match_pattern {
                     let pattern = pattern.replace("*", "");
                     if !pattern.is_empty() {
                        keys.retain(|k| k.contains(&pattern));
                     }
                }
                
                let mut frames = Vec::new();
                for key in keys {
                    let val = h.get(&key).unwrap();
                     frames.push(Frame::Bulk(Bytes::from(key.clone())));
                     frames.push(Frame::Bulk(val.clone()));
                }
                 // Result is [cursor, [key1, value1, ...]]
                let result = vec![
                    Frame::Bulk(Bytes::from("0")), // Cursor 0 means done
                    Frame::Array(frames),
                ];
                Frame::Array(result)
            },
            None => {
                 let result = vec![
                    Frame::Bulk(Bytes::from("0")),
                    Frame::Array(vec![]),
                ];
                Frame::Array(result)
            },
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HLen {
    key: String,
}

impl HLen {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HLen> {
        let key = parse.next_string()?;
        Ok(HLen { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.get_value(&self.key) {
            Some(DataType::Hash(h)) => Frame::Integer(h.len() as i64),
            None => Frame::Integer(0),
            _ => Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string()),
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}


/// Get the value of key.
///
/// If the key does not exist the special value nil is returned. An error is
/// returned if the value stored at key is not a string, because GET only
/// handles string values.
#[derive(Debug)]
pub struct Get {
    /// Name of the key to get
    key: String,
}

impl Get {
    /// Create a new `Get` command which fetches `key`.
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    /// Read the `Get` command from the `Parse` structure.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        // The `GET` string has already been consumed. The next value is the
        // name of the key to get. If the next value is not a string or the
        // input is fully consumed, then an error is returned.
        let key = parse.next_string()?;

        Ok(Get { key })
    }

    /// Apply the `Get` command to the specified `Db` instance.
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Get the value from the shared database state
        let response = if let Some(value) = db.get(&self.key) {
            // If a value is present, it is written to the client in "bulk"
            // format.
            Frame::Bulk(value)
        } else {
            // If there is no value, `Null` is written.
            Frame::Null
        };

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// Set `key` to hold the string `value`.
///
/// If `key` already holds a value, it is overwritten, regardless of its type.
/// Any previous time to live associated with the key is discarded on
/// successful SET operation.
#[derive(Debug)]
pub struct Set {
    /// the lookup key
    key: String,

    /// the value to be stored
    value: Bytes,
}

impl Set {
    /// Create a new `Set` command which sets `key` to `value`.
    pub fn new(key: impl ToString, value: Bytes) -> Set {
        Set {
            key: key.to_string(),
            value,
        }
    }

    /// Read the `Set` command from the `Parse` structure.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        // Read the key to set.
        let key = parse.next_string()?;

        // Read the value to set.
        let value = parse.next_bytes()?;

        Ok(Set { key, value })
    }

    /// Apply the `Set` command to the specified `Db` instance.
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Set the value in the shared database state
        db.set(self.key, self.value);

        // Create a success response
        let response = Frame::Simple("OK".to_string());

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// Removes the specified keys. A key is ignored if it does not exist.
///
/// Return the number of keys that were removed.
#[derive(Debug)]
pub struct Del {
    /// key to remove
    key: String,
}

impl Del {
    /// Create a new `Del` command which removes `key`.
    pub fn new(key: impl ToString) -> Del {
        Del {
            key: key.to_string(),
        }
    }

    /// Read the `Del` command from the `Parse` structure.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        // Read the key to delete.
        let key = parse.next_string()?;

        Ok(Del { key })
    }

    /// Apply the `Del` command to the specified `Db` instance.
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Delete the value from the shared database state
        // For now, we only support deleting a single key
        let num_deleted = if db.delete(&self.key) { 1 } else { 0 };

        // Create a response
        let response = Frame::Integer(num_deleted);

        // Write the response back to the client
        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// Returns PONG if no argument is provided, otherwise return a copy of the argument as a bulk.
#[derive(Debug)]
pub struct Ping {
    /// optional message
    msg: Option<String>,
}

impl Ping {
    /// Create a new `Ping` command.
    pub fn new(msg: Option<String>) -> Ping {
        Ping { msg }
    }

    /// Read the `Ping` command from the `Parse` structure.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
        match parse.next_string() {
            Ok(msg) => Ok(Ping { msg: Some(msg) }),
            Err(_) => Ok(Ping { msg: None }),
        }
    }

    /// Apply the `Ping` command.
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg {
            None => Frame::Simple("PONG".to_string()),
            Some(msg) => Frame::Bulk(Bytes::from(msg)),
        };

        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// Request for authentication.
#[derive(Debug)]
pub struct Auth {
    /// The password
    password: String,
    /// The username (optional, Redis 6+ allows it)
    username: Option<String>,
}

impl Auth {
    /// Create a new `Auth` command.
    pub fn new(password: impl ToString) -> Auth {
        Auth {
            password: password.to_string(),
            username: None,
        }
    }

    /// Read the `Auth` command from the `Parse` structure.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Auth> {
        let first = parse.next_string()?;
        
        // AUTH can be `AUTH password` or `AUTH username password`
        match parse.next_string() {
            Ok(second) => Ok(Auth {
                username: Some(first),
                password: second,
            }),
            Err(_) => Ok(Auth {
                username: None,
                password: first,
            }),
        }
    }

    /// Apply the `Auth` command.
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        // We don't actually check password for now, just return OK
        // This is to allow clients that force AUTH to connect
        let response = Frame::Simple("OK".to_string());

        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// Returns server information.
#[derive(Debug)]
pub struct Info {
    /// optional section
    section: Option<String>,
}

impl Info {
    /// Create a new `Info` command.
    pub fn new(section: Option<String>) -> Info {
        Info { section }
    }

    /// Read the `Info` command from the `Parse` structure.
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Info> {
        match parse.next_string() {
            Ok(section) => Ok(Info { section: Some(section) }),
            Err(_) => Ok(Info { section: None }),
        }
    }

    /// Apply the `Info` command.
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let msg = "role:master\r\nconnected_clients:1\r\nredis_version:0.1.0\r\n";
        let response = Frame::Bulk(Bytes::from(msg));

        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// Iterate the set of keys in the current database.
#[derive(Debug)]
pub struct Scan {
    /// The cursor
    cursor: u64,
    /// The match pattern
    match_pattern: Option<String>,
    /// The count (hint)
    count: Option<usize>,
}

impl Scan {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Scan> {
        let cursor_str = parse.next_string()?;
        let cursor = cursor_str.parse::<u64>().unwrap_or(0);
        
        let mut match_pattern = None;
        let mut count = None;

        while let Ok(arg) = parse.next_string() {
            match arg.to_lowercase().as_str() {
                "match" => {
                    match_pattern = Some(parse.next_string()?);
                }
                "count" => {
                    let count_str = parse.next_string()?;
                    count = Some(count_str.parse::<usize>().unwrap_or(10));
                }
                _ => {}
            }
        }

        Ok(Scan {
            cursor,
            match_pattern,
            count,
        })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Ignore cursor and returns all keys for now
        // This is valid if we always return cursor "0" indicating scan is complete
        // Real implementation would need to handle cursor logic
        let mut keys = db.keys();
        
        // Filter keys if match pattern is provided
        if let Some(pattern) = &self.match_pattern {
            // Simple robust glob matching is hard without crate. 
            // We'll support standard '*' wildcard only for now, otherwise simple substring
            let pattern = pattern.replace("*", "");
            if !pattern.is_empty() {
               keys.retain(|k| k.contains(&pattern));
            }
        }

        // Convert keys to frames
        let mut frames = Vec::new();
        for key in keys {
            frames.push(Frame::Bulk(Bytes::from(key)));
        }

        // Result is [cursor, [key1, key2, ...]]
        let mut result = Vec::new();
        result.push(Frame::Bulk(Bytes::from("0"))); // New cursor (0 means done)
        result.push(Frame::Array(frames));

        dst.write_frame(&Frame::Array(result)).await?;

        Ok(())
    }
}

/// Find all keys matching the given pattern.
#[derive(Debug)]
pub struct Keys {
    /// The match pattern
    pattern: String,
}

impl Keys {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Keys> {
        let pattern = parse.next_string()?;
        Ok(Keys { pattern })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut keys = db.keys();
        
        // Simple filtering: if pattern isn't just "*", filter
        if self.pattern != "*" {
             let pattern = self.pattern.replace("*", "");
             if !pattern.is_empty() {
                keys.retain(|k| k.contains(&pattern));
             }
        }

        let mut frames = Vec::new();
        for key in keys {
            frames.push(Frame::Bulk(Bytes::from(key)));
        }

        dst.write_frame(&Frame::Array(frames)).await?;

        Ok(())
    }
}

/// Returns the string representation of the value's type.
#[derive(Debug)]
pub struct Type {
    /// The key
    key: String,
}

impl Type {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Type> {
        let key = parse.next_string()?;
        Ok(Type { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.get_value(&self.key) {
            Some(DataType::String(_)) => Frame::Simple("string".to_string()),
            Some(DataType::List(_)) => Frame::Simple("list".to_string()),
            Some(DataType::Set(_)) => Frame::Simple("set".to_string()),
            Some(DataType::Hash(_)) => Frame::Simple("hash".to_string()),
            Some(DataType::ZSet(_)) => Frame::Simple("zset".to_string()),
            Some(DataType::Json(_)) => Frame::Simple("ReJSON-RL".to_string()),
            None => Frame::Simple("none".to_string()),
        };

        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// Return the number of keys in the currently-selected database.
#[derive(Debug)]
pub struct DbSize;

impl DbSize {
    pub(crate) fn parse_frames(_parse: &mut Parse) -> crate::Result<DbSize> {
        Ok(DbSize)
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let len = db.len();
        dst.write_frame(&Frame::Integer(len as i64)).await?;
        Ok(())
    }
}

/// Delete all the keys of the currently selected DB.
#[derive(Debug)]
pub struct FlushDb;

impl FlushDb {
    pub(crate) fn parse_frames(_parse: &mut Parse) -> crate::Result<FlushDb> {
        Ok(FlushDb)
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.clear();
        dst.write_frame(&Frame::Simple("OK".to_string())).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LPush {
    key: String,
    values: Vec<Bytes>,
}

impl LPush {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<LPush> {
        let key = parse.next_string()?;
        let mut values = Vec::new();
        while let Ok(val) = parse.next_bytes() {
            values.push(val);
        }
        Ok(LPush { key, values })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut list = match db.get_value(&self.key) {
            Some(DataType::List(l)) => l,
            None => Vec::new(),
            _ => {
                dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                return Ok(());
            }
        };

        for val in self.values {
            list.insert(0, val);
        }
        let len = list.len();
        db.set_value(self.key, DataType::List(list));

        dst.write_frame(&Frame::Integer(len as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct RPush {
    key: String,
    values: Vec<Bytes>,
}

impl RPush {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<RPush> {
        let key = parse.next_string()?;
        let mut values = Vec::new();
        while let Ok(val) = parse.next_bytes() {
            values.push(val);
        }
        Ok(RPush { key, values })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut list = match db.get_value(&self.key) {
            Some(DataType::List(l)) => l,
            None => Vec::new(),
            _ => {
                dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                return Ok(());
            }
        };

        for val in self.values {
            list.push(val);
        }
        let len = list.len();
        db.set_value(self.key, DataType::List(list));

        dst.write_frame(&Frame::Integer(len as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LPop {
    key: String,
}

impl LPop {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<LPop> {
        let key = parse.next_string()?;
        Ok(LPop { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut list = match db.get_value(&self.key) {
            Some(DataType::List(l)) => l,
            None => {
                 dst.write_frame(&Frame::Null).await?;
                 return Ok(());
            },
            _ => {
                dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                return Ok(());
            }
        };

        if list.is_empty() {
             dst.write_frame(&Frame::Null).await?;
        } else {
             let val = list.remove(0);
             db.set_value(self.key, DataType::List(list));
             dst.write_frame(&Frame::Bulk(val)).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct RPop {
    key: String,
}

impl RPop {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<RPop> {
        let key = parse.next_string()?;
        Ok(RPop { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut list = match db.get_value(&self.key) {
             Some(DataType::List(l)) => l,
             None => {
                 dst.write_frame(&Frame::Null).await?;
                 return Ok(());
             },
             _ => {
                 dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                 return Ok(());
             }
        };

        if let Some(val) = list.pop() {
             db.set_value(self.key, DataType::List(list));
             dst.write_frame(&Frame::Bulk(val)).await?;
        } else {
             dst.write_frame(&Frame::Null).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct LRange {
    key: String,
    start: i64,
    stop: i64,
}

impl LRange {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<LRange> {
        let key = parse.next_string()?;
        let start = parse.next_int()? as i64;
        let stop = parse.next_int()? as i64;
        Ok(LRange { key, start, stop })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let list = match db.get_value(&self.key) {
             Some(DataType::List(l)) => l,
             None => {
                 dst.write_frame(&Frame::Array(vec![])).await?;
                 return Ok(());
             },
             _ => {
                 dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                 return Ok(());
             }
        };

        let len = list.len() as i64;
        let start = if self.start < 0 { len + self.start } else { self.start };
        let stop = if self.stop < 0 { len + self.stop } else { self.stop };

        let start = start.max(0) as usize;
        let stop = stop.max(0) as usize;
        
        let mut frames = Vec::new();
        if start < list.len() {
             let stop = (stop + 1).min(list.len());
             if start < stop {
                 for i in start..stop {
                      frames.push(Frame::Bulk(list[i].clone()));
                 }
             }
        }

        dst.write_frame(&Frame::Array(frames)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SAdd {
    key: String,
    members: Vec<Bytes>,
}

impl SAdd {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SAdd> {
        let key = parse.next_string()?;
        let mut members = Vec::new();
        while let Ok(val) = parse.next_bytes() {
            members.push(val);
        }
        Ok(SAdd { key, members })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut set = match db.get_value(&self.key) {
            Some(DataType::Set(s)) => s,
            None => HashSet::new(),
            _ => {
                dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                return Ok(());
            }
        };

        let mut added = 0;
        for member in self.members {
             if set.insert(member) {
                 added += 1;
             }
        }
        db.set_value(self.key, DataType::Set(set));
        dst.write_frame(&Frame::Integer(added as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SMembers {
    key: String,
}

impl SMembers {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SMembers> {
        let key = parse.next_string()?;
        Ok(SMembers { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
         let set = match db.get_value(&self.key) {
            Some(DataType::Set(s)) => s,
            None => HashSet::new(),
            _ => {
                dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                return Ok(());
            }
        };

        let mut frames = Vec::new();
        for member in set {
             frames.push(Frame::Bulk(member));
        }
        dst.write_frame(&Frame::Array(frames)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SRem {
    key: String,
    members: Vec<Bytes>,
}

impl SRem {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SRem> {
        let key = parse.next_string()?;
        let mut members = Vec::new();
        while let Ok(val) = parse.next_bytes() {
            members.push(val);
        }
        Ok(SRem { key, members })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut set = match db.get_value(&self.key) {
            Some(DataType::Set(s)) => s,
            None => {
                 dst.write_frame(&Frame::Integer(0)).await?;
                 return Ok(());
            },
            _ => {
                dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                return Ok(());
            }
        };

        let mut removed = 0;
        for member in self.members {
             if set.remove(&member) {
                 removed += 1;
             }
        }
        db.set_value(self.key, DataType::Set(set));
        dst.write_frame(&Frame::Integer(removed as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct JsonSet {
    key: String,
    path: String,
    value: String,
}

impl JsonSet {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<JsonSet> {
        let key = parse.next_string()?;
        let path = parse.next_string()?; // currently ignored or simple check
        let value = parse.next_string()?;
        Ok(JsonSet { key, path, value })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Parse JSON
        let json_val: serde_json::Value = match serde_json::from_str(&self.value) {
            Ok(v) => v,
            Err(_) => {
                 dst.write_frame(&Frame::Error("ERR invalid json".to_string())).await?;
                 return Ok(());
            }
        };
        
        // For MVP, we ignore path if it's new (overwrite logic) or implement simple root set
        if self.path != "$" && self.path != "." {
             // For MVP, we only support root set.
        }

        db.set_value(self.key, DataType::Json(json_val));
        dst.write_frame(&Frame::Simple("OK".to_string())).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct JsonGet {
    key: String,
    path: Option<String>,
}

impl JsonGet {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<JsonGet> {
        let key = parse.next_string()?;
        let path = match parse.next_string() {
            Ok(p) => Some(p),
            Err(_) => None,
        };
        Ok(JsonGet { key, path })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let val = match db.get_value(&self.key) {
            Some(DataType::Json(v)) => v,
            None => {
                 dst.write_frame(&Frame::Null).await?;
                 return Ok(());
            },
            _ => {
                dst.write_frame(&Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".to_string())).await?;
                return Ok(());
            }
        };

        let output = if let Some(_path) = self.path {
             // Path filtering not implemented yet
             val.to_string()
        } else {
             val.to_string()
        };

        dst.write_frame(&Frame::Bulk(Bytes::from(output))).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ZAdd {
    key: String,
    elements: Vec<(f64, String)>,
}

impl ZAdd {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<ZAdd> {
        let key = parse.next_string()?;
        let mut elements = Vec::new();
        // Loop: score, member
        while let Ok(score_str) = parse.next_string() {
             let score = score_str.parse::<f64>().unwrap_or(0.0);
             let member = parse.next_string()?;
             elements.push((score, member));
        }
        Ok(ZAdd { key, elements })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut zset = match db.get_value(&self.key) {
            Some(DataType::ZSet(z)) => z,
            None => HashMap::new(),
            _ => {
                dst.write_frame(&Frame::Error("WRONGTYPE".to_string())).await?;
                return Ok(());
            }
        };

        let mut added = 0;
        for (score, member) in self.elements {
             if zset.insert(member, score).is_none() {
                 added += 1;
             }
        }
        db.set_value(self.key, DataType::ZSet(zset));
        dst.write_frame(&Frame::Integer(added as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ZRange {
    key: String,
    start: i64,
    stop: i64,
}

impl ZRange {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<ZRange> {
        let key = parse.next_string()?;
        let start = parse.next_int()? as i64;
        let stop = parse.next_int()? as i64;
        // Ignore WITHSCORES for now
        Ok(ZRange { key, start, stop })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let zset = match db.get_value(&self.key) {
             Some(DataType::ZSet(z)) => z,
             None => {
                 dst.write_frame(&Frame::Array(vec![])).await?;
                 return Ok(());
             },
             _ => {
                 dst.write_frame(&Frame::Error("WRONGTYPE".to_string())).await?;
                 return Ok(());
             }
        };

        // Convert to vec and sort
        let mut elements: Vec<(&String, &f64)> = zset.iter().collect();
        elements.sort_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal));

        let len = elements.len() as i64;
        let start = if self.start < 0 { len + self.start } else { self.start };
        let stop = if self.stop < 0 { len + self.stop } else { self.stop };

        let start = start.max(0) as usize;
        let stop = stop.max(0) as usize;
        
        let mut frames = Vec::new();
        if start < elements.len() {
             let stop = (stop + 1).min(elements.len());
             if start < stop {
                 for i in start..stop {
                      frames.push(Frame::Bulk(Bytes::from(elements[i].0.clone())));
                 }
             }
        }

        dst.write_frame(&Frame::Array(frames)).await?;
        Ok(())
    }
}

/// Represents an unknown command.
#[derive(Debug)]
pub struct Ttl {
    key: String,
}

impl Ttl {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ttl> {
        let key = parse.next_string()?;
        Ok(Ttl { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if db.exists(&self.key) {
            Frame::Integer(-1)
        } else {
            Frame::Integer(-2)
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Pttl {
    key: String,
}

impl Pttl {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Pttl> {
        let key = parse.next_string()?;
        Ok(Pttl { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if db.exists(&self.key) {
            Frame::Integer(-1)
        } else {
            Frame::Integer(-2)
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Select {
    db: i64,
}

impl Select {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Select> {
        let db = parse.next_int()?;
        Ok(Select { db })
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        dst.write_frame(&Frame::Simple("OK".to_string())).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    /// Create a new `Unknown` command.
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }

    /// Apply the `Unknown` command.
    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }

    /// Respond with an error.
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));

        dst.write_frame(&response).await?;

        Ok(())
    }
}

/// Utility for parsing a command from a `Frame`.
pub(crate) struct Parse {
    /// Iterator over the frame components
    parts: std::vec::IntoIter<Frame>,
}

impl Parse {
    /// Create a new `Parse` to parse the contents of `frame`.
    ///
    /// Returns `Err` if `frame` is not an array frame.
    pub(crate) fn new(frame: Frame) -> Result<Parse, String> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame)),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// Return the next integer.
    pub(crate) fn next_int(&mut self) -> Result<i64, String> {
        use Frame::*;

        match self.parts.next() {
            Some(Integer(i)) => Ok(i),
            Some(Simple(s)) => s.parse::<i64>().map_err(|_| "protocol error; invalid integer".into()),
            Some(Bulk(data)) => {
                let s = str::from_utf8(&data).map_err(|_| "protocol error; invalid utf8")?;
                s.parse::<i64>().map_err(|_| "protocol error; invalid integer".into())
            }
            None => Err("protocol error; unexpected end of frame".into()),
            _ => Err("protocol error; expected integer".into()),
        }
    }

    /// Return the next string.
    pub(crate) fn next_string(&mut self) -> Result<String, String> {
        match self.parts.next() {
            // Both `Simple` and `Bulk` representation may be strings. Strings
            // are parsed to UTF-8.
            Some(Frame::Simple(s)) => Ok(s),
            Some(Frame::Bulk(data)) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            None => Err("protocol error; unexpected end of frame".into()),
            _ => Err("protocol error; expected simple frame or bulk frame".into()),
        }
    }

    /// Return the next value as raw bytes.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, String> {
        match self.parts.next() {
            Some(Frame::Simple(s)) => Ok(Bytes::from(s.into_bytes())),
            Some(Frame::Bulk(data)) => Ok(data),
            None => Err("protocol error; unexpected end of frame".into()),
            _ => Err("protocol error; expected simple frame or bulk frame".into()),
        }
    }

    /// Ensure there are no more entries in the array
    pub(crate) fn finish(&mut self) -> Result<(), String> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame".into())
        }
    }
}
