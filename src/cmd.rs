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
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        parse.finish()?;

        Ok(command)
    }

    /// Apply the command to the specified `Db` instance.
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
            Ttl(cmd) => cmd.apply(db, dst).await, 
            Pttl(cmd) => cmd.apply(db, dst).await,
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

// RESTORED STRUCTS - Zero-Copy Key versions

#[derive(Debug)]
pub struct Get { key: Bytes }
impl Get {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> { Ok(Get { key: parse.next_bytes()? }) }
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = if let Some(value) = db.get(&self.key) { Frame::Bulk(value) } else { Frame::Null };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Set { key: Bytes, value: Bytes }
impl Set {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> { 
        Ok(Set { key: parse.next_bytes()?, value: parse.next_bytes()? }) 
    }
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.set(self.key, self.value);
        dst.write_frame(&Frame::Simple("OK".into())).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Del { key: Bytes }
impl Del {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> { Ok(Del { key: parse.next_bytes()? }) }
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let n = if db.delete(&self.key) { 1 } else { 0 };
        dst.write_frame(&Frame::Integer(n)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Ping { msg: Option<String> }
impl Ping {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ping> {
         match parse.next_string() { Ok(msg) => Ok(Ping { msg: Some(msg) }), Err(_) => Ok(Ping { msg: None }) }
    }
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = match self.msg { None => Frame::Simple("PONG".into()), Some(msg) => Frame::Bulk(Bytes::from(msg)) };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Auth { password: String, username: Option<String> }
impl Auth {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Auth> {
        let first = parse.next_string()?;
        match parse.next_string() {
            Ok(second) => Ok(Auth { username: Some(first), password: second }),
            Err(_) => Ok(Auth { username: None, password: first }),
        }
    }
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> { dst.write_frame(&Frame::Simple("OK".into())).await?; Ok(()) }
}

#[derive(Debug)]
pub struct Info { _section: Option<String> }
impl Info {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Info> {
        match parse.next_string() { Ok(s) => Ok(Info { _section: Some(s) }), Err(_) => Ok(Info { _section: None }) }
    }
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let msg = "role:master\r\nconnected_clients:1\r\nredis_version:0.1.0\r\n";
        dst.write_frame(&Frame::Bulk(Bytes::from(msg))).await?;
        Ok(())
    }
}


#[derive(Debug)]
pub struct Exists {
    key: Bytes,
}

impl Exists {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Exists> {
        let key = parse.next_bytes()?;
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
    key: Bytes,
    field: Bytes,
    value: Bytes,
}

impl HSet {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HSet> {
        let key = parse.next_bytes()?;
        let field = parse.next_bytes()?;
        let value = parse.next_bytes()?;
        Ok(HSet { key, field, value })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let result = db.hset(self.key, self.field, self.value);
        dst.write_frame(&Frame::Integer(result as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HGet {
    key: Bytes,
    field: Bytes,
}

impl HGet {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HGet> {
        let key = parse.next_bytes()?;
        let field = parse.next_bytes()?;
        Ok(HGet { key, field })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.hget(&self.key, &self.field) {
            Some(val) => Frame::Bulk(val),
            None => Frame::Null,
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HDel {
    key: Bytes,
    field: Bytes,
}

impl HDel {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HDel> {
        let key = parse.next_bytes()?;
        let field = parse.next_bytes()?;
        Ok(HDel { key, field })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
       let result = db.hdel(&self.key, &self.field);
       dst.write_frame(&Frame::Integer(result as i64)).await?;
       Ok(())
    }
}

#[derive(Debug)]
pub struct HExists {
    key: Bytes,
    field: Bytes,
}

impl HExists {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HExists> {
        let key = parse.next_bytes()?;
        let field = parse.next_bytes()?;
        Ok(HExists { key, field })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let result = db.hexists(&self.key, &self.field);
        dst.write_frame(&Frame::Integer(result as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HGetAll {
    key: Bytes,
}

impl HGetAll {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HGetAll> {
        let key = parse.next_bytes()?;
        Ok(HGetAll { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let response = match db.hgetall(&self.key) {
            Some(h) => {
                let mut frames = Vec::new();
                for (k, v) in h {
                    frames.push(Frame::Bulk(k));
                    frames.push(Frame::Bulk(v));
                }
                Frame::Array(frames)
            }
            None => Frame::Array(vec![]),
        };
        dst.write_frame(&response).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HKeys {
    key: Bytes,
}

impl HKeys {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HKeys> {
        let key = parse.next_bytes()?;
        Ok(HKeys { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let keys = db.hkeys(&self.key);
        let mut frames = Vec::new();
        for k in keys {
            frames.push(Frame::Bulk(k));
        }
        dst.write_frame(&Frame::Array(frames)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HVals {
    key: Bytes,
}

impl HVals {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HVals> {
        let key = parse.next_bytes()?;
        Ok(HVals { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
         let vals = db.hvals(&self.key);
         let mut frames = Vec::new();
         for v in vals {
             frames.push(Frame::Bulk(v));
         }
         dst.write_frame(&Frame::Array(frames)).await?;
         Ok(())
    }
}


#[derive(Debug)]
pub struct HLen {
    key: Bytes,
    _field: String,
} 
impl HLen {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HLen> {
        let key = parse.next_bytes()?;
        Ok(HLen { key, _field: String::new() }) 
    }
    
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let len = db.hlen(&self.key);
        dst.write_frame(&Frame::Integer(len as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct HScan {
    key: Bytes,
    _cursor: u64,
}

impl HScan {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<HScan> { 
        let key = parse.next_bytes()?;
        let cursor_str = parse.next_string()?;
        Ok(HScan { key, _cursor: cursor_str.parse().unwrap_or(0) }) 
    }
    
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        if let Some(map) = db.hgetall(&self.key) {
              let mut frames = Vec::new();
                for (k, v) in map {
                    frames.push(Frame::Bulk(k));
                    frames.push(Frame::Bulk(v));
                }
                let result = vec![
                    Frame::Bulk(Bytes::from("0")),
                    Frame::Array(frames),
                ];
                dst.write_frame(&Frame::Array(result)).await?;
        } else {
             let result = vec![
                Frame::Bulk(Bytes::from("0")),
                Frame::Array(vec![]),
            ];
            dst.write_frame(&Frame::Array(result)).await?;
        }
        Ok(())
    }
}


// Arrays / Lists
#[derive(Debug)]
pub struct LPush {
    key: Bytes,
    values: Vec<Bytes>,
}

impl LPush {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<LPush> {
        let key = parse.next_bytes()?;
        let mut values = Vec::new();
        // Support variadic arguments
        while let Ok(val) = parse.next_bytes() {
            values.push(val);
        }
        Ok(LPush { key, values })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut len = 0;
        for val in self.values {
            len = db.lpush(self.key.clone(), val);
        }
        dst.write_frame(&Frame::Integer(len as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct RPush {
    key: Bytes,
    values: Vec<Bytes>,
}

impl RPush {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<RPush> {
        let key = parse.next_bytes()?;
        let mut values = Vec::new();
        while let Ok(val) = parse.next_bytes() {
            values.push(val);
        }
        Ok(RPush { key, values })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut len = 0;
        for val in self.values {
            len = db.rpush(self.key.clone(), val);
        }
        dst.write_frame(&Frame::Integer(len as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct LPop {
    key: Bytes,
}

impl LPop {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<LPop> {
        let key = parse.next_bytes()?;
        Ok(LPop { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        match db.lpop(&self.key) {
            Some(val) => dst.write_frame(&Frame::Bulk(val)).await?,
            None => dst.write_frame(&Frame::Null).await?,
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct RPop {
    key: Bytes,
}

impl RPop {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<RPop> {
        let key = parse.next_bytes()?;
        Ok(RPop { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        match db.rpop(&self.key) {
            Some(val) => dst.write_frame(&Frame::Bulk(val)).await?,
            None => dst.write_frame(&Frame::Null).await?,
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct LRange {
    key: Bytes,
    start: i64,
    stop: i64,
}

impl LRange {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<LRange> {
        let key = parse.next_bytes()?;
        let start = parse.next_int()?;
        let stop = parse.next_int()?;
        Ok(LRange { key, start, stop })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let values = db.lrange(&self.key, self.start, self.stop);
        let mut frames = Vec::new();
        for v in values {
            frames.push(Frame::Bulk(v));
        }
        dst.write_frame(&Frame::Array(frames)).await?;
        Ok(())
    }
}


#[derive(Debug)]
pub struct SAdd {
    key: Bytes,
    members: Vec<Bytes>,
}

impl SAdd {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SAdd> {
        let key = parse.next_bytes()?;
        let mut members = Vec::new();
        while let Ok(val) = parse.next_bytes() {
            members.push(val);
        }
        Ok(SAdd { key, members })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut count = 0;
        for member in self.members {
             count += db.sadd(self.key.clone(), member);
        }
        dst.write_frame(&Frame::Integer(count as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SMembers {
    key: Bytes,
}

impl SMembers {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SMembers> {
        let key = parse.next_bytes()?;
        Ok(SMembers { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let members = db.smembers(&self.key);
        let mut frames = Vec::new();
        for m in members {
            frames.push(Frame::Bulk(m));
        }
        dst.write_frame(&Frame::Array(frames)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct SRem {
    key: Bytes,
    members: Vec<Bytes>,
}

impl SRem {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<SRem> {
        let key = parse.next_bytes()?;
        let mut members = Vec::new();
        while let Ok(val) = parse.next_bytes() {
            members.push(val);
        }
        Ok(SRem { key, members })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut count = 0;
        for member in self.members {
            count += db.srem(&self.key, &member);
        }
        dst.write_frame(&Frame::Integer(count as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ZAdd {
    key: Bytes,
    elements: Vec<(f64, Bytes)>,
}

impl ZAdd {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<ZAdd> {
        let key = parse.next_bytes()?;
        let mut elements = Vec::new();
        while let Ok(score_str) = parse.next_string() {
            let score = score_str.parse::<f64>().map_err(|_| "ERR value is not a valid float")?;
            let member = parse.next_bytes()?;
            elements.push((score, member));
        }
        Ok(ZAdd { key, elements })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let mut count = 0;
        for (score, member) in self.elements {
            count += db.zadd(self.key.clone(), score, member);
        }
        dst.write_frame(&Frame::Integer(count as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct ZRange {
    key: Bytes,
    start: i64,
    stop: i64,
    with_scores: bool,
}

impl ZRange {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<ZRange> {
        let key = parse.next_bytes()?;
        let start = parse.next_int()?;
        let stop = parse.next_int()?;
        let mut with_scores = false;
        if let Ok(arg) = parse.next_string() {
            if arg.to_lowercase() == "withscores" {
                with_scores = true;
            }
        }
        Ok(ZRange { key, start, stop, with_scores })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let values = db.zrange(&self.key, self.start, self.stop, self.with_scores);
        let mut frames = Vec::new();
        for (member, score) in values {
            frames.push(Frame::Bulk(member));
            if self.with_scores {
                frames.push(Frame::Bulk(Bytes::from(score.to_string())));
            }
        }
        dst.write_frame(&Frame::Array(frames)).await?;
        Ok(())
    }
}


#[derive(Debug)]
pub struct JsonSet {
    key: Bytes,
    path: String,
    value: String,
}

impl JsonSet {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<JsonSet> {
        let key = parse.next_bytes()?;
        let path = parse.next_string()?;
        let value = parse.next_string()?;
        Ok(JsonSet { key, path, value })
    }
     pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
         let mut json_val = match db.get_value_clone(&self.key) {
             Some(DataType::Json(v)) => v,
             None => serde_json::Value::Null,
             _ => {
                  dst.write_frame(&Frame::Error("WRONGTYPE".into())).await?;
                  return Ok(());
             }
         };
         
         if self.path == "$" {
              if let Ok(v) = serde_json::from_str(&self.value) {
                  db.set_value(self.key, DataType::Json(v));
                  dst.write_frame(&Frame::Simple("OK".into())).await?;
              } else {
                   dst.write_frame(&Frame::Error("ERR invalid json".into())).await?;
              }
         } else {
            dst.write_frame(&Frame::Error("ERR only root path $ supported for now".into())).await?;
         }
         Ok(())
    }
}

#[derive(Debug)]
pub struct JsonGet {
    key: Bytes,
    path: String,
}

impl JsonGet {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<JsonGet> {
        let key = parse.next_bytes()?;
        let path = parse.next_string()?;
        Ok(JsonGet { key, path })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        if let Some(DataType::Json(v)) = db.get_value_clone(&self.key) {
             if self.path == "$" {
                 let s = v.to_string();
                 dst.write_frame(&Frame::Bulk(Bytes::from(s))).await?;
             } else {
                 dst.write_frame(&Frame::Null).await?;
             }
        } else {
             dst.write_frame(&Frame::Null).await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Keys {
    pattern: String,
}

impl Keys {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Keys> {
        let pattern = parse.next_string()?;
        Ok(Keys { pattern })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // Warning: This is O(N) over all keys
        let keys = db.keys();
        let mut frames = Vec::new();
        
        // Simple pattern matching (only support "*")
        let match_all = self.pattern == "*";
        
        for k in keys {
            if match_all {
                frames.push(Frame::Bulk(k));
            } else {
                // Convert bytes to string for pattern match (expensive but compatible)
                // If invalid utf8, skip? or include? Redis matches bytes.
                // Assuming keys are utf8 for pattern matching for now.
                if let Ok(s) = String::from_utf8(k.to_vec()) {
                    if s == self.pattern {
                         frames.push(Frame::Bulk(k));
                    }
                }
            }
        }
        dst.write_frame(&Frame::Array(frames)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Type {
    key: Bytes,
}

impl Type {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Type> {
        let key = parse.next_bytes()?;
        Ok(Type { key })
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        let t = match db.get_value_clone(&self.key) {
            Some(DataType::String(_)) => "string",
            Some(DataType::List(_)) => "list",
            Some(DataType::Set(_)) => "set",
            Some(DataType::Hash(_)) => "hash",
            Some(DataType::ZSet(_)) => "zset",
            Some(DataType::Json(_)) => "ReJSON-RL",
            None => "none",
        };
       dst.write_frame(&Frame::Simple(t.into())).await?;
       Ok(())
    }
}

#[derive(Debug)]
pub struct DbSize {}
impl DbSize {
    pub(crate) fn parse_frames(_parse: &mut Parse) -> crate::Result<DbSize> { Ok(DbSize {}) }
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        dst.write_frame(&Frame::Integer(db.len() as i64)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct FlushDb {}
impl FlushDb {
    pub(crate) fn parse_frames(_parse: &mut Parse) -> crate::Result<FlushDb> { Ok(FlushDb {}) }
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.clear();
        dst.write_frame(&Frame::Simple("OK".into())).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Ttl { key: Bytes }
impl Ttl {
     pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Ttl> { Ok(Ttl { key: parse.next_bytes()? }) }
     pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
         if db.exists(&self.key) {
             dst.write_frame(&Frame::Integer(-1)).await?; // No expiry support yet
         } else {
             dst.write_frame(&Frame::Integer(-2)).await?;
         }
         Ok(())
     }
}

#[derive(Debug)]
pub struct Pttl { key: Bytes }
impl Pttl {
     pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Pttl> { Ok(Pttl { key: parse.next_bytes()? }) }
     pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
         if db.exists(&self.key) {
             dst.write_frame(&Frame::Integer(-1)).await?; 
         } else {
             dst.write_frame(&Frame::Integer(-2)).await?;
         }
         Ok(())
     }
}

#[derive(Debug)]
pub struct Scan { _cursor: u64 }
impl Scan {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Scan> {
        let _cursor = parse.next_string()?;
        Ok(Scan { _cursor: 0 })
    }
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
         // Full Scan O(N) for now
         let keys = db.keys();
         let mut frames = Vec::new();
         for k in keys {
             frames.push(Frame::Bulk(k));
         }
         let result = vec![
            Frame::Bulk(Bytes::from("0")),
            Frame::Array(frames),
         ];
         dst.write_frame(&Frame::Array(result)).await?;
         Ok(())
    }
}


#[derive(Debug)]
pub struct Select { _db: i64 }
impl Select {
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Select> {
        Ok(Select { _db: parse.next_int()? })
    }
    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        dst.write_frame(&Frame::Simple("OK".into())).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }

    pub async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));
        dst.write_frame(&response).await?;
        Ok(())
    }
}

/// Utility for extracting data from a `Frame` array.
pub(crate) struct Parse {
    parts: std::vec::IntoIter<Frame>,
}

impl Parse {
    pub(crate) fn new(frame: Frame) -> crate::Result<Parse> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    pub(crate) fn next(&mut self) -> crate::Result<Frame> {
        self.parts.next().ok_or_else(|| "protocol error; end of stream".into())
    }

    pub(crate) fn next_string(&mut self) -> crate::Result<String> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => std::str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!("protocol error; expected simple frame or bulk frame, got {:?}", frame).into()),
        }
    }
    
    pub(crate) fn next_bytes(&mut self) -> crate::Result<Bytes> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!("protocol error; expected simple frame or bulk frame, got {:?}", frame).into()),
        }
    }

    pub(crate) fn next_int(&mut self) -> crate::Result<i64> {
        use atoi::atoi;

        const MSG: &str = "protocol error; invalid number";

        match self.next()? {
            Frame::Integer(v) => Ok(v),
            Frame::Simple(data) => atoi::<i64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<i64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error; expected int frame but got {:?}", frame).into()),
        }
    }

    pub(crate) fn finish(&mut self) -> crate::Result<()> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame".into())
        }
    }
}
