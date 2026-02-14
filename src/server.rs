use crate::{Command, Connection, Db};

use tokio::net::{TcpListener, TcpStream};
use tracing::{error, instrument};

/// Run the mini-redis server.
///
/// Accepts connections from the supplied listener. For each accepted
/// connection, processing is handled by a "handler" task.
///
/// The `Db` instance is shared across all tasks.
pub async fn run(listener: TcpListener) -> crate::Result<()> {
    let db = Db::new();

    loop {
        // Accept a new socket. This will return a `TcpStream` and the remote
        // peer's address.
        let (socket, _) = listener.accept().await?;

        // Clone the handle to the hash map.
        let db = db.clone();

        // Spawn a new task to process the connection.
        tokio::spawn(async move {
            // Process the connection. If an error is encountered, log it.
            if let Err(err) = process(socket, db).await {
                error!(cause = ?err, "connection error");
            }
        });
    }
}

/// Process a single connection.
///
/// Request frames are read from the socket and processed. Responses are
/// written back to the socket.
use bytes::Bytes;

struct TransactionState {
    queued: Vec<Command>,
    watched: Vec<(Bytes, u64)>,
    active: bool,
}

impl TransactionState {
    fn new() -> Self {
        TransactionState {
            queued: Vec::new(),
            watched: Vec::new(),
            active: false,
        }
    }
}

/// Process a single connection.
#[instrument(skip(socket, db))]
async fn process(socket: TcpStream, db: Db) -> crate::Result<()> {
    let mut connection = Connection::new(socket);
    let mut txn_state = TransactionState::new();

    while let Some(frame) = connection.read_frame().await? {
        let cmd = match Command::from_frame(frame) {
            Ok(cmd) => cmd,
            Err(err) => {
                let response = crate::Frame::Error(err.to_string());
                connection.write_frame(&response).await?;
                continue;
            }
        };

        match cmd {
            Command::Multi(_) => {
                if txn_state.active {
                    connection.write_frame(&crate::Frame::Error("ERR MULTI calls can not be nested".into())).await?;
                } else {
                    txn_state.active = true;
                    connection.write_frame(&crate::Frame::Simple("OK".into())).await?;
                }
            }
            Command::Discard(_) => {
                if !txn_state.active {
                     connection.write_frame(&crate::Frame::Error("ERR DISCARD without MULTI".into())).await?;
                } else {
                    txn_state.queued.clear();
                    txn_state.watched.clear();
                    txn_state.active = false;
                    connection.write_frame(&crate::Frame::Simple("OK".into())).await?;
                }
            }
            Command::Watch(ref watch_cmd) => {
                 // WATCH allowed inside active transaction? Redis says:
                 // "WATCH inside MULTI is not allowed" (ERR).
                 if txn_state.active {
                     connection.write_frame(&crate::Frame::Error("ERR WATCH inside MULTI is not allowed".into())).await?;
                 } else {
                     // Capture versions
                     // We need to lock to read versions?
                     // versions are atomic, but we might want consistent view?
                     // Relaxed ordering is fine as we just capture current state.
                     // A read lock on batch_lock might be good to ensure we don't watch in middle of another EXEC?
                     // Yes, treat WATCH like a read op.
                     {
                         let _guard = db.batch_lock.read().await;
                         for key in &watch_cmd.match_keys {
                             let shard_idx = db.get_shard_index(key);
                             let ver = db.get_shard_version(shard_idx);
                             txn_state.watched.retain(|(k, _)| k != key); // Replace if existing
                             txn_state.watched.push((key.clone().into(), ver));
                         }
                     }
                     connection.write_frame(&crate::Frame::Simple("OK".into())).await?;
                 }
            }
            Command::Exec(_) => {
                 if !txn_state.active {
                      connection.write_frame(&crate::Frame::Error("ERR EXEC without MULTI".into())).await?;
                 } else {
                      // 1. Acquire WRITE lock
                      let _guard = db.batch_lock.write().await;
                      
                      // 2. Validate watched keys
                      let mut valid = true;
                      for (key, ver) in &txn_state.watched {
                           let shard_idx = db.get_shard_index(key);
                           let current_ver = db.get_shard_version(shard_idx);
                           if current_ver != *ver {
                               valid = false;
                               break;
                           }
                      }
                      
                      if !valid {
                          // Transaction aborted
                          connection.write_frame(&crate::Frame::Null).await?; // Nil response for abort
                      } else {
                          // 3. Execute queued commands
                          // 3. Execute queued commands
                          
                          // We need to capture the output of each command.
                          // Command::apply writes to connection. We don't want that for EXEC?
                          // Redis EXEC returns Array of results.
                          // Our `apply` writes directly to `dst`.
                          // THIS IS A PROBLEM.
                          // `apply` currently writes to `connection`.
                          // If we run `apply`, it will write frames to `connection`.
                          // But we want to wrap them in an Array frame.
                          // And `apply` might write Errors, Integers, etc.
                          // Solution: Create a temporary buffer/Connection to capture output?
                          // `Connection` wraps a `TcpStream`. hard to mock.
                          
                          // Refactor: `apply` should return `Frame`?
                          // If I change `apply` signature to return `Frame`, it's a huge refactor.
                          
                          // Shortcut:
                          // `EXEC` writes `*N` (Array len).
                          // Then we invoke `apply` for each command.
                          // Each `apply` writes its result to the stream.
                          // Effectively streaming the Array content.
                          // This is VALID RESP. An Array is `*N\r\n` followed by N frames.
                          // So we can send `*N` header, then let commands write themselves.
                          // IF we don't fail in the middle.
                          // If a command fails (e.g. valid syntax but runtime error), it writes Error frame. That's fine in Array.
                          
                          // Wait, what if `apply` fails (returns Err)?
                          // Then we might have partial array.
                          // Redis transactions usually don't fail on parsing commands (checked at queue time).
                          // Runtime errors are sent as Error frames inside the array.
                          
                          // So:
                          // 1. Write `*Len`
                          // 2. Loop queued: run `apply`.
                          // 3. If `apply` returns Err (network error?), we are in trouble. But `apply` returns `crate::Result`.
                          // If network error, connection closes anyway.
                          
                          connection.start_array(txn_state.queued.len()).await?; 
                          for q_cmd in txn_state.queued.drain(..) {
                               if let Err(e) = q_cmd.apply(&db, &mut connection).await {
                                   return Err(e);
                               }
                          }
                      }
                      
                      // Cleanup
                      txn_state.queued.clear();
                      txn_state.watched.clear();
                      txn_state.active = false;
                 }
            }
            _ => {
                if txn_state.active {
                    txn_state.queued.push(cmd);
                    connection.write_frame(&crate::Frame::Simple("QUEUED".into())).await?;
                } else {
                    // Normal execution
                    // Acquire READ lock
                    let _guard = db.batch_lock.read().await;
                    cmd.apply(&db, &mut connection).await?;
                }
            }
        }
    }

    Ok(())
}
