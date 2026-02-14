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
#[instrument(skip(socket, db))]
async fn process(socket: TcpStream, db: Db) -> crate::Result<()> {
    // The `Connection` lets us read/write redis **frames** instead of
    // byte streams. The `Connection` type is defined by mini-redis.
    let mut connection = Connection::new(socket);

    // While there is no error, read frames from the connection.
    //
    // The `read_frame` method returns `None` when the peer closes
    // the socket.
    while let Some(frame) = connection.read_frame().await? {
        let response = match Command::from_frame(frame) {
            Ok(cmd) => {
                // info!(?cmd, "received command"); // Performance bottleneck
                // Perform the work
                cmd.apply(&db, &mut connection).await
            }
            Err(err) => {
                // The command is not recognized or invalid.
                let response = crate::Frame::Error(err.to_string());
                connection.write_frame(&response).await.map_err(|e| e.into())
            }
        };

        if let Err(err) = response {
            error!(cause = ?err, "failed to write response");
            return Err(err);
        }
    }

    Ok(())
}
