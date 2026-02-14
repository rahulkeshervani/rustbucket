use tokio::net::TcpListener;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> rustbucket::Result<()> {
    // Set up logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    // Bind the listener to the address
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    info!("Listening on 127.0.0.1:6379");

    rustbucket::run(listener).await
}
