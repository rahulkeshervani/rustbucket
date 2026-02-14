pub mod cmd;
pub mod connection;
pub mod db;
pub mod frame;
pub mod server;

pub use cmd::Command;
pub use connection::Connection;
pub use db::Db;
pub use frame::Frame;
pub use server::run;

/// A specialized `Result` type for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for this crate.
pub type Error = Box<dyn std::error::Error + Send + Sync>;
