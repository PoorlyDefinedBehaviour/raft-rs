use std::time::Duration;

use tonic::transport::Server;
use tracing::info;
use tracing_bunyan_formatter::{BunyanFormattingLayer, JsonStorageLayer};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

use crate::raft::{Config, Raft};

mod raft;
#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  std::env::set_var(
    "RUST_LOG",
    std::env::var("RUST_LOG").unwrap_or_else(|_| format!("{}=trace", env!("CARGO_PKG_NAME"))),
  );

  dotenv::dotenv().ok();

  let (non_blocking_writer, _guard) = tracing_appender::non_blocking(std::io::stdout());

  let app_name = concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION")).to_string();

  let bunyan_formatting_layer = BunyanFormattingLayer::new(app_name, non_blocking_writer);

  let subscriber = Registry::default()
    .with(JsonStorageLayer)
    .with(bunyan_formatting_layer);

  tracing::subscriber::set_global_default(subscriber).unwrap();

  // ADDRESS=127.0.0.1:5000 PEERS=http://127.0.0.1:5001,http://127.0.0.1:5002 cargo r
  // ADDRESS=127.0.0.1:5001 PEERS=http://127.0.0.1:5000,http://127.0.0.1:5002 cargo r
  // ADDRESS=127.0.0.1:5002 PEERS=http://127.0.0.1:5000,http://127.0.0.1:5001 cargo r

  let address = std::env::var("ADDRESS").unwrap().parse()?;
  let peers = std::env::var("PEERS")
    .unwrap()
    .split(",")
    .map(|s| s.to_owned())
    .collect();

  info!(?address, ?peers, "starting server");

  Server::builder()
    .add_service(raft::raft_server::RaftServer::new(Raft::new(Config {
      min_heartbeat_timeout: Duration::from_millis(2000),
      min_election_timeout: Duration::from_millis(1500),
      send_heartbeat_timeout: Duration::from_millis(1000),
      servers: peers,
    })))
    .serve(address)
    .await?;

  Ok(())
}
