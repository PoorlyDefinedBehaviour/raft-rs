use std::time::Duration;

use dotenv::dotenv;
use tonic::transport::Server;
use tracing::info;

use crate::raft::{Config, Raft};

mod raft;
#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  std::env::set_var(
    "RUST_LOG",
    std::env::var("RUST_LOG").unwrap_or_else(|_| String::from("raft=trace")),
  );

  dotenv().ok();

  tracing_subscriber::fmt::init();

  let address = format!("127.0.0.1:{}", std::env::var("PORT").unwrap()).parse()?;

  info!("starting server: {}", &address);

  Server::builder()
    .add_service(raft::raft_server::RaftServer::new(Raft::new(Config {
      min_heartbeat_timeout: Duration::from_millis(2000),
      min_election_timeout: Duration::from_millis(1500),
      send_heartbeat_timeout: Duration::from_millis(1000),
      servers: vec![],
    })))
    .serve(address)
    .await?;

  Ok(())
}
