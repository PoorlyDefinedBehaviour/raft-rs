use std::time::Duration;

use dotenv::dotenv;
use tokio::task::JoinHandle;
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

  // ADDRESS=127.0.0.1:5000 PEERS=http://127.0.0.1:5001,http://127.0.0.1:5002 cargo r
  // ADDRESS=127.0.0.1:5001 PEERS=http://127.0.0.1:5000,http://127.0.0.1:5002 cargo r
  // ADDRESS=127.0.0.1:5002 PEERS=http://127.0.0.1:5000,http://127.0.0.1:5001 cargo r

  start_raft_server(
    std::env::var("ADDRESS").unwrap(),
    std::env::var("PEERS")
      .unwrap()
      .split(",")
      .map(|s| s.to_owned())
      .collect(),
  )
  .await?;
  /*
  let _result = tokio::join!(
    start_raft_server(
      String::from("127.0.0.1:5000"),
      vec![
        String::from("http://127.0.0.1:5001"),
        // String::from("http://127.0.0.1:5002"),
        // String::from("http://127.0.0.1:5003"),
        // String::from("http://127.0.0.1:5004)"),
      ]
    ),
    start_raft_server(
      String::from("127.0.0.1:5001"),
      vec![
        String::from("http://127.0.0.1:5000"),
        // String::from("http://127.0.0.1:5002"),
        // String::from("http://127.0.0.1:5003"),
        // String::from("http://127.0.0.1:5004)"),
      ]
    ),
    /*   start_raft_server(
      String::from("127.0.0.1:5002"),
      vec![
        String::from("http://127.0.0.1:5001"),
        String::from("http://127.0.0.1:5000"),
        String::from("http://127.0.0.1:5003"),
        String::from("http://127.0.0.1:5004)"),
      ]
    ),
    start_raft_server(
      String::from("127.0.0.1:5003"),
      vec![
        String::from("http://127.0.0.1:5001"),
        String::from("http://127.0.0.1:5002"),
        String::from("http://127.0.0.1:5000"),
        String::from("http://127.0.0.1:5004)"),
      ]
    ),
    start_raft_server(
      String::from("127.0.0.1:5004"),
      vec![
        String::from("http://127.0.0.1:5001"),
        String::from("http://127.0.0.1:5002"),
        String::from("http://127.0.0.1:5003"),
        String::from("http://127.0.0.1:5000)"),
      ]
    )*/
  );*/

  Ok(())
}

fn start_raft_server(address: String, servers: Vec<String>) -> JoinHandle<()> {
  info!("starting server: {}", &address);

  tokio::spawn(async move {
    Server::builder()
      .add_service(raft::raft_server::RaftServer::new(Raft::new(Config {
        min_heartbeat_timeout: Duration::from_millis(2000),
        min_election_timeout: Duration::from_millis(1500),
        send_heartbeat_timeout: Duration::from_millis(1000),
        servers,
      })))
      .serve(address.parse().unwrap())
      .await
      .unwrap();
  })
}
