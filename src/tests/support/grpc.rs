use std::time::Duration;

use tokio::net::TcpListener;
use tonic::transport::{Channel, Server};

use crate::raft;
use crate::raft::Raft;

/// Starts the grpc server in a random port and returns a client that will make requests to it.
///
/// This function is useful for tests that need the server to be running.
///
/// Takes Raft as argument because we may need to test the server when
/// the raft node is in a specific state. If you need to do that,
/// create a new Raft instance in the test, modify it and then pass it to
/// `start_server`.
pub async fn start_server(raft: Raft) -> raft::raft_client::RaftClient<Channel> {
  let svc = raft::raft_server::RaftServer::new(raft);

  let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
  let addr = listener.local_addr().unwrap();

  tokio::spawn(async move {
    Server::builder()
      .timeout(Duration::from_millis(5000))
      .add_service(svc)
      .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
      .await
      .unwrap();
  });

  raft::raft_client::RaftClient::connect(format!("http://{}", addr))
    .await
    .unwrap()
}
