use dotenv::dotenv;

mod raft;

fn main() {
  std::env::set_var(
    "RUST_LOG",
    std::env::var("RUST_LOG").unwrap_or_else(|_| String::from("raft=trace")),
  );

  dotenv().ok();

  tracing_subscriber::fmt::init();

  dbg!(raft::RequestVoteRequest::default());
  dbg!(raft::AppendEntriesRequest::default());
}
