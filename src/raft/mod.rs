use tonic::{Request, Response, Status};

// Include Rust files generated in build.rs
tonic::include_proto!("raft.v1");

#[derive(Debug)]
pub struct Raft {
  /// # Persistent state on all servers
  ///
  /// Latest term server has seen.
  ///
  /// Initialized to 0 on first boot.
  ///
  /// Increases monotonically.
  current_term: u64,
  /// Candidate id that received vote in current term.
  ///
  /// `voted_for` is None if the server has not voted for a candidate.
  voted_for: Option<u64>,
  /// Each entry contains a command for state machine and the term
  /// when entry was received by the leader.
  ///
  /// Note that the first index should be 1.
  logs: Vec<Log>,
  /// # Volatile state on all servers
  ///
  /// Index of highest log entry known to be committed.
  ///
  /// Initialized to 0.
  ///
  /// Increases monotonically.
  commitex_index: u64,
  /// Index of highest log entry applied to state machine.
  ///
  /// Initialized to 0.
  ///
  /// Increases monotonically.
  last_applied: u64,
  // TODO: use type state
  /// # Volatile state on leaders
  ///
  /// For each server, index of the next log entry
  /// to send for that server.
  ///
  /// Initialized to leader last log index + 1.
  next_index: Vec<u64>,
  /// For each server, index of highest log entry known to be replicated on the server.
  ///
  /// Initialized to 0.
  ///
  /// Increases monotonically.
  match_index: Vec<u64>,
}

impl Raft {
  pub fn new() -> Self {
    todo!()
  }
}

#[tonic::async_trait]
impl raft_server::Raft for Raft {
  async fn request_vote(
    &self,
    request: Request<RequestVoteRequest>,
  ) -> Result<Response<RequestVoteResponse>, Status> {
    let request = request.into_inner();
    panic!("TODO");
  }

  async fn append_entries(
    &self,
    request: Request<AppendEntriesRequest>,
  ) -> Result<Response<AppendEntriesResponse>, Status> {
    let request = request.into_inner();
    panic!("TODO");
  }
}
