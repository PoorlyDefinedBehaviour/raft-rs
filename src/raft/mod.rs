use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tracing::instrument;

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
  voted_for: Mutex<Option<u64>>,
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
  committed_index: u64,
  /// Index of highest log entry applied to state machine.
  ///
  /// Initialized to 0.
  ///
  /// Increases monotonically.
  last_applied_index: u64,
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
  #[instrument]
  pub fn new() -> Self {
    // TODO: handle boot from state persisted to persitent storage.
    Self {
      current_term: 0,
      voted_for: Mutex::new(None),
      logs: vec![],
      committed_index: 0,
      last_applied_index: 0,
      // TODO: next_index.len() should be qual to the amount of servers in the cluster.
      next_index: vec![],
      // TODO: match_index.len() should be qual to the amount of servers in the cluster.
      match_index: vec![],
    }
  }

  /// Returns true if the server decides to vote
  /// for the candidate to be the new cluster leader.
  async fn vote(&self, candidate: RequestVoteRequest) -> bool {
    // Candidate cannot become the cluster leader if its
    // current term is less than some other server.
    if candidate.term < self.current_term {
      return false;
    }

    let mut voted_for = self.voted_for.lock().await;

    // If the server has already voted for another candidate,
    // it cannot vote again.
    if voted_for.is_some() {
      return false;
    }

    // The candidate log must as up-to-date as the server's log, otherwise
    // it cannot become the cluster leader.
    if (candidate.last_log_index as usize) < self.logs.len() {
      return false;
    }

    // TODO: am i needed?
    if let Some(entry) = self.logs.last() {
      if entry.term != candidate.last_log_term {
        return false;
      }
    }

    *voted_for = Some(candidate.candidate_id);

    true
  }
}

#[tonic::async_trait]
impl raft_server::Raft for Raft {
  #[instrument]
  async fn request_vote(
    &self,
    request: Request<RequestVoteRequest>,
  ) -> Result<Response<RequestVoteResponse>, Status> {
    let request = request.into_inner();

    return Ok(Response::new(RequestVoteResponse {
      term: self.current_term,
      vote_granted: self.vote(request).await,
    }));
  }

  #[instrument]
  async fn append_entries(
    &self,
    request: Request<AppendEntriesRequest>,
  ) -> Result<Response<AppendEntriesResponse>, Status> {
    let request = request.into_inner();
    panic!("TODO");
  }
}

#[cfg(test)]
mod tests {
  use crate::tests::support;

  use super::*;

  #[test_log::test(tokio::test)]
  async fn server_does_not_grant_vote_if_candidates_term_is_less_than_the_current_term() {
    let mut raft = Raft::new();

    raft.current_term = 1;

    let mut client = support::grpc::start_server(raft).await;

    let response = client
      .request_vote(Request::new(RequestVoteRequest {
        term: 0,
        candidate_id: 1,
        last_log_index: 0,
        last_log_term: 0,
      }))
      .await
      .unwrap()
      .into_inner();

    let expected = RequestVoteResponse {
      term: 1,
      vote_granted: false,
    };

    assert_eq!(expected, response);
  }

  #[test_log::test(tokio::test)]
  async fn server_grants_vote_if_has_not_voted_yet() {
    let mut raft = Raft::new();

    raft.current_term = 0;

    let mut client = support::grpc::start_server(raft).await;

    let response = client
      .request_vote(Request::new(RequestVoteRequest {
        term: 0,
        candidate_id: 1,
        last_log_index: 0,
        last_log_term: 0,
      }))
      .await
      .unwrap()
      .into_inner();

    let expected = RequestVoteResponse {
      term: 0,
      vote_granted: true,
    };

    assert_eq!(expected, response);
  }

  #[test_log::test(tokio::test)]
  async fn server_does_not_grant_vote_if_it_has_already_voted_for_another_candidate() {
    let mut raft = Raft::new();

    raft.current_term = 0;

    let mut client = support::grpc::start_server(raft).await;

    let _ = client
      .request_vote(Request::new(RequestVoteRequest {
        term: 0,
        candidate_id: 1,
        last_log_index: 0,
        last_log_term: 0,
      }))
      .await
      .unwrap();

    let response = client
      .request_vote(Request::new(RequestVoteRequest {
        term: 0,
        candidate_id: 1,
        last_log_index: 0,
        last_log_term: 0,
      }))
      .await
      .unwrap()
      .into_inner();

    let expected = RequestVoteResponse {
      term: 0,
      vote_granted: false,
    };

    assert_eq!(expected, response);
  }
}
