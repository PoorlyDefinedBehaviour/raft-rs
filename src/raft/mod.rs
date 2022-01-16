use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};
use tracing::{info, instrument};

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
  logs: RwLock<Vec<Log>>,
  /// # Volatile state on all servers
  ///
  /// Index of highest log entry known to be committed.
  ///
  /// Initialized to 0.
  ///
  /// Increases monotonically.
  committed_index: Mutex<u64>,
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
      logs: RwLock::new(vec![]),
      committed_index: Mutex::new(0),
      last_applied_index: 0,
      // TODO: next_index.len() should be qual to the amount of servers in the cluster.
      next_index: vec![],
      // TODO: match_index.len() should be qual to the amount of servers in the cluster.
      match_index: vec![],
    }
  }

  /// Returns true if the server decides to vote
  /// for the candidate to be the new cluster leader.
  #[instrument(skip_all)]
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

    let logs = self.logs.read().await;

    // The candidate log must as up-to-date as the server's log, otherwise
    // it cannot become the cluster leader.
    if let Some(index) = candidate.last_log_index {
      if (index as usize) < logs.len() - 1 {
        return false;
      }
    }

    // TODO: am i needed?
    if let (Some(entry), Some(candidate_last_log_term)) = (logs.last(), candidate.last_log_term) {
      if entry.term != candidate_last_log_term {
        return false;
      }
    }

    *voted_for = Some(candidate.candidate_id);

    true
  }

  /// Appends new entries to the server logs.
  ///
  /// To append new entries to the server logs:
  ///
  /// - The leader must map the server to a term thats greater or equal to the server current term.
  /// - The server must contain an entry at `request.previous_log_index`
  /// whose term matches `request.previous_log_term`.
  #[instrument(skip_all)]
  async fn append_entries(&self, mut request: AppendEntriesRequest) -> bool {
    // If the term that the leader thinks this server has is not correct,
    // the server does not accept the new log entries.
    if request.term < self.current_term {
      info!(
        request.term,
        self.current_term, "request denied: term is less than the current term",
      );
      return false;
    }

    let mut logs = self.logs.write().await;

    info!(
      previous_log_index = request.previous_log_index,
      last_log_index = logs.len(),
    );

    if let Some(index) = request.previous_log_index {
      let index = index as usize;

      if index >= logs.len() {
        info!("request denied: no log at index");

        return false;
      }

      if let (Some(log), Some(term)) = (logs.get(index), request.previous_log_term) {
        if log.term != term {
          info!(
            "
              log at index has a diffent term than expected.
              truncating logs that come after the request index.
              "
          );

          logs.truncate(index);
        }
      }
    }

    info!(
      number_of_entries = request.entries.len(),
      "appending entries to the log"
    );

    logs.append(&mut request.entries);

    let mut committed_index = self.committed_index.lock().await;

    if request.leader_commit_index > *committed_index {
      *committed_index = std::cmp::min(request.leader_commit_index, logs.len() as u64 - 1);
    }

    info!(committed_index = *committed_index, "new commited_index");

    true
  }
}

#[tonic::async_trait]
impl raft_server::Raft for Raft {
  #[instrument(skip_all)]
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

  #[instrument(skip_all)]
  async fn append_entries(
    &self,
    request: Request<AppendEntriesRequest>,
  ) -> Result<Response<AppendEntriesResponse>, Status> {
    let request = request.into_inner();

    Ok(Response::new(AppendEntriesResponse {
      term: self.current_term,
      success: self.append_entries(request).await,
    }))
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
        last_log_index: None,
        last_log_term: None,
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
        last_log_index: None,
        last_log_term: None,
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
        last_log_index: None,
        last_log_term: None,
      }))
      .await
      .unwrap();

    let response = client
      .request_vote(Request::new(RequestVoteRequest {
        term: 0,
        candidate_id: 1,
        last_log_index: None,
        last_log_term: None,
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

  #[test_log::test(tokio::test)]
  async fn server_does_not_append_entries_if_request_term_is_less_than_the_current_term() {
    let mut follower = Raft::new();

    follower.current_term = 2;

    let mut client = support::grpc::start_server(follower).await;

    let response = client
      .append_entries(Request::new(AppendEntriesRequest {
        term: 1,
        leader_id: 0,
        previous_log_index: Some(0),
        previous_log_term: Some(0),
        entries: vec![],
        leader_commit_index: 0,
      }))
      .await
      .unwrap()
      .into_inner();

    let expected = AppendEntriesResponse {
      term: 2,
      success: false,
    };

    assert_eq!(expected, response);
  }

  #[test_log::test(tokio::test)]
  async fn when_appending_entries_deletes_conflicting_entries_that_come_after_the_previous_log_index_sent_in_the_request(
  ) {
    let mut follower = Raft::new();

    follower.current_term = 2;

    {
      let mut logs = follower.logs.write().await;
      *logs = vec![
        Log {
          term: 0,
          value: vec![],
        },
        Log {
          term: 1,
          value: vec![],
        },
        Log {
          term: 2,
          value: vec![],
        },
      ];
    }

    let mut client = support::grpc::start_server(follower).await;

    let response = client
      .append_entries(Request::new(AppendEntriesRequest {
        term: 1,
        leader_id: 0,
        previous_log_index: Some(1),
        previous_log_term: Some(0),
        entries: vec![Log {
          term: 1,
          value: "hello world".as_bytes().to_vec(),
        }],
        leader_commit_index: 0,
      }))
      .await
      .unwrap()
      .into_inner();

    let expected = vec![
      Log {
        term: 0,
        value: vec![],
      },
      Log {
        term: 1,
        value: "hello world".as_bytes().to_vec(),
      },
    ];
  }

  #[test_log::test(tokio::test)]
  async fn server_does_not_append_entries_if_it_does_not_contain_an_entry_at_previous_log_index_sent_in_the_request(
  ) {
    let mut follower = Raft::new();

    follower.current_term = 1;

    let mut client = support::grpc::start_server(follower).await;

    let response = client
      .append_entries(Request::new(AppendEntriesRequest {
        term: 1,
        leader_id: 0,
        previous_log_index: Some(0),
        previous_log_term: Some(0),
        entries: vec![],
        leader_commit_index: 0,
      }))
      .await
      .unwrap()
      .into_inner();

    let expected = AppendEntriesResponse {
      term: 1,
      success: false,
    };

    assert_eq!(expected, response);
  }

  #[test_log::test(tokio::test)]
  async fn appends_new_entries_to_server_logs() {
    let follower = Raft::new();

    let mut client = support::grpc::start_server(follower).await;

    let response = client
      .append_entries(Request::new(AppendEntriesRequest {
        term: 0,
        leader_id: 0,
        previous_log_index: None,
        previous_log_term: None,
        entries: vec![Log {
          term: 0,
          value: "hello world".as_bytes().to_vec(),
        }],
        leader_commit_index: 0,
      }))
      .await
      .unwrap()
      .into_inner();

    let expected = AppendEntriesResponse {
      term: 0,
      success: true,
    };

    assert_eq!(expected, response);
  }
}
