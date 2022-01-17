use rand::Rng;
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
  /// when the entry was received by the leader.
  ///
  /// Note that the first index should be 1.
  logs: RwLock<Vec<Log>>,
  /// # Volatile state on all servers
  ///
  /// A unique number that identifies this node.
  // TODO: does this need to increase monotonically?
  node_id: u64,
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
      // TODO: is it ok for the node id to be random?
      node_id: rand::thread_rng().gen(),
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
  ///
  /// Conflicting entries are deleted from the server log.
  /// An entry is considered to be conflicting with another entry
  /// if it has the same index as a new entry but a different term.
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
      info!(
        request.leader_commit_index,
        last_log_index = logs.len(),
        "leader commit index if greater than the server commit index. updating it."
      );
      *committed_index = std::cmp::min(request.leader_commit_index, logs.len() as u64);
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
mod unit_tests {
  use super::*;

  #[test_log::test(tokio::test)]
  async fn follower_does_not_vote_for_candidate_if_candidates_term_is_less_than_the_follower_current_term(
  ) {
    let mut candidate = Raft::new();

    candidate.current_term = 0;

    let mut follower = Raft::new();

    follower.current_term = 1;

    let vote_granted = follower
      .vote(RequestVoteRequest {
        term: candidate.current_term,
        candidate_id: candidate.node_id,
        last_log_index: None, // Because candidate has no logs.
        last_log_term: None,  // Because candi has no logs.
      })
      .await;

    assert_eq!(false, vote_granted);
  }

  #[test_log::test(tokio::test)]
  async fn follower_does_not_for_candidate_if_it_has_already_voted_for_another_candidate() {
    let candidate_1 = Raft::new();

    let candidate_2 = Raft::new();

    let follower = Raft::new();

    let vote_granted_to_candidate_1 = follower
      .vote(RequestVoteRequest {
        term: candidate_1.current_term,
        candidate_id: candidate_1.node_id,
        last_log_index: None, // Because candidate has no logs.
        last_log_term: None,  // Because candi has no logs.
      })
      .await;

    let vote_granted_to_candidate_2 = follower
      .vote(RequestVoteRequest {
        term: candidate_2.current_term,
        candidate_id: candidate_2.node_id,
        last_log_index: None, // Because candidate has no logs.
        last_log_term: None,  // Because candi has no logs.
      })
      .await;

    assert_eq!(true, vote_granted_to_candidate_1);
    assert_eq!(false, vote_granted_to_candidate_2);
  }

  #[test_log::test(tokio::test)]
  async fn follower_votes_for_candidate() {
    let candidate = Raft::new();

    let follower = Raft::new();

    let vote_granted = follower
      .vote(RequestVoteRequest {
        term: candidate.current_term,
        candidate_id: candidate.node_id,
        last_log_index: None, // Because candidate has no logs.
        last_log_term: None,  // Because candi has no logs.
      })
      .await;

    assert_eq!(true, vote_granted);
  }

  #[test_log::test(tokio::test)]
  async fn does_not_append_entries_if_leader_term_is_less_than_the_follower_current_term() {
    let leader = Raft::new();

    let mut follower = Raft::new();

    follower.current_term = 1;

    let success = follower
      .append_entries(AppendEntriesRequest {
        term: 0,
        leader_id: leader.node_id,
        previous_log_index: None,
        previous_log_term: None,
        entries: vec![],
        leader_commit_index: 0,
      })
      .await;

    assert_eq!(false, success);
  }

  #[test_log::test(tokio::test)]
  async fn does_not_append_entries_if_log_does_not_contain_an_entry_at_previous_log_index_sent_in_the_request(
  ) {
    let leader = Raft::new();

    let mut follower = Raft::new();

    follower.current_term = 1;

    let success = follower
      .append_entries(AppendEntriesRequest {
        term: 0,
        leader_id: leader.node_id,
        previous_log_index: Some(0),
        previous_log_term: Some(0),
        entries: vec![],
        leader_commit_index: 0,
      })
      .await;

    assert_eq!(false, success);
  }

  #[test_log::test(tokio::test)]
  async fn while_appending_entries_deletes_conflicting_entries() {
    let leader = Raft::new();

    let mut follower = Raft::new();

    let _ = follower
      .append_entries(AppendEntriesRequest {
        term: 0,
        leader_id: leader.node_id,
        previous_log_index: None,
        previous_log_term: None,
        entries: vec![
          Log {
            term: 0,
            value: "a".as_bytes().to_vec(),
          },
          Log {
            term: 1,
            value: "b".as_bytes().to_vec(),
          },
          Log {
            term: 1,
            value: "c".as_bytes().to_vec(),
          },
        ],
        leader_commit_index: 0,
      })
      .await;

    follower.current_term = 2;

    let _ = follower
      .append_entries(AppendEntriesRequest {
        term: 2,
        leader_id: leader.node_id,
        previous_log_index: Some(1),
        previous_log_term: Some(2),
        entries: vec![Log {
          term: 2,
          value: "d".as_bytes().to_vec(),
        }],
        leader_commit_index: 0,
      })
      .await;

    let expected = vec![
      Log {
        term: 0,
        value: "a".as_bytes().to_vec(),
      },
      Log {
        term: 2,
        value: "d".as_bytes().to_vec(),
      },
    ];

    assert_eq!(expected, follower.logs.into_inner());
  }

  #[test_log::test(tokio::test)]
  async fn append_entries_returns_true_when_entries_are_appended() {
    let leader = Raft::new();

    let follower = Raft::new();

    assert_eq!(
      true,
      follower
        .append_entries(AppendEntriesRequest {
          term: 0,
          leader_id: leader.node_id,
          previous_log_index: None,
          previous_log_term: None,
          entries: vec![Log {
            term: 0,
            value: "a".as_bytes().to_vec()
          }],
          leader_commit_index: 0,
        })
        .await
    );

    assert_eq!(
      true,
      follower
        .append_entries(AppendEntriesRequest {
          term: 0,
          leader_id: leader.node_id,
          previous_log_index: None,
          previous_log_term: None,
          entries: vec![Log {
            term: 0,
            value: "b".as_bytes().to_vec()
          }],
          leader_commit_index: 0,
        })
        .await
    );
  }

  #[test_log::test(tokio::test)]
  async fn after_appending_new_entries_the_server_committed_index_is_updated() {
    let leader = Raft::new();

    *leader.committed_index.lock().await = 1;

    let follower = Raft::new();

    let _ = follower
      .append_entries(AppendEntriesRequest {
        term: 1,
        leader_id: leader.node_id,
        previous_log_index: None,
        previous_log_term: None,
        entries: vec![Log {
          term: 1,
          value: "hello world 1".as_bytes().to_vec(),
        }],
        leader_commit_index: *leader.committed_index.lock().await,
      })
      .await;

    // The follower commit index becomes the server commit index
    // because min(leader commit index, follower.logs.lock().await.len()) = 1.
    assert_eq!(1, *follower.committed_index.lock().await);

    *leader.committed_index.lock().await = 3;

    let _ = follower
      .append_entries(AppendEntriesRequest {
        term: 3,
        leader_id: leader.node_id,
        previous_log_index: None,
        previous_log_term: None,
        entries: vec![Log {
          term: 3,
          value: "hello world 2".as_bytes().to_vec(),
        }],
        leader_commit_index: *leader.committed_index.lock().await,
      })
      .await;

    // The follower commit index becomes the server commit index
    // because min(leader commit index, follower.logs.lock().await.len()) = 2.
    // The leader commit index is 3 but the follower logs has 2 entries.
    assert_eq!(2, *follower.committed_index.lock().await);
  }
}

#[cfg(test)]
mod integration_tests {
  use crate::tests::support;

  use super::*;

  #[test_log::test(tokio::test)]
  async fn server_does_not_grant_vote_if_candidates_term_is_less_than_the_current_term() {
    let mut follower = Raft::new();

    follower.current_term = 1;

    let mut client = support::grpc::start_server(follower).await;

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
    let mut follower = Raft::new();

    follower.current_term = 0;

    let mut client = support::grpc::start_server(follower).await;

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
    let mut follower = Raft::new();

    follower.current_term = 0;

    let mut client = support::grpc::start_server(follower).await;

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
