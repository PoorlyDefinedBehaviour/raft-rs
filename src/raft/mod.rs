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
  current_term: RwLock<u64>,
  /// Candidate that received vote in the current term.
  ///
  /// `voted_for` is None if the server has not voted for a candidate.
  voted_for: Mutex<Option<Candidate>>,
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
  /// The server state, let's us know if the server
  /// is a leader, follower or candidate.
  state: RwLock<State>,
  /// Index of highest log entry applied to state machine.
  ///
  /// Initialized to 0.
  ///
  /// Increases monotonically.
  last_applied_index: u64,
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

/// The candidate the server voted for.
#[derive(Debug, PartialEq)]
struct Candidate {
  /// The id of the candidate that the server voted for.
  candidate_id: u64,
  /// The term in which the server voted for the candidate.
  term: u64,
}

/// Contains the states that a Raft server can be in.
///
/// Every Raft server starts in the Follower state.
#[derive(Debug, PartialEq)]
enum State {
  /// The Raft server is cluster leader.
  Leader,
  /// The Raft server is a follower.
  Follower,
  /// The Raft server is a candidate trying to become the cluster leader.
  Candidate,
}

impl Raft {
  #[instrument]
  pub fn new() -> Self {
    // TODO: handle boot from state persisted to persitent storage.
    Self {
      // TODO: is it ok for the node id to be random?
      // should we use uuids here?
      node_id: rand::thread_rng().gen(),
      state: RwLock::new(State::Follower),
      current_term: RwLock::new(0),
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
  async fn vote(&self, candidate: RequestVoteRequest) -> RequestVoteResponse {
    let mut current_term = self.current_term.write().await;

    // Candidate cannot become the cluster leader if its
    // current term is less than some other server.
    if candidate.term < *current_term {
      info!(
        self.node_id,
        current_term = *current_term,
        candidate.candidate_id,
        candidate.term,
        "vote=false because candidates term is less than the current term"
      );
      return RequestVoteResponse {
        term: *current_term,
        vote_granted: false,
      };
    }

    // If our term is out of date, update it and transition to
    // follower state.
    if candidate.term > *current_term {
      info!(
        self.node_id,
        current_term = *current_term,
        candidate.candidate_id,
        candidate.term,
        "our current term is out of date, updating it and transitioning to follower"
      );

      *self.state.write().await = State::Follower;

      *current_term = candidate.term;
    }

    let mut voted_for = self.voted_for.lock().await;

    // If the server has already voted for another candidate
    // in the same term it cannot vote again.
    if let Some(voted_candidate) = &*voted_for {
      // If we already voted for this candidate in the same term, we grant the vote again.
      if voted_candidate.term == candidate.term
        && voted_candidate.candidate_id == candidate.candidate_id
      {
        info!(
          self.node_id,
          current_term = *current_term,
          candidate.candidate_id,
          candidate.term,
          "vote=true already voted for this candidate in the current term"
        );
        return RequestVoteResponse {
          term: *current_term,
          vote_granted: true,
        };
      }

      // If we voted for someone else, we won't grant the vote for this candidate.
      info!(
        self.node_id,
        current_term = *current_term,
        candidate.candidate_id,
        candidate.term,
        "vote=false because we already voted for another candidate {:?}",
        *voted_for
      );
      return RequestVoteResponse {
        term: *current_term,
        vote_granted: false,
      };
    }

    let logs = self.logs.read().await;

    // The candidate log must as up-to-date as the server's log, otherwise
    // it cannot become the cluster leader.
    if let Some(entry) = logs.last() {
      if entry.term > candidate.last_log_entry_term {
        info!(
            self.node_id,
            candidate.candidate_id,
            candidate.last_log_entry_term,
            "vote=false because the term of our last log entry is greater than the candidate's last log entry term",
          );
        return RequestVoteResponse {
          term: *current_term,
          vote_granted: false,
        };
      }

      if (logs.len() as u64) > candidate.last_log_entry_index {
        info!(
            self.node_id,
            candidate.candidate_id,
            candidate.last_log_entry_term,
            "vote=false because our log is longer than the candidate's log even though our logs end in the same term",
          );
        return RequestVoteResponse {
          term: *current_term,
          vote_granted: false,
        };
      }
    }

    // TODO: persist to stable storage

    *voted_for = Some(Candidate {
      candidate_id: candidate.candidate_id,
      term: *current_term,
    });

    return RequestVoteResponse {
      term: *current_term,
      vote_granted: true,
    };
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
  async fn append_entries(&self, mut request: AppendEntriesRequest) -> AppendEntriesResponse {
    let mut current_term = self.current_term.write().await;

    // If the leader has an older term than the server, ignore the request.
    if request.leader_term < *current_term {
      info!(
        request.leader_term,
        current_term = *current_term,
        "request denied: term is less than the current term",
      );
      return AppendEntriesResponse {
        term: *current_term,
        success: false,
      };
    }

    // If our term is not the newest one or if we are not in the follower state,
    // we will transition to Follower state (if we are not a follower already)
    // and update our term to the newest one.
    if request.leader_term > *current_term || *self.state.read().await != State::Follower {
      info!(
        self.node_id,
        current_term = *current_term,
        request.leader_id,
        request.leader_term,
        "leader has a newer term, transitioning to follower and updating current term"
      );

      *self.state.write().await = State::Follower;

      *current_term = request.leader_term;
    }

    let mut logs = self.logs.write().await;

    info!(
      previous_log_index = request.previous_log_index,
      last_log_entry_index = logs.len(),
    );

    // The previous log index will be 0 if the candidate has no logs.
    if request.previous_log_index > 0 {
      match logs.get(request.previous_log_index as usize) {
        None => {
          info!("request denied: no log at index");

          return AppendEntriesResponse {
            term: *current_term,
            success: false,
          };
        }
        Some(entry) => {
          if entry.term != request.previous_log_term {
            info!(
              "
                log at index has a diffent term than expected.
                truncating logs that come after the request index.
                "
            );

            logs.truncate(request.previous_log_index as usize);
          }
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
        last_log_entry_index = logs.len(),
        "leader commit index if greater than the server commit index. updating it."
      );
      *committed_index = std::cmp::min(request.leader_commit_index, logs.len() as u64);
    }

    info!(committed_index = *committed_index, "new commited_index");

    AppendEntriesResponse {
      term: *current_term,
      success: true,
    }
  }
}

#[tonic::async_trait]
impl raft_server::Raft for Raft {
  async fn request_vote(
    &self,
    request: Request<RequestVoteRequest>,
  ) -> Result<Response<RequestVoteResponse>, Status> {
    let request = request.into_inner();

    return Ok(Response::new(self.vote(request).await));
  }

  async fn append_entries(
    &self,
    request: Request<AppendEntriesRequest>,
  ) -> Result<Response<AppendEntriesResponse>, Status> {
    let request = request.into_inner();

    Ok(Response::new(self.append_entries(request).await))
  }
}

#[cfg(test)]
mod unit_tests {
  use super::*;

  #[test_log::test(tokio::test)]
  async fn follower_does_not_vote_for_candidate_if_candidate_has_less_log_entries_than_the_follower(
  ) {
    let leader = Raft::new();

    let follower = Raft::new();

    assert_eq!(
      AppendEntriesResponse {
        term: 0,
        success: true
      },
      follower
        .append_entries(AppendEntriesRequest {
          leader_term: *leader.current_term.read().await,
          leader_id: leader.node_id,
          previous_log_index: 0,
          previous_log_term: 0,
          entries: vec![Log {
            value: "hello world".as_bytes().to_vec(),
            term: 0,
          }],
          leader_commit_index: leader.last_applied_index,
        })
        .await
    );

    let candidate = Raft::new();

    let response = follower
      .vote(RequestVoteRequest {
        term: *candidate.current_term.read().await,
        candidate_id: candidate.node_id,
        last_log_entry_index: 0, // Because candidate has no logs.
        last_log_entry_term: 0,  // Because candidate has no logs.
      })
      .await;

    assert_eq!(
      RequestVoteResponse {
        term: *leader.current_term.read().await,
        vote_granted: false
      },
      response
    );
  }

  #[test_log::test(tokio::test)]
  async fn follower_does_not_vote_for_candidate_if_candidates_term_is_less_than_the_follower_current_term(
  ) {
    let candidate = Raft::new();

    *candidate.current_term.write().await = 0;

    let follower = Raft::new();

    *follower.current_term.write().await = 1;

    let response = follower
      .vote(RequestVoteRequest {
        term: *candidate.current_term.read().await,
        candidate_id: candidate.node_id,
        last_log_entry_index: 0, // Because candidate has no logs.
        last_log_entry_term: 0,  // Because candidate has no logs.
      })
      .await;

    assert_eq!(
      RequestVoteResponse {
        term: *follower.current_term.read().await,
        vote_granted: false,
      },
      response
    );
  }

  #[test_log::test(tokio::test)]
  async fn follower_does_not_vote_for_candidate_if_it_has_already_voted_for_another_candidate() {
    let candidate_1 = Raft::new();

    let candidate_2 = Raft::new();

    let follower = Raft::new();

    let candidate_1_response = follower
      .vote(RequestVoteRequest {
        term: *candidate_1.current_term.read().await,
        candidate_id: candidate_1.node_id,
        last_log_entry_index: 0, // Because candidate has no logs.
        last_log_entry_term: 0,  // Because candidate has no logs.
      })
      .await;

    let candidate_2_response = follower
      .vote(RequestVoteRequest {
        term: *candidate_2.current_term.read().await,
        candidate_id: candidate_2.node_id,
        last_log_entry_index: 0, // Because candidate has no logs.
        last_log_entry_term: 0,  // Because candidate has no logs.
      })
      .await;

    assert_eq!(
      RequestVoteResponse {
        term: *follower.current_term.read().await,
        vote_granted: true,
      },
      candidate_1_response
    );
    assert_eq!(
      RequestVoteResponse {
        term: *follower.current_term.read().await,
        vote_granted: false,
      },
      candidate_2_response
    );
  }

  #[test_log::test(tokio::test)]
  async fn follower_votes_for_candidate() {
    let candidate = Raft::new();

    let follower = Raft::new();

    let response = follower
      .vote(RequestVoteRequest {
        term: *candidate.current_term.read().await,
        candidate_id: candidate.node_id,
        last_log_entry_index: 0, // Because candidate has no logs.
        last_log_entry_term: 0,  // Because candidate has no logs.
      })
      .await;

    assert_eq!(
      RequestVoteResponse {
        term: *follower.current_term.read().await,
        vote_granted: true,
      },
      response
    );
  }

  #[test_log::test(tokio::test)]
  async fn does_not_append_entries_if_leader_term_is_less_than_the_follower_current_term() {
    let leader = Raft::new();

    let follower = Raft::new();

    *follower.current_term.write().await = 1;

    let response = follower
      .append_entries(AppendEntriesRequest {
        leader_term: 0,
        leader_id: leader.node_id,
        previous_log_index: 0,
        previous_log_term: 0,
        entries: vec![],
        leader_commit_index: 0,
      })
      .await;

    assert_eq!(
      AppendEntriesResponse {
        term: *follower.current_term.read().await,
        success: false,
      },
      response
    );
  }

  #[test_log::test(tokio::test)]
  async fn does_not_append_entries_if_log_does_not_contain_an_entry_at_previous_log_index_sent_in_the_request(
  ) {
    let leader = Raft::new();

    let follower = Raft::new();

    *follower.current_term.write().await = 1;

    let response = follower
      .append_entries(AppendEntriesRequest {
        leader_term: 0,
        leader_id: leader.node_id,
        previous_log_index: 0,
        previous_log_term: 0,
        entries: vec![],
        leader_commit_index: 0,
      })
      .await;

    assert_eq!(
      AppendEntriesResponse {
        term: *follower.current_term.read().await,
        success: false,
      },
      response
    );
  }

  #[test_log::test(tokio::test)]
  async fn while_appending_entries_deletes_conflicting_entries() {
    let leader = Raft::new();

    let follower = Raft::new();

    let _ = follower
      .append_entries(AppendEntriesRequest {
        leader_term: *leader.current_term.read().await,
        leader_id: leader.node_id,
        previous_log_index: 0,
        previous_log_term: 0,
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

    *follower.current_term.write().await = 2;

    let _ = follower
      .append_entries(AppendEntriesRequest {
        leader_term: 2,
        leader_id: leader.node_id,
        previous_log_index: 1,
        previous_log_term: 2,
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
      AppendEntriesResponse {
        term: *leader.current_term.read().await,
        success: true,
      },
      follower
        .append_entries(AppendEntriesRequest {
          leader_term: 0,
          leader_id: leader.node_id,
          previous_log_index: 0,
          previous_log_term: 0,
          entries: vec![Log {
            term: 0,
            value: "a".as_bytes().to_vec()
          }],
          leader_commit_index: 0,
        })
        .await
    );

    assert_eq!(
      AppendEntriesResponse {
        term: *leader.current_term.read().await,
        success: true,
      },
      follower
        .append_entries(AppendEntriesRequest {
          leader_term: 0,
          leader_id: leader.node_id,
          previous_log_index: 0,
          previous_log_term: 0,
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
        leader_term: 1,
        leader_id: leader.node_id,
        previous_log_index: 0,
        previous_log_term: 0,
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
        leader_term: 3,
        leader_id: leader.node_id,
        previous_log_index: 0,
        previous_log_term: 0,
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
    let follower = Raft::new();

    *follower.current_term.write().await = 1;

    let mut client = support::grpc::start_server(follower).await;

    let response = client
      .request_vote(Request::new(RequestVoteRequest {
        term: 0,
        candidate_id: 1,
        last_log_entry_index: 0,
        last_log_entry_term: 0,
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
    let follower = Raft::new();

    *follower.current_term.write().await = 0;

    let mut client = support::grpc::start_server(follower).await;

    let response = client
      .request_vote(Request::new(RequestVoteRequest {
        term: 0,
        candidate_id: 1,
        last_log_entry_index: 0,
        last_log_entry_term: 0,
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
    let follower = Raft::new();

    let candidate_1 = Raft::new();

    let candidate_2 = Raft::new();

    let mut client = support::grpc::start_server(follower).await;

    let _ = client
      .request_vote(Request::new(RequestVoteRequest {
        term: *candidate_1.current_term.read().await,
        candidate_id: candidate_1.node_id,
        last_log_entry_index: 0,
        last_log_entry_term: 0,
      }))
      .await
      .unwrap();

    let response = client
      .request_vote(Request::new(RequestVoteRequest {
        term: *candidate_2.current_term.read().await,
        candidate_id: candidate_2.node_id,
        last_log_entry_index: 0,
        last_log_entry_term: 0,
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
    let follower = Raft::new();

    *follower.current_term.write().await = 2;

    let mut client = support::grpc::start_server(follower).await;

    let response = client
      .append_entries(Request::new(AppendEntriesRequest {
        leader_term: 1,
        leader_id: 0,
        previous_log_index: 0,
        previous_log_term: 0,
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
    let follower = Raft::new();

    *follower.current_term.write().await = 1;

    let mut client = support::grpc::start_server(follower).await;

    let response = client
      .append_entries(Request::new(AppendEntriesRequest {
        leader_term: 1,
        leader_id: 0,
        previous_log_index: 1,
        previous_log_term: 0,
        entries: vec![Log {
          value: "hello world".as_bytes().to_vec(),
          term: 0,
        }],
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
  async fn server_does_not_append_entries_if_the_log_term_at_previous_log_index_does_not_match_the_entry_thats_in_the_index(
  ) {
    let follower = Raft::new();

    *follower.current_term.write().await = 1;

    let mut client = support::grpc::start_server(follower).await;

    let _ = client
      .append_entries(Request::new(AppendEntriesRequest {
        leader_term: 1,
        leader_id: 1,
        previous_log_index: 0,
        previous_log_term: 0,
        entries: vec![Log {
          value: "hello world".as_bytes().to_vec(),
          term: 1,
        }],
        leader_commit_index: 0,
      }))
      .await
      .unwrap()
      .into_inner();

    let response = client
      .append_entries(Request::new(AppendEntriesRequest {
        leader_term: 1,
        leader_id: 1,
        previous_log_index: 1,
        previous_log_term: 0,
        entries: vec![Log {
          value: "hello world".as_bytes().to_vec(),
          term: 1,
        }],
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
    let leader = Raft::new();

    let follower = Raft::new();

    let mut client = support::grpc::start_server(follower).await;

    let response = client
      .append_entries(Request::new(AppendEntriesRequest {
        leader_term: *leader.current_term.read().await,
        leader_id: leader.node_id,
        previous_log_index: 0,
        previous_log_term: 0,
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
