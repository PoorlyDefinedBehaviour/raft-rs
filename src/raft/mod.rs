use std::{sync::Arc, time::Duration};

use rand::Rng;
use tokio::{
  select,
  sync::{Mutex, RwLock},
};
use tonic::{Request, Response, Status};
use tracing::{debug, info, instrument};

// Include Rust files generated in build.rs
tonic::include_proto!("raft.v1");

/// The Raft server config.
#[derive(Debug)]
pub struct Config {
  /// The minimum amount of time a follower will wait without
  /// receiving a heartbeat from the leader before
  /// becoming a candidate and starting an election.
  /// The actual heartbeat timeout will be a random number between
  /// `min_heartbeat_timeout` and `min_heartbeat_timeout` * 2.
  pub min_heartbeat_timeout: Duration,
  /// The minimum amount of time a candidate will wait for an election
  /// to end and for a new leader to be elected before restarting
  /// the election.
  /// The actual election timeout will be a random number between
  /// `min_election_timeout` and `min_election_timeout` * 2.
  pub min_election_timeout: Duration,
  /// The amount of time the leader will wait before sending
  /// an empty AppendEntries requests to the followers to ensure
  /// that they do not start an election.
  pub send_heartbeat_timeout: Duration,
  /// The list of the addresses of the servers in the cluster.
  pub servers: Vec<String>,
}

impl Default for Config {
  fn default() -> Self {
    Self {
      min_heartbeat_timeout: Duration::from_millis(250),
      min_election_timeout: Duration::from_millis(150),
      send_heartbeat_timeout: Duration::from_millis(50),
      servers: Vec::new(),
    }
  }
}

/// Messages sent by the client that are sent to
/// the channel consumed by Raft::run.
#[derive(Debug)]
enum Message {
  AppendEntries {
    request: AppendEntriesRequest,
    response_tx: tokio::sync::oneshot::Sender<AppendEntriesResponse>,
  },
  RequestVote {
    request: RequestVoteRequest,
    response_tx: tokio::sync::oneshot::Sender<RequestVoteResponse>,
  },
  InstallSnapshot {
    request: InstallSnapshotRequest,
    response_tx: tokio::sync::oneshot::Sender<InstallSnapshotResponse>,
  },
}

#[derive(Debug)]
pub struct Raft {
  /// The Raft server configuration.
  config: Config,
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
  /// Channels used to send and consume RPC requests.
  request_tx: Mutex<tokio::sync::mpsc::Sender<Message>>,
  request_rx: Mutex<tokio::sync::mpsc::Receiver<Message>>,
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
  pub fn new(config: Config) -> Arc<Self> {
    let (request_tx, request_rx) = tokio::sync::mpsc::channel(1024);

    // TODO: handle boot from state persisted to persitent storage.
    let raft = Arc::new(Self {
      config,
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
      request_tx: Mutex::new(request_tx),
      request_rx: Mutex::new(request_rx),
    });

    let raft_arc_clone = Arc::clone(&raft);

    tokio::spawn(async move { raft_arc_clone.run().await });

    raft
  }

  /// The Raft state machine.
  ///
  /// It delegates to sub state machines depending
  /// on the current state.
  #[instrument(skip_all)]
  async fn run(self: Arc<Self>) {
    info!(self.node_id, "starting Raft state machine");

    loop {
      let state = self.state.read().await;
      info!("current state: {:?}", *state);
      match *state {
        State::Follower => {
          drop(state);
          self.run_follower().await;
        }
        State::Candidate => {
          drop(state);
          self.run_candidate().await;
        }
        State::Leader => {
          drop(state);
          self.run_leader().await;
        }
      }
    }
  }

  /// Returns a random heartbeat timeout that's between the minimum timeout and the minimum timeout * 2.
  fn heartbeat_timeout(&self) -> Duration {
    rand::thread_rng()
      .gen_range(self.config.min_heartbeat_timeout..=self.config.min_heartbeat_timeout * 2)
  }

  /// Returns a random election timeout that's between the minimum timeout and the minimum timeout * 2.
  fn election_timeout(&self) -> Duration {
    rand::thread_rng()
      .gen_range(self.config.min_election_timeout..=self.config.min_election_timeout * 2)
  }

  /// Returns true when the server is the cluster leader.
  async fn is_leader(&self) -> bool {
    *self.state.read().await == State::Leader
  }

  /// Returns true when the server is a follower.
  async fn is_follower(&self) -> bool {
    *self.state.read().await == State::Follower
  }

  /// Returns true when the server is a candidate.
  async fn is_candidate(&self) -> bool {
    *self.state.read().await == State::Candidate
  }

  /// The follower state machine.
  async fn run_follower(&self) {
    info!("running follower fsm");

    let mut request_rx = self.request_rx.lock().await;

    loop {
      if !self.is_follower().await {
        return;
      }

      select! {
        Some(message) = request_rx.recv() => {
          match message {
            Message::AppendEntries{ request, response_tx } => {
              let response = self.append_entries(request).await;
              let _ = response_tx.send(response).unwrap();
            },
            Message::RequestVote{ request,response_tx } => {
              let response = self.vote(request).await;
              let _ = response_tx.send(response).unwrap();
            }
            Message::InstallSnapshot { request, response_tx } => {
              let response = self.install_snapshot(request).await;
              let _ = response_tx.send(response).unwrap();
            }
          }
        }
         // Heartbeat timeout.
         _ = tokio::time::sleep(self.heartbeat_timeout()) => {
          info!(self.node_id, "heartbeat timed out, becoming a candidate");

          // Become a candidate because we will start an election to become the leader.
          *self.state.write().await = State::Candidate;
        }
      }
    }
  }

  /// The candidate state machine.
  async fn run_candidate(&self) {
    info!(self.node_id, "running candidate fsm");

    let mut request_rx = self.request_rx.lock().await;

    loop {
      if !self.is_candidate().await {
        info!("not candidate, exiting candidate fsm");
        return;
      }

      select! {
        // Start an election to try to become the leader.
        _ = self.start_election() => {}
        Some(message) = request_rx.recv()=> {
          match message {
            Message::AppendEntries{ request, response_tx } => {
              let response = self.append_entries(request).await;
              let _ = response_tx.send(response).unwrap();
            },
            Message::RequestVote{ request,response_tx } => {
              let response = self.vote(request).await;
              let _ = response_tx.send(response).unwrap();
            }
            Message::InstallSnapshot { request, response_tx } => {
              let response = self.install_snapshot(request).await;
              let _ = response_tx.send(response).unwrap();
            }
          }
        }
      }
    }
  }

  /// The leader state machine.
  async fn run_leader(&self) {
    info!(self.node_id, "running leader fsm");

    let mut request_rx = self.request_rx.lock().await;

    while self.is_leader().await {
      select! {
        Some(message) = request_rx.recv()=> {
          match message {
            Message::AppendEntries{ request, response_tx } => {
              let response = self.append_entries(request).await;
              let _ = response_tx.send(response).unwrap();
            },
            Message::RequestVote{ request,response_tx } => {
              let response = self.vote(request).await;
              let _ = response_tx.send(response).unwrap();
            }
            Message::InstallSnapshot { request, response_tx } => {
              let response = self.install_snapshot(request).await;
              let _ = response_tx.send(response).unwrap();
            }
          }
        }
        _ = tokio::time::sleep(self.heartbeat_timeout()) => {
          info!(self.node_id, "sending heartbeat to followers");

          self.send_heartbeat_to_followers().await;
        }
      }
    }
  }

  /// Sends an empty AppendEntries request to follower to assert
  /// our leadership.
  ///
  /// Becomes a follower if any of the follower has a term that's
  /// greater than the term we have.
  async fn send_heartbeat_to_followers(&self) {
    info!(
      "sending heartbeat to {} followers",
      self.config.servers.len()
    );

    if self.config.servers.is_empty() {
      return;
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel(self.config.servers.len());

    let handles = self.config.servers.iter().cloned().map(|server| {
      let tx = tx.clone();

      tokio::spawn(async move {
        let server = server.clone();

        info!("sending heartbeat to {}", &server);

        let mut client = raft_client::RaftClient::connect(server.clone())
          .await
          .unwrap();

        // TODO: fill RequestVoteRequest with the candidate's info.
        let response = client
          .append_entries(AppendEntriesRequest {
            // TODO: replace me with the leader data
            ..Default::default()
          })
          .await;

        tx.send(response).await.unwrap();
      })
    });

    while let Some(result) = rx.recv().await {
      debug!(?result);

      if let Ok(response) = result {
        // TODO: compare response term to our own
        break;
      }
    }

    // Cancel RPC calls that have not been completed yet.
    for handle in handles {
      handle.abort();
    }
  }

  /// Returns the amount of votes needed for the server become a leader
  /// after an election.
  ///
  /// To become a leader, a candidate needs the majority of votes.
  /// That is, if the cluster has 5 servers, the candidate needs
  /// to receive 3 votes to become the new leader.
  fn votes_needed_to_become_leader(&self) -> usize {
    self.config.servers.len() / 2 + 1
  }

  /// Makes RPC calls to each node in the cluster asking for a vote.
  ///
  /// Returns the number of votes the server got from the other nodes.
  ///
  /// Note that it may return before every request is completed if the server
  /// gets the majority of votes.
  async fn request_votes_from_peers(&self, votes_needed: usize) -> usize {
    // We always get at least one vote:
    // our own vote because we voted for ourselves.
    let mut votes_received = 1;

    if self.config.servers.is_empty() {
      info!("single node. not requesting vote from peers");
      return votes_received;
    }

    info!(
      "sending RequestVote requests to {} peers",
      self.config.servers.len()
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel(self.config.servers.len());

    let handles = self.config.servers.iter().cloned().map(|server| {
      let tx = tx.clone();

      tokio::spawn(async move {
        let server = server.clone();

        info!("requesting vote from {}", &server);

        let mut client = raft_client::RaftClient::connect(server.clone())
          .await
          .unwrap();

        // TODO: fill RequestVoteRequest with the candidate's info.
        let response = client
          .request_vote(RequestVoteRequest {
            term: 0,
            candidate_id: 0,
            last_log_entry_index: 0, // Because candidate has no logs.
            last_log_entry_term: 0,  // Because candidate has no logs.
          })
          .await;

        tx.send(response).await.unwrap();
      })
    });

    while let Some(result) = rx.recv().await {
      debug!(?result);

      if let Ok(response) = result {
        if response.into_inner().vote_granted {
          votes_received += 1;
        }
      }

      if votes_received >= votes_needed {
        break;
      }
    }

    // Cancel RPC calls that have not been completed yet.
    // Note that there will be RPC calls to cancel only when we
    // get the majority of votes before every request completes.
    for handle in handles {
      handle.abort();
    }

    votes_received
  }

  /// Starts a new term by increment the current term by 1.
  ///
  /// Sets voted_for to None because the server has not voted
  /// in the new term yet.
  async fn start_new_term(&self) {
    // TODO: this is bad, locking two things at different times.
    // We should unify the mutable state.
    *self.current_term.write().await += 1;
    *self.voted_for.lock().await = None;
  }

  /// After a follower receives no heartbeats for the determined
  /// heartbeat timeout, it becomes a candidate and initiates an election.
  async fn start_election(&self) {
    let election_timeout = self.election_timeout();

    info!(self.node_id, ?election_timeout, "starting an election");

    select! {
      _ = async {
        // Transition to candidate state.
        *self.state.write().await = State::Candidate;

        // Start a new term.
        self.start_new_term().await;

        let votes_needed = self.votes_needed_to_become_leader();

        // TODO: don't we need to go back to follower mode if a peer RequestVote response
        // returns a term that's greater than ours?
        let votes_received = self.request_votes_from_peers(votes_needed).await;

        info!("need {} votes, received {}", votes_needed, votes_received);

        // If we got enough votes, transition to leader state.
        if votes_received >= votes_needed {
          info!("got enough votes, becoming leader");
          *self.state.write().await = State::Leader;
        }
      } => {}
      _ = tokio::time::sleep(election_timeout) => {
        info!(self.node_id, "election timed out, going back to follower state");
        *self.state.write().await = State::Follower;
      }
    };
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

    RequestVoteResponse {
      term: *current_term,
      vote_granted: true,
    }
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

  /// Updates follower logs based on a snapshot from the cluster leader.
  pub async fn install_snapshot(&self, request: InstallSnapshotRequest) -> InstallSnapshotResponse {
    let mut current_term = self.current_term.write().await;

    // Reply immediately with the current term
    // if the leader term is less than the current term.
    // The leader will use the term we return to update itself.
    if request.leader_term < *current_term {
      info!(
        request_term = request.leader_term,
        current_term = *current_term,
        "request term is less than the current term"
      );
      return InstallSnapshotResponse {
        term: *current_term,
      };
    }

    let mut state = self.state.write().await;

    // If our term is out of date, update it and transition
    // into the follower state.
    if request.leader_term > *current_term {
      info!(
        self.node_id,
        current_term = *current_term,
        request.leader_id,
        request.leader_term,
        "leader has a newer term, transitioning to follower and updating current term"
      );

      *state = State::Follower;
      *current_term = request.leader_term;
    }

    // TODO: save current leader

    InstallSnapshotResponse {
      term: *current_term,
    }
  }
}

#[tonic::async_trait]
impl raft_server::Raft for Arc<Raft> {
  async fn request_vote(
    &self,
    request: Request<RequestVoteRequest>,
  ) -> Result<Response<RequestVoteResponse>, Status> {
    let request = request.into_inner();

    let (tx, rx) = tokio::sync::oneshot::channel();

    {
      let request_tx = self.request_tx.lock().await;

      request_tx
        .send(Message::RequestVote {
          request,
          response_tx: tx,
        })
        .await
        .unwrap();
    }

    Ok(Response::new(rx.await.unwrap()))
  }

  async fn append_entries(
    &self,
    request: Request<AppendEntriesRequest>,
  ) -> Result<Response<AppendEntriesResponse>, Status> {
    let request = request.into_inner();

    let (tx, rx) = tokio::sync::oneshot::channel();

    {
      let request_tx = self.request_tx.lock().await;

      request_tx
        .send(Message::AppendEntries {
          request,
          response_tx: tx,
        })
        .await
        .unwrap();
    }

    Ok(Response::new(rx.await.unwrap()))
  }

  async fn install_snapshot(
    &self,
    request: Request<InstallSnapshotRequest>,
  ) -> Result<Response<InstallSnapshotResponse>, Status> {
    let request = request.into_inner();

    let (tx, rx) = tokio::sync::oneshot::channel();

    {
      let request_tx = self.request_tx.lock().await;

      request_tx
        .send(Message::InstallSnapshot {
          request,
          response_tx: tx,
        })
        .await
        .unwrap();
    }

    Ok(Response::new(rx.await.unwrap()))
  }
}

#[cfg(test)]
mod unit_tests {
  use super::*;

  #[test_log::test(tokio::test)]
  async fn follower_does_not_vote_for_candidate_if_candidate_has_less_log_entries_than_the_follower(
  ) {
    let leader = Raft::new(Config::default());

    let follower = Raft::new(Config::default());

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

    let candidate = Raft::new(Config::default());

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
    let candidate = Raft::new(Config::default());

    *candidate.current_term.write().await = 0;

    let follower = Raft::new(Config::default());

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
    let candidate_1 = Raft::new(Config::default());

    let candidate_2 = Raft::new(Config::default());

    let follower = Raft::new(Config::default());

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
    let candidate = Raft::new(Config::default());

    let follower = Raft::new(Config::default());

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
    let leader = Raft::new(Config::default());

    let follower = Raft::new(Config::default());

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
    let leader = Raft::new(Config::default());

    let follower = Raft::new(Config::default());

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
    let leader = Raft::new(Config::default());

    let follower = Raft::new(Config::default());

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

    // TODO: fix me
    // assert_eq!(expected, follower.into_inner().logs.into_inner());
  }

  #[test_log::test(tokio::test)]
  async fn append_entries_returns_true_when_entries_are_appended() {
    let leader = Raft::new(Config::default());

    let follower = Raft::new(Config::default());

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
    let leader = Raft::new(Config::default());

    *leader.committed_index.lock().await = 1;

    let follower = Raft::new(Config::default());

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

  #[test_log::test(tokio::test)]
  async fn does_not_install_snapshot_if_leader_term_is_less_than_the_current_term() {
    let leader = Raft::new(Config::default());

    *leader.current_term.write().await = 0;

    let follower = Raft::new(Config::default());

    *follower.current_term.write().await = 1;

    let response = follower
      .install_snapshot(InstallSnapshotRequest {
        leader_term: *leader.current_term.read().await,
        leader_id: leader.node_id,
        last_included_index: 0,
        last_included_term: 0,
        offset: 0,
        data: vec![],
        done: true,
      })
      .await;

    assert_eq!(
      InstallSnapshotResponse {
        term: *follower.current_term.write().await
      },
      response
    );
  }
}

#[cfg(test)]
mod integration_tests {
  use crate::tests::support;

  use super::*;

  #[test_log::test(tokio::test)]
  async fn server_does_not_grant_vote_if_candidates_term_is_less_than_the_current_term() {
    let follower = Raft::new(Config::default());

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
    let follower = Raft::new(Config::default());

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
    let follower = Raft::new(Config::default());

    let candidate_1 = Raft::new(Config::default());

    let candidate_2 = Raft::new(Config::default());

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
    let follower = Raft::new(Config::default());

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
    let follower = Raft::new(Config::default());

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
    let follower = Raft::new(Config::default());

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
    let leader = Raft::new(Config::default());

    let follower = Raft::new(Config::default());

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
