use super::{Address, Entry, Event, Message, Response, Scan, Status};
use crate::error::{Error, Result};

use log::{debug, error};
use std::collections::{BTreeMap, HashMap, HashSet};

/// A Raft-managed state machine.
pub trait State: Send {
    /// Returns the last applied index from the state machine, used when initializing the driver.
    fn applied_index(&self) -> u64;

    /// Mutates the state machine. If the state machine returns Error::Internal, the Raft node
    /// halts. For any other error, the state is applied and the error propagated to the caller.
    fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>>;

    /// Queries the state machine. All errors are propagated to the caller.
    fn query(&self, command: Vec<u8>) -> Result<Vec<u8>>;
}

#[derive(Debug, PartialEq)]
/// A driver instruction.
pub enum Instruction {
    /// Abort all pending operations, e.g. due to leader change.
    Abort,
    /// Apply a log entry.
    Apply { entry: Entry },
    /// Notify the given address with the result of applying the entry at the given index.
    Notify { id: Vec<u8>, address: Address, index: u64 },
    /// Query the state machine when the given term and index has been confirmed by vote.
    Query { id: Vec<u8>, address: Address, command: Vec<u8>, term: u64, index: u64, quorum: u64 },
    /// Extend the given server status and return it to the given address.
    Status { id: Vec<u8>, address: Address, status: Box<Status> },
    /// Votes for queries at the given term and commit index.
    Vote { term: u64, index: u64, address: Address },
}

/// A driver query.
struct Query {
    id: Vec<u8>,
    term: u64,
    address: Address,
    command: Vec<u8>,
    quorum: u64,
    votes: HashSet<Address>,
}


#[cfg(test)]
pub mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug)]
    pub struct TestState {
        commands: Arc<Mutex<Vec<Vec<u8>>>>,
        applied_index: Arc<Mutex<u64>>,
    }

    impl TestState {
        pub fn new(applied_index: u64) -> Self {
            Self {
                commands: Arc::new(Mutex::new(Vec::new())),
                applied_index: Arc::new(Mutex::new(applied_index)),
            }
        }

        pub fn list(&self) -> Vec<Vec<u8>> {
            self.commands.lock().unwrap().clone()
        }
    }

    impl State for TestState {
        fn applied_index(&self) -> u64 {
            *self.applied_index.lock().unwrap()
        }

        // Appends the command to the internal commands list.
        fn mutate(&mut self, index: u64, command: Vec<u8>) -> Result<Vec<u8>> {
            self.commands.lock()?.push(command.clone());
            *self.applied_index.lock()? = index;
            Ok(command)
        }

        // Appends the command to the internal commands list.
        fn query(&self, command: Vec<u8>) -> Result<Vec<u8>> {
            self.commands.lock()?.push(command.clone());
            Ok(command)
        }
    }
    //async fn setup() -> Result<(

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_abort() -> Result<()> {
        let (state, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Notify {
            id: vec![0x01],
            index: 1,
            address: Address::Peer("a".into()),
        })?;
        state_tx.send(Instruction::Query {
            id: vec![0x02],
            address: Address::Client,
            command: vec![0xf0],
            term: 1,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Vote { term: 1, index: 1, address: Address::Local })?;
        state_tx.send(Instruction::Abort)?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![
                Message {
                    from: Address::Local,
                    to: Address::Peer("a".into()),
                    term: 0,
                    event: Event::ClientResponse { id: vec![0x01], response: Err(Error::Abort) }
                },
                Message {
                    from: Address::Local,
                    to: Address::Client,
                    term: 0,
                    event: Event::ClientResponse { id: vec![0x02], response: Err(Error::Abort) }
                }
            ]
        );
        assert_eq!(state.list(), Vec::<Vec<u8>>::new());
        assert_eq!(state.applied_index(), 0);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_apply() -> Result<()> {
        let (state, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Notify {
            id: vec![0x01],
            index: 2,
            address: Address::Client,
        })?;
        state_tx.send(Instruction::Apply { entry: Entry { index: 1, term: 1, command: None } })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 2, term: 1, command: Some(vec![0xaf]) },
        })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![Message {
                from: Address::Local,
                to: Address::Client,
                term: 0,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::State(vec![0xaf]))
                }
            }]
        );
        assert_eq!(state.list(), vec![vec![0xaf]]);
        assert_eq!(state.applied_index(), 2);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_query() -> Result<()> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            term: 2,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 2, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { term: 2, index: 1, address: Address::Local })?;
        state_tx.send(Instruction::Vote {
            term: 2,
            index: 1,
            address: Address::Peer("a".into()),
        })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(
            node_rx.collect::<Vec<_>>().await,
            vec![Message {
                from: Address::Local,
                to: Address::Client,
                term: 0,
                event: Event::ClientResponse {
                    id: vec![0x01],
                    response: Ok(Response::State(vec![0xf0]))
                }
            }]
        );

        Ok(())
    }

    // A query for an index submitted in a given term cannot be satisfied by votes below that term.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_query_noterm() -> Result<()> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            term: 2,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 1, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { term: 2, index: 1, address: Address::Local })?;
        state_tx.send(Instruction::Vote {
            term: 1,
            index: 1,
            address: Address::Peer("a".into()),
        })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(node_rx.collect::<Vec<_>>().await, vec![]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn driver_query_noquorum() -> Result<()> {
        let (_, state_tx, node_rx) = setup().await?;

        state_tx.send(Instruction::Query {
            id: vec![0x01],
            address: Address::Client,
            command: vec![0xf0],
            term: 1,
            index: 1,
            quorum: 2,
        })?;
        state_tx.send(Instruction::Apply {
            entry: Entry { index: 1, term: 1, command: Some(vec![0xaf]) },
        })?;
        state_tx.send(Instruction::Vote { term: 1, index: 1, address: Address::Local })?;
        std::mem::drop(state_tx);

        let node_rx = UnboundedReceiverStream::new(node_rx);
        assert_eq!(node_rx.collect::<Vec<_>>().await, vec![]);

        Ok(())
    }
}
