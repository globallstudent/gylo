//! Types shared between the gylo supervisor, the Postgres backend, and the
//! Python worker protocol.

mod protocol;

pub use protocol::{Decoder, MAX_FRAME_BYTES, Message, Outcome, ProtocolError, encode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: i64,
    pub task: String,
    pub payload: Vec<u8>,
    pub attempt: i16,
    pub max_attempts: i16,
}
