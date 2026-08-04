//! Types shared between the gylo supervisor, the Postgres backend, and the
//! Python worker protocol.

mod job;
mod protocol;

pub use job::{Job, JobState, UnknownState};
pub use protocol::{Decoder, MAX_FRAME_BYTES, Message, Outcome, ProtocolError, encode};
