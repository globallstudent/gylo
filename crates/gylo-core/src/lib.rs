//! Types shared between the gylo supervisor, the Postgres backend, and the
//! Python worker protocol.

mod capability;
mod protocol;
mod schedule;

pub use capability::{Capabilities, Feature, Unsupported};
pub use protocol::{
    CronRegistration, Decoder, MAX_FRAME_BYTES, Message, Outcome, ProtocolError, encode,
};
pub use schedule::{Schedule, ScheduleError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: i64,
    pub task: String,
    pub payload: Vec<u8>,
    pub attempt: i16,
    pub max_attempts: i16,
    /// Whether this job's completed steps are kept for replay on a retry.
    pub durable: bool,
}
