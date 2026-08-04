use std::fmt;
use std::str::FromStr;

/// Lifecycle state of a job, mirroring the `gylo_job_state` enum in Postgres.
///
/// There is no scheduled or retryable state: a job waiting to run later, for
/// either reason, is `Available` with a future `scheduled_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JobState {
    Available,
    Running,
    Completed,
    /// Retries exhausted; the dead-letter state.
    Discarded,
    Cancelled,
}

impl JobState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Discarded => "discarded",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_final(self) -> bool {
        matches!(self, Self::Completed | Self::Discarded | Self::Cancelled)
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("unknown job state {0:?}")]
pub struct UnknownState(pub String);

impl FromStr for JobState {
    type Err = UnknownState;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "available" => Ok(Self::Available),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "discarded" => Ok(Self::Discarded),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(UnknownState(other.to_owned())),
        }
    }
}

/// A leased job on its way to a Python worker.
///
/// `payload` is opaque here: the supervisor moves the bytes without decoding
/// them, since only the Python child needs real objects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: i64,
    pub task: String,
    pub payload: Vec<u8>,
    pub attempt: i16,
    pub max_attempts: i16,
}

impl Job {
    pub const fn is_last_attempt(&self) -> bool {
        self.attempt >= self.max_attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_labels_round_trip() {
        for state in [
            JobState::Available,
            JobState::Running,
            JobState::Completed,
            JobState::Discarded,
            JobState::Cancelled,
        ] {
            assert_eq!(state.as_str().parse::<JobState>().unwrap(), state);
        }
    }

    #[test]
    fn unknown_state_is_rejected() {
        assert!("retryable".parse::<JobState>().is_err());
    }

    #[test]
    fn only_terminal_states_are_final() {
        assert!(!JobState::Available.is_final());
        assert!(!JobState::Running.is_final());
        assert!(JobState::Completed.is_final());
        assert!(JobState::Discarded.is_final());
        assert!(JobState::Cancelled.is_final());
    }

    #[test]
    fn last_attempt_is_detected() {
        let job = Job {
            id: 1,
            task: "t".to_owned(),
            payload: Vec::new(),
            attempt: 3,
            max_attempts: 3,
        };
        assert!(job.is_last_attempt());
        assert!(!Job { attempt: 2, ..job }.is_last_attempt());
    }
}
