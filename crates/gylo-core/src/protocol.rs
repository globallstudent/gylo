//! Wire protocol between the Rust supervisor and its Python children.
//!
//! Frames are length-prefixed and carried full-duplex: dispatches flow down
//! and completions flow back independently, so neither direction ever waits on
//! a round trip.
//!
//! ```text
//! frame    = u32 body_len (LE) || body
//! body     = u8 kind || kind-specific
//!
//! dispatch = 0x00 || i64 job_id || u16 task_len || task_utf8 || payload
//! complete = 0x01 || i64 job_id || u8 outcome || error_utf8
//! register = 0x02 || messagepack [[name, queue, task, expr, tz, payload], ..]
//!
//! outcome  = 0x00 success | 0x01 failed, retryable | 0x02 failed, terminal
//! ```

const HEADER_BYTES: usize = 4;
const KIND_DISPATCH: u8 = 0x00;
const KIND_COMPLETE: u8 = 0x01;
const KIND_REGISTER: u8 = 0x02;
const OUTCOME_SUCCESS: u8 = 0x00;
const OUTCOME_RETRY: u8 = 0x01;
const OUTCOME_TERMINAL: u8 = 0x02;
const COMPACT_THRESHOLD: usize = 1 << 16;

/// Largest frame body accepted, guarding against a corrupt length prefix
/// causing an unbounded allocation.
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Success,
    /// `retry` is decided by the child from the task's policy, since only it
    /// can see the exception type.
    Failure {
        error: String,
        retry: bool,
    },
}

/// A schedule the child declared, sent once when it connects.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CronRegistration {
    pub name: String,
    pub queue: String,
    pub task: String,
    pub expression: String,
    pub timezone: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Schedules declared by the child, sent once on connect. Carried as
    /// MessagePack rather than hand-framed: it is a variable-shaped structure
    /// sent once per session, so the hot path's byte layout buys nothing here.
    Register(Vec<CronRegistration>),
    Dispatch {
        id: i64,
        task: String,
        payload: Vec<u8>,
    },
    Complete {
        id: i64,
        outcome: Outcome,
    },
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("frame body of {0} bytes exceeds the {MAX_FRAME_BYTES} byte limit")]
    FrameTooLarge(usize),
    #[error("frame body is truncated")]
    Truncated,
    #[error("unknown message kind {0:#04x}")]
    UnknownKind(u8),
    #[error("unknown outcome {0:#04x}")]
    UnknownOutcome(u8),
    #[error("{0} is not valid utf-8")]
    NotUtf8(&'static str),
    #[error("task name of {0} bytes exceeds the 65535 byte limit")]
    TaskNameTooLong(usize),
    #[error("registration payload is malformed: {0}")]
    BadRegistration(String),
}

/// Append `message` to `out` as a complete frame.
///
/// `out` is left byte-for-byte unchanged if encoding fails, so a rejected
/// message cannot leave a partial frame in a batch and desynchronise the
/// stream. Callers batch many frames into one buffer and rely on this.
pub fn encode(message: &Message, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
    let start = out.len();
    match encode_frame(message, out, start) {
        Ok(()) => Ok(()),
        Err(error) => {
            out.truncate(start);
            Err(error)
        }
    }
}

const DISPATCH_HEAD_BYTES: usize = 1 + 8 + 2;
const COMPLETE_HEAD_BYTES: usize = 1 + 8 + 1;

fn encode_frame(message: &Message, out: &mut Vec<u8>, start: usize) -> Result<(), ProtocolError> {
    let registration;
    let body_len = match message {
        Message::Register(entries) => {
            registration = rmp_serde::to_vec(entries)
                .map_err(|error| ProtocolError::BadRegistration(error.to_string()))?;
            1 + registration.len()
        }
        Message::Dispatch { task, payload, .. } => {
            registration = Vec::new();
            u16::try_from(task.len()).map_err(|_| ProtocolError::TaskNameTooLong(task.len()))?;
            DISPATCH_HEAD_BYTES + task.len() + payload.len()
        }
        Message::Complete { outcome, .. } => {
            registration = Vec::new();
            match outcome {
                Outcome::Success => COMPLETE_HEAD_BYTES,
                Outcome::Failure { error, .. } => COMPLETE_HEAD_BYTES + error.len(),
            }
        }
    };
    if body_len > MAX_FRAME_BYTES {
        return Err(ProtocolError::FrameTooLarge(body_len));
    }
    let header = u32::try_from(body_len).map_err(|_| ProtocolError::FrameTooLarge(body_len))?;

    out.reserve(HEADER_BYTES + body_len);
    out.extend_from_slice(&header.to_le_bytes());

    match message {
        Message::Register(_) => {
            out.push(KIND_REGISTER);
            out.extend_from_slice(&registration);
        }
        Message::Dispatch { id, task, payload } => {
            let name_len = u16::try_from(task.len())
                .map_err(|_| ProtocolError::TaskNameTooLong(task.len()))?;
            out.push(KIND_DISPATCH);
            out.extend_from_slice(&id.to_le_bytes());
            out.extend_from_slice(&name_len.to_le_bytes());
            out.extend_from_slice(task.as_bytes());
            out.extend_from_slice(payload);
        }
        Message::Complete { id, outcome } => {
            out.push(KIND_COMPLETE);
            out.extend_from_slice(&id.to_le_bytes());
            match outcome {
                Outcome::Success => out.push(OUTCOME_SUCCESS),
                Outcome::Failure { error, retry } => {
                    out.push(if *retry {
                        OUTCOME_RETRY
                    } else {
                        OUTCOME_TERMINAL
                    });
                    out.extend_from_slice(error.as_bytes());
                }
            }
        }
    }

    debug_assert_eq!(out.len() - start - HEADER_BYTES, body_len);
    Ok(())
}

/// Reassembles messages from arbitrarily chunked reads.
#[derive(Debug, Default)]
pub struct Decoder {
    buf: Vec<u8>,
    pos: usize,
}

impl Decoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn extend(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    fn buffered(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// `None` means the buffer holds a partial frame and needs more bytes.
    pub fn next_message(&mut self) -> Result<Option<Message>, ProtocolError> {
        if self.buffered() < HEADER_BYTES {
            return Ok(None);
        }
        let header: [u8; HEADER_BYTES] = self.buf[self.pos..self.pos + HEADER_BYTES]
            .try_into()
            .expect("slice is HEADER_BYTES long");
        let body_len = u32::from_le_bytes(header) as usize;
        if body_len > MAX_FRAME_BYTES {
            return Err(ProtocolError::FrameTooLarge(body_len));
        }
        if self.buffered() - HEADER_BYTES < body_len {
            return Ok(None);
        }

        let body_start = self.pos + HEADER_BYTES;
        let message = decode_body(&self.buf[body_start..body_start + body_len])?;
        self.pos = body_start + body_len;

        if self.pos >= COMPACT_THRESHOLD {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
        Ok(Some(message))
    }
}

fn decode_body(body: &[u8]) -> Result<Message, ProtocolError> {
    let (&kind, rest) = body.split_first().ok_or(ProtocolError::Truncated)?;
    match kind {
        KIND_DISPATCH => {
            if rest.len() < 10 {
                return Err(ProtocolError::Truncated);
            }
            let id = i64::from_le_bytes(rest[..8].try_into().expect("slice is 8 long"));
            let name_len =
                u16::from_le_bytes(rest[8..10].try_into().expect("slice is 2 long")) as usize;
            let rest = &rest[10..];
            if rest.len() < name_len {
                return Err(ProtocolError::Truncated);
            }
            let task = std::str::from_utf8(&rest[..name_len])
                .map_err(|_| ProtocolError::NotUtf8("task name"))?
                .to_owned();
            Ok(Message::Dispatch {
                id,
                task,
                payload: rest[name_len..].to_vec(),
            })
        }
        KIND_COMPLETE => {
            if rest.len() < 9 {
                return Err(ProtocolError::Truncated);
            }
            let id = i64::from_le_bytes(rest[..8].try_into().expect("slice is 8 long"));
            let outcome = match rest[8] {
                OUTCOME_SUCCESS => Outcome::Success,
                code @ (OUTCOME_RETRY | OUTCOME_TERMINAL) => Outcome::Failure {
                    error: std::str::from_utf8(&rest[9..])
                        .map_err(|_| ProtocolError::NotUtf8("error message"))?
                        .to_owned(),
                    retry: code == OUTCOME_RETRY,
                },
                other => return Err(ProtocolError::UnknownOutcome(other)),
            };
            Ok(Message::Complete { id, outcome })
        }
        KIND_REGISTER => rmp_serde::from_slice(rest)
            .map(Message::Register)
            .map_err(|error| ProtocolError::BadRegistration(error.to_string())),
        other => Err(ProtocolError::UnknownKind(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(message: &Message) -> Message {
        let mut buf = Vec::new();
        encode(message, &mut buf).unwrap();
        let mut decoder = Decoder::new();
        decoder.extend(&buf);
        let decoded = decoder.next_message().unwrap().unwrap();
        assert_eq!(decoder.buffered(), 0);
        decoded
    }

    fn dispatch() -> Message {
        Message::Dispatch {
            id: -42,
            task: "billing.charge".to_owned(),
            payload: vec![0x93, 0x01, 0x02, 0x03],
        }
    }

    #[test]
    fn registration_round_trips() {
        let message = Message::Register(vec![CronRegistration {
            name: "nightly".to_owned(),
            queue: "default".to_owned(),
            task: "reports.nightly".to_owned(),
            expression: "0 3 * * *".to_owned(),
            timezone: "Europe/London".to_owned(),
            payload: vec![0x92, 0x90, 0x80],
        }]);
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn an_empty_registration_round_trips() {
        let message = Message::Register(Vec::new());
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn a_corrupt_registration_body_is_rejected() {
        let mut decoder = Decoder::new();
        decoder.extend(&[3, 0, 0, 0, KIND_REGISTER, 0xC1, 0xC1]);
        assert!(matches!(
            decoder.next_message(),
            Err(ProtocolError::BadRegistration(_))
        ));
    }

    #[test]
    fn dispatch_round_trips() {
        assert_eq!(round_trip(&dispatch()), dispatch());
    }

    #[test]
    fn success_round_trips() {
        let message = Message::Complete {
            id: 7,
            outcome: Outcome::Success,
        };
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn retryable_failure_round_trips() {
        let message = Message::Complete {
            id: 7,
            outcome: Outcome::Failure {
                error: "ValueError: café".to_owned(),
                retry: true,
            },
        };
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn terminal_failure_round_trips() {
        let message = Message::Complete {
            id: 7,
            outcome: Outcome::Failure {
                error: "ValueError: nope".to_owned(),
                retry: false,
            },
        };
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn retryable_and_terminal_use_distinct_codes() {
        let mut retryable = Vec::new();
        let mut terminal = Vec::new();
        encode(
            &Message::Complete {
                id: 1,
                outcome: Outcome::Failure {
                    error: String::new(),
                    retry: true,
                },
            },
            &mut retryable,
        )
        .unwrap();
        encode(
            &Message::Complete {
                id: 1,
                outcome: Outcome::Failure {
                    error: String::new(),
                    retry: false,
                },
            },
            &mut terminal,
        )
        .unwrap();
        assert_ne!(retryable, terminal);
    }

    #[test]
    fn empty_payload_and_task_survive() {
        let message = Message::Dispatch {
            id: 0,
            task: String::new(),
            payload: Vec::new(),
        };
        assert_eq!(round_trip(&message), message);
    }

    #[test]
    fn messages_batched_into_one_buffer_all_decode() {
        let mut buf = Vec::new();
        for id in 0..64 {
            encode(
                &Message::Complete {
                    id,
                    outcome: Outcome::Success,
                },
                &mut buf,
            )
            .unwrap();
        }
        let mut decoder = Decoder::new();
        decoder.extend(&buf);
        for id in 0..64 {
            assert_eq!(
                decoder.next_message().unwrap().unwrap(),
                Message::Complete {
                    id,
                    outcome: Outcome::Success
                }
            );
        }
        assert_eq!(decoder.next_message().unwrap(), None);
    }

    #[test]
    fn frame_split_across_reads_waits_for_the_rest() {
        let mut buf = Vec::new();
        encode(&dispatch(), &mut buf).unwrap();
        let mut decoder = Decoder::new();
        for byte in &buf[..buf.len() - 1] {
            decoder.extend(std::slice::from_ref(byte));
            assert_eq!(decoder.next_message().unwrap(), None);
        }
        decoder.extend(&buf[buf.len() - 1..]);
        assert_eq!(decoder.next_message().unwrap().unwrap(), dispatch());
    }

    #[test]
    fn oversized_length_prefix_is_rejected() {
        let mut decoder = Decoder::new();
        decoder.extend(&u32::MAX.to_le_bytes());
        assert_eq!(
            decoder.next_message(),
            Err(ProtocolError::FrameTooLarge(u32::MAX as usize))
        );
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let mut decoder = Decoder::new();
        decoder.extend(&[1, 0, 0, 0, 0xFE]);
        assert_eq!(
            decoder.next_message(),
            Err(ProtocolError::UnknownKind(0xFE))
        );
    }

    #[test]
    fn unknown_outcome_is_rejected() {
        let mut body = vec![KIND_COMPLETE];
        body.extend_from_slice(&1i64.to_le_bytes());
        body.push(0x7F);
        let mut decoder = Decoder::new();
        decoder.extend(&u32::try_from(body.len()).unwrap().to_le_bytes());
        decoder.extend(&body);
        assert_eq!(
            decoder.next_message(),
            Err(ProtocolError::UnknownOutcome(0x7F))
        );
    }

    #[test]
    fn truncated_body_is_rejected() {
        let mut decoder = Decoder::new();
        decoder.extend(&[3, 0, 0, 0, KIND_DISPATCH, 0, 0]);
        assert_eq!(decoder.next_message(), Err(ProtocolError::Truncated));
    }

    #[test]
    fn task_name_over_u16_is_rejected() {
        let message = Message::Dispatch {
            id: 1,
            task: "x".repeat(65_536),
            payload: Vec::new(),
        };
        let mut buf = Vec::new();
        assert_eq!(
            encode(&message, &mut buf),
            Err(ProtocolError::TaskNameTooLong(65_536))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn rejected_message_leaves_a_batch_intact() {
        let mut buf = Vec::new();
        encode(&dispatch(), &mut buf).unwrap();
        let good = buf.clone();

        let oversized = Message::Dispatch {
            id: 2,
            task: "x".repeat(65_536),
            payload: Vec::new(),
        };
        assert!(encode(&oversized, &mut buf).is_err());
        assert_eq!(buf, good);

        let mut decoder = Decoder::new();
        decoder.extend(&buf);
        assert_eq!(decoder.next_message().unwrap().unwrap(), dispatch());
        assert_eq!(decoder.next_message().unwrap(), None);
    }

    #[test]
    fn decoder_compacts_after_sustained_reads() {
        let mut decoder = Decoder::new();
        let mut buf = Vec::new();
        encode(
            &Message::Complete {
                id: 1,
                outcome: Outcome::Success,
            },
            &mut buf,
        )
        .unwrap();
        for _ in 0..20_000 {
            decoder.extend(&buf);
            decoder.next_message().unwrap().unwrap();
        }
        assert!(decoder.buffered() == 0);
        assert!(decoder.buf.len() < COMPACT_THRESHOLD + buf.len());
    }
}
