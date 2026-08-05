//! Properties the codec must hold for every input, not for chosen examples.
//!
//! The example suite already caught one framing defect by luck — a rejected
//! message leaving a partial header in a shared buffer. A generator searches
//! for the next one instead of waiting for it.

use gylo_core::{CronRegistration, Decoder, Message, Outcome, encode};
use proptest::prelude::*;

fn small_bytes() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..512)
}

fn name() -> impl Strategy<Value = String> {
    // includes multi-byte UTF-8, since lengths on the wire are bytes and a
    // length counted in chars would corrupt everything after the name
    "[a-zA-Z0-9_.:\u{e9}\u{4e16}]{0,40}"
}

fn outcome() -> impl Strategy<Value = Outcome> {
    prop_oneof![
        small_bytes().prop_map(|result| Outcome::Success { result }),
        (".{0,200}", any::<bool>()).prop_map(|(error, retry)| Outcome::Failure { error, retry }),
    ]
}

fn registration() -> impl Strategy<Value = CronRegistration> {
    (name(), name(), name(), name(), name(), small_bytes()).prop_map(
        |(name, queue, task, expression, timezone, payload)| CronRegistration {
            name,
            queue,
            task,
            expression,
            timezone,
            payload,
        },
    )
}

fn message() -> impl Strategy<Value = Message> {
    prop_oneof![
        proptest::collection::vec(registration(), 0..4).prop_map(Message::Register),
        (
            any::<i64>(),
            proptest::collection::vec((name(), small_bytes()), 0..6)
        )
            .prop_map(|(id, steps)| Message::Steps { id, steps }),
        (any::<i64>(), name(), small_bytes()).prop_map(|(id, name, result)| Message::Record {
            id,
            name,
            result
        }),
        (any::<i64>(), name()).prop_map(|(id, name)| Message::Stored { id, name }),
        (any::<i64>(), name(), small_bytes()).prop_map(|(id, task, payload)| Message::Dispatch {
            id,
            task,
            payload
        }),
        (any::<i64>(), outcome()).prop_map(|(id, outcome)| Message::Complete { id, outcome }),
    ]
}

fn decode_all(decoder: &mut Decoder) -> Vec<Message> {
    let mut out = Vec::new();
    while let Ok(Some(message)) = decoder.next_message() {
        out.push(message);
    }
    out
}

proptest! {
    #[test]
    fn every_message_round_trips(message in message()) {
        let mut buffer = Vec::new();
        encode(&message, &mut buffer).expect("generated messages are within limits");

        let mut decoder = Decoder::new();
        decoder.extend(&buffer);
        let decoded = decode_all(&mut decoder);

        prop_assert_eq!(decoded, vec![message]);
        prop_assert_eq!(decoder.next_message().unwrap(), None, "no bytes left over");
    }

    #[test]
    fn a_batch_survives_any_split_points(
        messages in proptest::collection::vec(message(), 1..8),
        cuts in proptest::collection::vec(1usize..4096, 0..12),
    ) {
        let mut buffer = Vec::new();
        for message in &messages {
            encode(message, &mut buffer).expect("within limits");
        }

        // deliver the same bytes in arbitrary-sized reads, because the socket
        // owes the decoder no alignment with frame boundaries
        let mut decoder = Decoder::new();
        let mut decoded = Vec::new();
        let mut offset = 0;
        for cut in cuts {
            let end = (offset + cut).min(buffer.len());
            decoder.extend(&buffer[offset..end]);
            decoded.extend(decode_all(&mut decoder));
            offset = end;
        }
        decoder.extend(&buffer[offset..]);
        decoded.extend(decode_all(&mut decoder));

        prop_assert_eq!(decoded, messages);
    }

    #[test]
    fn arbitrary_bytes_never_panic_the_decoder(
        chunks in proptest::collection::vec(
            proptest::collection::vec(any::<u8>(), 0..600),
            1..6,
        ),
    ) {
        let mut decoder = Decoder::new();
        for chunk in &chunks {
            decoder.extend(chunk);
            // errors are the contract for garbage; panics and infinite loops
            // are not. Cap the drain so a looping decoder fails the test
            // rather than hanging it.
            for _ in 0..10_000 {
                match decoder.next_message() {
                    Ok(Some(_)) => {}
                    Ok(None) | Err(_) => break,
                }
            }
        }
    }

    #[test]
    fn garbage_after_a_valid_frame_does_not_corrupt_it(
        message in message(),
        garbage in proptest::collection::vec(any::<u8>(), 1..300),
    ) {
        let mut buffer = Vec::new();
        encode(&message, &mut buffer).expect("within limits");
        buffer.extend_from_slice(&garbage);

        let mut decoder = Decoder::new();
        decoder.extend(&buffer);

        prop_assert_eq!(decoder.next_message().unwrap(), Some(message));
    }
}

proptest! {
    #[test]
    fn a_rejected_message_never_disturbs_the_batch_around_it(
        before in proptest::collection::vec(message(), 0..4),
        after in proptest::collection::vec(message(), 0..4),
    ) {
        let mut buffer = Vec::new();
        for message in &before {
            encode(message, &mut buffer).expect("within limits");
        }

        let rejected = Message::Dispatch {
            id: 1,
            task: "x".repeat(usize::from(u16::MAX) + 1),
            payload: Vec::new(),
        };
        let held = buffer.clone();
        prop_assert!(encode(&rejected, &mut buffer).is_err());
        prop_assert_eq!(
            &buffer, &held,
            "a failed encode into a shared buffer must leave it byte-for-byte \
             unchanged, or the batch around it desynchronises the stream"
        );

        for message in &after {
            encode(message, &mut buffer).expect("within limits");
        }
        let mut decoder = Decoder::new();
        decoder.extend(&buffer);
        let expected: Vec<Message> =
            before.iter().chain(after.iter()).cloned().collect();
        prop_assert_eq!(decode_all(&mut decoder), expected);
    }
}
