//! Live behaviour of the cancellable asynchronous generation stream.
//!
//! Every test that needs the on-device model is `#[ignore]`d and **fails**
//! rather than skipping when the model is unavailable, so a captured `SKIP`
//! line can never be mistaken for a pass. Run them explicitly as live
//! evidence:
//!
//! ```text
//! cargo test --features async,macos_26_0,macos_27_0 --test async_stream -- --ignored --test-threads=1
//! ```
//!
//! The deterministic transport behaviour (coalescing, terminals, bounds) is
//! unit-tested in `src/stream.rs` and runs under an ordinary `cargo test`.

#![cfg(all(feature = "async", feature = "macos_26_0"))]

use std::thread::sleep;
use std::time::{Duration, Instant};

use foundation_models::schema::{
    DynamicGenerationProperty, DynamicGenerationSchema, GenerationSchema,
};
use foundation_models::stream::{
    Generation, GenerationHandle, GenerationUpdate, BOUNDED_UPDATE_CAPACITY,
};
use foundation_models::{FMError, GenerationOptions, LanguageModelSession, SystemLanguageModel};

/// Longest the harness waits for native settlement before calling the binding
/// broken. The bridge itself imposes no deadline on a cancelled request's
/// rollback, so this budget is the test's own bound.
const SETTLE_BUDGET: Duration = Duration::from_secs(20);

fn require_model() {
    assert!(
        SystemLanguageModel::is_available(),
        "the on-device model is unavailable ({:?}); this test is live evidence \
         and must not be reported as a pass",
        SystemLanguageModel::availability()
    );
}

const LONG_PROMPT: &str =
    "List twenty distinct seaside towns, one per line, with a short clause about each.";

fn text_session() -> LanguageModelSession {
    LanguageModelSession::with_instructions("Answer in plain prose.").expect("session")
}

/// Drain a stream, asserting append-only integrity, and return the full text.
fn drain(generation: &mut Generation) -> Result<String, FMError> {
    let mut text = String::new();
    while let Some(update) = pollster::block_on(generation.next()) {
        match update? {
            GenerationUpdate::Chunk(delta) => {
                assert!(!delta.is_empty(), "a text delta must carry new characters");
                text.push_str(&delta);
            }
            GenerationUpdate::Replace(snapshot) => {
                // A prefix rewrite must be reported as a replacement, never
                // appended as though it were a delta.
                text = snapshot;
            }
            other => panic!("text stream must not emit {other:?}"),
        }
    }
    Ok(text)
}

fn await_settled(handle: &GenerationHandle) {
    let started = Instant::now();
    pollster::block_on(handle.settled());
    assert!(
        handle.is_settled(),
        "settled() returned while the native task was still live"
    );
    assert!(
        started.elapsed() < SETTLE_BUDGET,
        "native settlement took {:?}, beyond the {SETTLE_BUDGET:?} budget",
        started.elapsed()
    );
}

#[test]
const fn generation_types_are_shareable_across_threads() {
    const fn assert_send_sync<T: Send + Sync>() {}
    const fn assert_send<T: Send>() {}
    assert_send_sync::<GenerationHandle>();
    assert_send::<Generation>();
}

#[test]
#[ignore = "requires a ready Apple Intelligence model on macOS 27"]
fn stream_text_emits_append_only_deltas_and_one_terminal() {
    require_model();
    let session = text_session();
    let mut generation = session
        .stream_text("Name three Norse gods.", GenerationOptions::new())
        .expect("stream start");
    let handle = generation.handle();

    let text = drain(&mut generation).expect("stream completes");
    assert!(!text.trim().is_empty(), "stream produced no text");

    // The terminal is sticky: the stream stays finished.
    assert!(
        pollster::block_on(generation.next()).is_none(),
        "a finished stream must keep returning None"
    );
    await_settled(&handle);

    // The accepted turn is in the transcript exactly once.
    let transcript = session.transcript().expect("transcript");
    let responses = transcript
        .entries()
        .iter()
        .filter(|entry| matches!(entry, foundation_models::TranscriptEntry::Response(_)))
        .count();
    assert_eq!(responses, 1, "exactly one assistant turn must be committed");
}

#[test]
#[ignore = "requires a ready Apple Intelligence model on macOS 27"]
fn cancel_mid_stream_reports_cancelled_then_settles_and_frees_the_session() {
    require_model();
    let session = text_session();
    let mut generation = session
        .stream_text(LONG_PROMPT, GenerationOptions::new())
        .expect("stream start");
    let handle = generation.handle();

    let first = pollster::block_on(generation.next())
        .expect("a stream must deliver an update or a terminal")
        .expect("the first update must not be an error");
    assert!(matches!(
        first,
        GenerationUpdate::Chunk(_) | GenerationUpdate::Replace(_)
    ));

    handle.cancel();
    assert!(handle.is_cancelled());

    // Cancellation is reported as cancellation, not as a fabricated failure,
    // and it is terminal.
    let mut terminal = None;
    while let Some(update) = pollster::block_on(generation.next()) {
        match update {
            Ok(_) => continue,
            Err(error) => {
                terminal = Some(error);
                break;
            }
        }
    }
    assert!(
        matches!(terminal, Some(FMError::Cancelled)),
        "expected FMError::Cancelled, got {terminal:?}"
    );
    assert!(pollster::block_on(generation.next()).is_none());

    await_settled(&handle);

    // The native session is reusable only after settlement; this would trap or
    // report ConcurrentRequests if the cancelled work had not been released.
    let reply = session.respond("Reply with the single word: ready.");
    assert!(reply.is_ok(), "post-cancel reuse failed: {reply:?}");
}

#[test]
#[ignore = "requires a ready Apple Intelligence model on macOS 27"]
fn dropping_a_generation_cancels_and_still_settles() {
    require_model();
    let session = text_session();
    let generation = session
        .stream_text(LONG_PROMPT, GenerationOptions::new())
        .expect("stream start");
    let handle = generation.handle();

    // The consumer walks away without reading or cancelling explicitly.
    drop(generation);
    assert!(handle.is_cancelled(), "drop must request cancellation");
    await_settled(&handle);

    let reply = session.respond("Reply with the single word: ready.");
    assert!(reply.is_ok(), "post-drop reuse failed: {reply:?}");
}

#[test]
#[ignore = "requires a ready Apple Intelligence model on macOS 27"]
fn a_stopped_receiver_keeps_transport_bounded_without_losing_text() {
    require_model();
    let session = text_session();
    let mut generation = session
        .stream_text(LONG_PROMPT, GenerationOptions::new())
        .expect("stream start");
    let handle = generation.handle();

    // Stop reading entirely while the model generates. Nothing native may
    // block on this consumer, and buffering must stay bounded.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !handle.is_settled() && Instant::now() < deadline {
        assert!(
            generation.buffered_updates() <= BOUNDED_UPDATE_CAPACITY,
            "transport buffered {} updates, above the bound",
            generation.buffered_updates()
        );
        sleep(Duration::from_millis(50));
    }

    // Draining afterwards still reconstructs the whole answer: coalesced
    // cumulative snapshots must not drop generated text.
    let text = drain(&mut generation).expect("stream completes");
    assert!(
        text.len() > 40,
        "a stopped receiver lost generated text: {text:?}"
    );
    await_settled(&handle);
}

#[test]
#[ignore = "requires a ready Apple Intelligence model on macOS 27"]
fn cancelling_a_stopped_receiver_releases_native_waiters() {
    require_model();
    let session = text_session();
    let generation = session
        .stream_text(LONG_PROMPT, GenerationOptions::new())
        .expect("stream start");
    let handle = generation.handle();

    // Never read a single update, let the buffer fill, then cancel.
    sleep(Duration::from_millis(600));
    assert!(
        generation.buffered_updates() <= BOUNDED_UPDATE_CAPACITY,
        "transport exceeded its bound while the receiver was idle"
    );
    handle.cancel();
    drop(generation);
    await_settled(&handle);

    let reply = session.respond("Reply with the single word: ready.");
    assert!(reply.is_ok(), "post-cancel reuse failed: {reply:?}");
}

#[test]
#[ignore = "requires a ready Apple Intelligence model on macOS 27"]
fn constrained_stream_emits_structured_snapshots_and_completes() {
    require_model();

    let schema = GenerationSchema::from_dynamic(
        DynamicGenerationSchema::object("Town").with_property(
            "name",
            DynamicGenerationProperty::new(DynamicGenerationSchema::string()),
        ),
        [],
    )
    .expect("schema");

    let session = text_session();
    let mut generation = session
        .stream_generated_async(
            "Name one seaside town.",
            &schema,
            true,
            GenerationOptions::new(),
        )
        .expect("stream start");
    let handle = generation.handle();

    let mut last = None;
    while let Some(update) = pollster::block_on(generation.next()) {
        match update.expect("structured stream must not fail") {
            GenerationUpdate::Snapshot(snapshot) => last = Some(snapshot),
            other => panic!("constrained stream emitted {other:?}"),
        }
    }
    let snapshot = last.expect("at least one structured snapshot");
    assert!(
        snapshot.is_complete,
        "the final structured snapshot must be complete"
    );
    serde_json::from_str::<serde_json::Value>(&snapshot.content_json)
        .expect("the final snapshot must be valid JSON");
    await_settled(&handle);
}

#[test]
#[ignore = "requires a ready Apple Intelligence model on macOS 27"]
fn a_second_stream_waits_for_the_cancelled_one() {
    require_model();
    let session = text_session();
    let first = session
        .stream_text(LONG_PROMPT, GenerationOptions::new())
        .expect("stream start");
    let first_handle = first.handle();
    sleep(Duration::from_millis(300));
    first_handle.cancel();
    drop(first);
    // Starting the next request immediately must not fail: the bridge gate
    // serialises it behind the cancelled request's settlement.
    let mut second = session
        .stream_text(
            "Reply with the single word: ready.",
            GenerationOptions::new(),
        )
        .expect("second stream start");
    let second_handle = second.handle();
    let text = drain(&mut second).expect("second stream completes");
    assert!(!text.trim().is_empty());
    await_settled(&second_handle);
    assert!(first_handle.is_settled());
}
