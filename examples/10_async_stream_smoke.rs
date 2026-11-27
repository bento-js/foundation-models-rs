//! Direct native smoke test for cancellable streaming and typed macOS 27 errors.
//!
//! ```text
//! cargo run --release --features async,macos_26_0,macos_27_0 --example 10_async_stream_smoke
//! ```
//!
//! Unlike the other examples this one **fails** instead of skipping when the
//! on-device model is unavailable: it exists to prove live behaviour, so a
//! silent early return would be worthless as evidence. Every check prints
//! `ok: <what>`; the process exits non-zero on the first failure.

use std::process::ExitCode;
use std::thread::sleep;
use std::time::{Duration, Instant};

use foundation_models::schema::{
    DynamicGenerationProperty, DynamicGenerationSchema, GenerationSchema,
};
use foundation_models::stream::{Generation, GenerationUpdate, BOUNDED_UPDATE_CAPACITY};
use foundation_models::{FMError, GenerationOptions, LanguageModelSession, SystemLanguageModel};

const LONG_PROMPT: &str =
    "List twenty distinct seaside towns, one per line, with a short clause about each.";

fn ok(what: &str) {
    println!("ok: {what}");
}

fn drain(generation: &mut Generation) -> Result<String, FMError> {
    let mut text = String::new();
    let mut chunks = 0usize;
    while let Some(update) = pollster::block_on(generation.next()) {
        match update? {
            GenerationUpdate::Chunk(delta) => {
                chunks += 1;
                text.push_str(&delta);
            }
            GenerationUpdate::Replace(snapshot) => text = snapshot,
            other => {
                return Err(FMError::InvalidArgument(
                    format!("text stream emitted {other:?}").into(),
                ))
            }
        }
    }
    println!("   ({chunks} text deltas, {} bytes)", text.len());
    Ok(text)
}

/// 1. Incremental text streaming reaches a single terminal and settles.
fn check_incremental_stream(session: &LanguageModelSession) -> Result<(), String> {
    let mut generation = session
        .stream_text("Name three Norse gods.", GenerationOptions::new())
        .map_err(|error| format!("stream start failed: {error}"))?;
    let handle = generation.handle();
    let text = drain(&mut generation).map_err(|error| format!("stream failed: {error}"))?;
    if text.trim().is_empty() {
        return Err("stream produced no text".to_owned());
    }
    if pollster::block_on(generation.next()).is_some() {
        return Err("a finished stream kept yielding updates".to_owned());
    }
    pollster::block_on(handle.settled());
    ok("incremental text stream reached a single terminal and settled");
    Ok(())
}

/// 2. Cancellation mid-stream, then reuse of the same native session.
fn check_cancellation(session: &LanguageModelSession) -> Result<(), String> {
    let mut generation = session
        .stream_text(LONG_PROMPT, GenerationOptions::new())
        .map_err(|error| format!("stream start failed: {error}"))?;
    let handle = generation.handle();
    pollster::block_on(generation.next())
        .ok_or_else(|| "stream ended before its first update".to_owned())?
        .map_err(|error| format!("first update was an error: {error}"))?;
    handle.cancel();

    let mut terminal = None;
    while let Some(update) = pollster::block_on(generation.next()) {
        if let Err(error) = update {
            terminal = Some(error);
            break;
        }
    }
    match terminal {
        Some(FMError::Cancelled) => ok("cancellation reported as FMError::Cancelled"),
        other => return Err(format!("expected FMError::Cancelled, got {other:?}")),
    }

    let started = Instant::now();
    pollster::block_on(handle.settled());
    println!("   (settled in {:?})", started.elapsed());
    session
        .respond("Reply with the single word: ready.")
        .map_err(|error| format!("native session unusable after cancellation: {error}"))?;
    ok("native session reusable after cancel + settlement");
    Ok(())
}

/// 3. A receiver that stops reading: bounded transport, then a safe drop.
fn check_stopped_receiver(session: &LanguageModelSession) -> Result<(), String> {
    let generation = session
        .stream_text(LONG_PROMPT, GenerationOptions::new())
        .map_err(|error| format!("stream start failed: {error}"))?;
    let handle = generation.handle();
    sleep(Duration::from_millis(600));

    let buffered = generation.buffered_updates();
    if buffered > BOUNDED_UPDATE_CAPACITY {
        return Err(format!(
            "transport buffered {buffered} updates, above the bound of {BOUNDED_UPDATE_CAPACITY}"
        ));
    }
    println!("   ({buffered} updates buffered while the receiver was idle)");

    drop(generation);
    if !handle.is_cancelled() {
        return Err("dropping a generation did not request cancellation".to_owned());
    }
    pollster::block_on(handle.settled());
    session
        .respond("Reply with the single word: ready.")
        .map_err(|error| format!("native session unusable after drop: {error}"))?;
    ok("stopped receiver stayed bounded, dropped safely and released native waiters");
    Ok(())
}

/// 4. Constrained streaming buffers snapshots into one validated JSON result.
fn check_constrained_stream(session: &LanguageModelSession) -> Result<(), String> {
    let schema = GenerationSchema::from_dynamic(
        DynamicGenerationSchema::object("Town").with_property(
            "name",
            DynamicGenerationProperty::new(DynamicGenerationSchema::string()),
        ),
        [],
    )
    .map_err(|error| format!("schema translation failed: {error}"))?;

    let mut generation = session
        .stream_generated_async(
            "Name one seaside town.",
            &schema,
            true,
            GenerationOptions::new(),
        )
        .map_err(|error| format!("constrained stream start failed: {error}"))?;
    let handle = generation.handle();

    let mut last = None;
    while let Some(update) = pollster::block_on(generation.next()) {
        match update.map_err(|error| format!("constrained stream failed: {error}"))? {
            GenerationUpdate::Snapshot(snapshot) => last = Some(snapshot),
            other => return Err(format!("constrained stream emitted {other:?}")),
        }
    }
    let snapshot = last.ok_or_else(|| "constrained stream produced no snapshot".to_owned())?;
    if !snapshot.is_complete {
        return Err("final structured snapshot was incomplete".to_owned());
    }
    serde_json::from_str::<serde_json::Value>(&snapshot.content_json)
        .map_err(|error| format!("final snapshot is not valid JSON: {error}"))?;
    pollster::block_on(handle.settled());
    println!("   ({})", snapshot.content_json);
    ok("constrained stream produced one validated JSON result");
    Ok(())
}

/// Context exhaustion can be provoked reliably; other native error categories
/// are exercised with authentic Apple error values in the Swift tests.
fn check_context_overflow(session: &LanguageModelSession) -> Result<(), String> {
    let context_size = SystemLanguageModel::context_size();
    let oversized = "seaside town, ".repeat(context_size.max(8192) * 2);
    let mut generation = session
        .stream_text(oversized.as_str(), GenerationOptions::new())
        .map_err(|error| format!("oversized stream start failed: {error}"))?;
    let handle = generation.handle();

    let mut overflow = None;
    while let Some(update) = pollster::block_on(generation.next()) {
        if let Err(error) = update {
            overflow = Some(error);
            break;
        }
    }
    pollster::block_on(handle.settled());
    match overflow {
        Some(FMError::ContextWindowExceeded(_)) => {}
        other => {
            return Err(format!(
                "an oversized prompt reported {other:?} instead of \
                 FMError::ContextWindowExceeded (native context size {context_size})"
            ))
        }
    }
    ok("context exhaustion survived typed conversion as ContextWindowExceeded");
    Ok(())
}

fn run() -> Result<(), String> {
    if !SystemLanguageModel::is_available() {
        return Err(format!(
            "the on-device model is unavailable ({:?}); this smoke test proves live \
             behaviour and must not be reported as a pass",
            SystemLanguageModel::availability()
        ));
    }
    ok("model available");

    let session = LanguageModelSession::with_instructions("Answer in plain prose.")
        .map_err(|error| format!("session creation failed: {error}"))?;

    check_incremental_stream(&session)?;
    check_cancellation(&session)?;
    check_stopped_receiver(&session)?;
    check_constrained_stream(&session)?;
    check_context_overflow(&session)
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => {
            println!("PASS: native streaming smoke test");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("FAIL: {message}");
            ExitCode::FAILURE
        }
    }
}
