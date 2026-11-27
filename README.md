# foundation-models

Safe, idiomatic Rust bindings for Apple's [FoundationModels](https://developer.apple.com/documentation/foundationmodels) framework — the on-device large language model that ships with Apple Intelligence on macOS 26+.

## Features

- **Sessions and multi-turn chat** — create, restore, inspect, and persist `LanguageModelSession`s
- **Streaming** — text deltas and structured-generation snapshots, synchronously or as a cancellable `Generation` future
- **Cancellation** — dropping a future or a `Generation`, or leaving a stream early, cancels the generation; the next request on the session waits for it to wind down instead of failing with `ConcurrentRequests`, and `GenerationHandle::settled` reports when the native work has actually finished
- **Async API** — executor-agnostic `Future` wrappers for `respond(to:)`, `respond(to:generating:)`, and adapter lifecycle (see `async_api` module)
- **Tool calling** — register Rust callbacks as `FoundationModels` `Tool`s
- **Structured generation** — JSON-schema validation, dynamic schemas, string-choice schemas, array guides, explicit nil-representation helpers, and Rust `Generable` traits
- **Structured content helpers** — typed `GenerationId`, string-backed `Decimal`, `GeneratedContentKind`, and generated-content builders with optional IDs
- **System model configuration** — availability, use cases, guardrails, locales, adapter handles, context size, and async token counting
- **Transcript support** — typed transcript inspection plus raw JSON round-tripping
- **Response / tool definitions** — `ResponseFormat::generating`, inferred `Tool::generable`, and transcript `ToolDefinition` helpers
- **Typed error metadata** — `FMError` accessors for recovery suggestions, refusal helpers, tool-call details, and generation/schema/adapter-error contexts
- **Feedback attachments** — full `LanguageModelFeedback` issue/sentiment support
- **Typed macOS 27 errors** — behind `macos_27_0`, Apple's `LanguageModelError`, `SystemLanguageModel.Error`, `LanguageModelSession.Error`, and `GeneratedContent.ParsingError` map to typed `FMError` variants with their payload fields

## Requirements

- macOS 26.0 or newer (build host **and** runtime); macOS 27.0 and the Xcode 27 SDK for the `macos_27_0` feature
- Xcode 26 SDK

## Installation

These instructions use Bento's fork. The cancellable streaming API and macOS 27 error mapping are not in the published `0.12.0` crate. Commit `Cargo.lock` or replace the branch with an immutable `rev` when consuming the fork.

```toml
[dependencies.foundation-models]
git = "https://github.com/bento-js/foundation-models-rs"
branch = "bento/prompt-api"
features = ["macos_26_0"]
```

## Async API

Enable the `async` feature to get executor-agnostic `Future` wrappers that
work with any async runtime (Tokio, async-std, smol, pollster, …):

```toml
[dependencies.foundation-models]
git = "https://github.com/bento-js/foundation-models-rs"
branch = "bento/prompt-api"
features = ["macos_26_0", "async"]
```

```rust,no_run
use foundation_models::{LanguageModelSession, SystemLanguageModel};
#[cfg(feature = "async")]
use foundation_models::async_api::AsyncSession;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
if !SystemLanguageModel::is_available() { return Ok(()); }
#[cfg(feature = "async")]
pollster::block_on(async {
    let session = LanguageModelSession::new()?;
    let reply = AsyncSession::new(&session).respond("Name three Norse gods.")?.await?;
    println!("{}", reply.content);
    Ok::<(), Box<dyn std::error::Error>>(())
})?;
# Ok(())
# }
```

| Type | Apple API |
|------|-----------|
| `AsyncSession::respond` | `LanguageModelSession.respond(to:)` |
| `AsyncSession::respond_generating` | `LanguageModelSession.respond(to:generating:)` |
| `AsyncAdapter::from_name` | `SystemLanguageModel.Adapter init(name:)` |
| `AsyncAdapter::compatibility` | `Adapter.compatibleAdapterIdentifiers(name:)` |
| `AsyncAdapter::compile` | `SystemLanguageModel.Adapter.compile()` |
| `SystemLanguageModel::token_count` | `SystemLanguageModel.tokenCount(for:)` |
| `ConfiguredSystemLanguageModel::token_count_for_{instructions,tools,schema,transcript}` | `SystemLanguageModel.tokenCount(for:)` overloads (macOS 26.4+) |
| `LanguageModelSession::stream_text` | `LanguageModelSession.streamResponse(to:)` |
| `LanguageModelSession::stream_generated_async` | `LanguageModelSession.streamResponse(to:schema:)` |

Futures own everything they need, so a session may be dropped while one is
pending. Dropping a future cancels its generation, and async errors keep their
`FMError` variant and metadata.

### Cancellable streaming

`stream_text` and `stream_generated_async` return a `Generation`: an ordinary future-based stream that parks no thread.

```rust,no_run
#[cfg(feature = "async")]
use foundation_models::stream::GenerationUpdate;
use foundation_models::{GenerationOptions, LanguageModelSession};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
#[cfg(feature = "async")]
pollster::block_on(async {
    let session = LanguageModelSession::new()?;
    let mut generation =
        session.stream_text("Tell me a haiku about Rust.", GenerationOptions::new())?;
    let handle = generation.handle();

    while let Some(update) = generation.next().await {
        match update? {
            GenerationUpdate::Chunk(delta) => print!("{delta}"),
            GenerationUpdate::Replace(text) => println!("\n(rewritten) {text}"),
            other => println!("{other:?}"),
        }
    }

    // Wait for the native task before reusing or releasing the session.
    handle.settled().await;
    Ok::<(), Box<dyn std::error::Error>>(())
})?;
# Ok(())
# }
```

- `handle.cancel()` requests native cancellation. A generation cancelled before completion reports `FMError::Cancelled`; an already completed generation retains its cached success. Dropping the `Generation` also requests cancellation.
- `handle.settled()` resolves when the Swift task has really finished. For a cancelled request that includes waiting for `FoundationModels` to release it: cancelling makes the stream return normally, and the transcript rollback lands afterwards. Await it before reusing or releasing the session; the handle outlives the stream, so it also works after a drop. The bridge sets no deadline of its own — impose your own bound if you need one, and report a timeout rather than assuming completion.
- Transport buffering is bounded by `stream::BOUNDED_UPDATE_CAPACITY`. Apple's snapshots are cumulative, so a slow consumer coalesces snapshots instead of blocking the model or losing text. Coalescing still cannot hide a rewrite of text you already received: it arrives as `Replace`, never appended as a delta.

`LanguageModelSession::stream` remains the synchronous, callback-based API.

## Quick start

```rust,no_run
use foundation_models::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if !SystemLanguageModel::is_available() {
        eprintln!("Unavailable: {:?}", SystemLanguageModel::availability());
        return Ok(());
    }

    let session = LanguageModelSession::with_instructions(
        "Answer in a single concise sentence.",
    )?;
    let reply = session.respond("Why is the sky blue?")?;
    println!("{reply}");
    Ok(())
}
```

## Tool calling

```rust,no_run
use foundation_models::prelude::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct EchoArgs {
    message: String,
}

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let schema = GenerationSchema::from_dynamic(
    DynamicGenerationSchema::object("EchoArgs").with_property(
        "message",
        DynamicGenerationProperty::new(DynamicGenerationSchema::string()),
    ),
    [],
)?;

let tool = Tool::json("echo", "Echo the provided message.", schema, |args: EchoArgs| {
    Ok(args.message)
});

let session = LanguageModelSession::builder()
    .instructions("Use tools when explicitly asked.")?
    .tool(tool)
    .build()?;

let reply = session.respond_prompt(
    "Use the echo tool exactly once with the message 'hello from Rust'.",
)?;
println!("{reply}");
# Ok(())
# }
```

Tool handlers run on a dispatch queue rather than on Swift's cooperative
thread pool, so a handler may block or call a synchronous `respond`. The Swift
session owns its tools and keeps them alive for as long as it can call them.

## Structured generation

```rust,no_run
use foundation_models::prelude::*;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let schema = GenerationSchema::from_dynamic(
    DynamicGenerationSchema::object("Movie")
        .with_property(
            "title",
            DynamicGenerationProperty::new(DynamicGenerationSchema::string()),
        )
        .with_property(
            "year",
            DynamicGenerationProperty::new(DynamicGenerationSchema::integer()),
        ),
    [],
)?;

let session = LanguageModelSession::new()?;
let response = session.respond_generated(
    "Return JSON for one classic science-fiction movie.",
    &schema,
    true,
)?;
println!("{}", response.json_string()?);
# Ok(())
# }
```

## Coverage-oriented helpers

```rust,no_run
use foundation_models::prelude::*;

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let schema = GenerationSchema::from_dynamic(
    DynamicGenerationSchema::array_of(DynamicGenerationSchema::string()).with_guides([
        GenerationGuide::minimum_count(1),
        GenerationGuide::maximum_count(3),
        GenerationGuide::element(GenerationGuide::string_pattern("^[a-z]+$")),
    ]),
    [],
)?;

let response_format = ResponseFormat::generating::<GeneratedContent>()?;
let tool = Tool::generable("echo", "Echo structured content", |args: GeneratedContent| {
    Ok(args.json_string()?)
})?;

println!("{}", schema.json_schema());
println!("{}", response_format.name());
println!("{}", tool.definition().name);
# Ok(())
# }
```

## Smoke example

```bash
cargo run --example 06_smoke --features macos_26_0
cargo run --example 07_schema_surface --features macos_26_0
```

## Notes

- Swift-only compile-time macros such as `@Generable` and `@Guide` are exposed as Rust runtime traits/builders (`Generable`, `GenerationGuide`, `DynamicGenerationSchema`).
- There is no BackgroundAssets integration: the former `backgroundassets` feature only re-exported the sibling crate, and its one SDK link, `SystemLanguageModel.Adapter.isCompatible(_ assetPack:)`, was deprecated in macOS 26.4 and removed in 27.0. Depend on `backgroundassets` directly.
- Text streams deliver `StreamEvent::Chunk` deltas cut from the full snapshot on UTF-8 boundaries, so concatenating them reproduces the reply even when a grapheme cluster grows across snapshots (ZWJ emoji, skin tones, combining marks). If the model rewrites text it already streamed, `StreamEvent::Replace` carries the whole new text.
- The `macos_27_0` feature requires the Xcode 27 SDK at build time and macOS 27 at runtime. With an older SDK the build prints a warning and the macOS 27 error families stay unmapped rather than silently claiming support.
- `GeneratedContent::generation_id()` returns the attached `GenerationId`. It owns the Swift `GenerationID` it names, which the bridge releases when the last clone is dropped; `best_effort_string()` and `Display` give its description. A `Refusal` taken from an error owns its Swift refusal the same way.
- Typed generation/schema/adapter error metadata plus refusal helpers are available through `FMError::{generation_error_context, adapter_asset_error_context, schema_error_context, recovery_suggestion, failure_reason, refusal, tool_call_error}`.
- Xcode 26.5's `FoundationModels.swiftinterface` does **not** expose standalone `PromptTag`, `Conversation`, `ToolCallingMode`, `SystemPrompt`, `Examples`, `LanguageModelInputContent`, `LanguageModelOutputContent`, or `Streaming` symbols; see [`COVERAGE.md`](COVERAGE.md) for the audited matrix.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at your option.
