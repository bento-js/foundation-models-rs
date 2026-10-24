# Fork patch record

This fork of [`foundation-models`](https://github.com/doom-fish/foundation-models-rs) branches from the published `0.12.0` release, commit `cf7ab5592b985b0766a3c0ef83e4625c76bef5eb`. It exists because two gaps in that release block a safe host integration: streaming cannot be consumed asynchronously or cancelled by its consumer, and macOS 27's typed `LanguageModelError` collapses into `FMError::Unknown`.

Every change below is deliberately narrow and self-contained so it can be offered upstream as separate commits. No public API was removed or renamed, and no cloud, provider, or model-selection surface was added.

## 1. Registry resolution for `doom-fish-utils`

`Cargo.toml` declared `doom-fish-utils = { path = "../doom-fish-utils", version = ">=0.4.1, <0.5" }`. The sibling path exists only in the author's workspace, so a clone of the repository fails during manifest resolution and a `git`/`rev` dependency on the repository can never resolve. The `path` key is removed; the published crate resolves from the registry.

Upstream value: a clone of the repository builds. Upstream may prefer to restore the path key behind a local workspace member instead.

## 2. Asynchronous, cancellable generation streams

New module `src/stream.rs` (Cargo feature `async`) plus two methods on `LanguageModelSession`:

```rust
let mut generation = session.stream_text(prompt, GenerationOptions::new())?;
let handle = generation.handle();
while let Some(update) = generation.next().await {
    match update? { /* Chunk / Replace / Snapshot */ }
}
handle.settled().await;
```

- `Generation::next` is an ordinary `Future`; it parks no thread and needs no async runtime. The crate stays executor-agnostic: the implementation uses `std` wakers only and adds no dependency.
- `GenerationHandle` is `Clone + Send + Sync + 'static`. It carries `cancel()`, `is_cancelled()`, `settled()` and `is_settled()`, so a consumer can drop the stream and still await native settlement — the hook a host needs for a truthful `dispose()`.
- Each `settled()` waiter owns a slot and clears it when its future is dropped, with slots reused. A host that gives up on settlement — a shutdown timeout that retries — therefore releases its waker, and everything that waker owns, at once rather than when the native task eventually finishes, which is exactly when a rollback may still be pending.
- Every waker call-out — clone, drop and wake — happens with no transport or settlement lock held. `RawWakerVTable` entries are arbitrary consumer code and may ask the handle questions, so running one under a lock can deadlock the native callback that is trying to deliver a terminal. Registration clones first and then re-checks the state under the lock, so a snapshot or terminal arriving in between cannot swallow a wake; a stale waker is taken out under the lock and dropped after it is released. The path that merely reports data already present clones nothing.
- Transport is bounded by `BOUNDED_UPDATE_CAPACITY` snapshots. Apple's snapshots are cumulative, so a full buffer coalesces the newest snapshot into the pending one instead of blocking Swift or discarding text. The delivered text lives in the transport, so deltas — and rewrites — are judged against exactly what the consumer has been handed. Coalescing therefore cannot hide a rewrite: if a discarded snapshot did not extend the delivered text, a sticky flag makes the next text update a `GenerationUpdate::Replace`, even when a later snapshot restores the prefix. That costs one string and one bool, with no extra queue slots and no blocking.
- Dropping a `Generation` cancels the native task. The callback owns its own reference to the shared state, so a late callback can never touch released memory, and the dropping thread never blocks.
- The existing blocking `stream`, `stream_prompt`, `stream_generated` and `StreamEvent` API is untouched.

Upstream value: this is the `AsyncSequence` ("Tier 2") stream that `src/async_api.rs` documents as deferred.

## 3. Native terminal settlement

A cancelled stream's terminal callback arrives _before_ FoundationModels has released the request, and cancelling makes the stream return normally rather than throw. A direct macOS 27.0 probe saw `isResponding` already false while the transcript still held the cancelled request, with the entries disappearing about ten milliseconds later — so neither signal alone is proof. Rust had no way to observe the end of that cleanup at all, so a consumer could not tell when the native session was safe to reuse or release.

- Swift gains `fm_task_await_settled(task, context, callback)` and `fm_task_is_settled(task)` in `swift-bridge/Sources/FoundationModelsBridge/Tasks.swift`. The former invokes the callback once the task body — including the cancelled-stream rollback and the request gate's release — has finished; a null handle counts as settled, because the bridge only returns null after delivering a terminal callback.
- `FMTaskHandle` now clears its task reference when the body completes. That breaks the retain cycle between the handle and its `Task`, which previously leaked one handle plus one task per request, and makes `wait()` return immediately for an already-finished task.
- `SwiftTask` exposes cancellation and settlement registration to the crate.
- The outcome of a stream is decided by its own cached terminal result, `ResponseStream.collect()`, not by `Task.isCancelled` sampled after the loop. A direct macOS 27.0 probe showed why: a cancellation delivered after the generation finished still sets `Task.isCancelled`, while `collect()` returns the cached success and the response stays legitimately committed — the old classification then waited for a rollback that could never happen, forever. `collect()` is consulted even when iteration threw, so an aborted iteration is still diagnosed from the framework's own answer. Only a genuine `CancellationError` leaves cleanup owed, and the bridge waits for it.
- When iteration stopped before the final snapshot but `collect()` reports success, the cached full response is delivered before the success terminal, so a consumer told "success" can never be holding a prefix.
- Bridge encoding failures stay distinct from native outcomes: they are reported as the terminal and never replaced by, or swallowed into, a native success.

Upstream value: fixes a per-request leak and gives any consumer a settlement signal; `fm_task_await_settled` is additive FFI.

## 4. Typed macOS 27 `LanguageModelError`

New Cargo feature `macos_27_0`, following the existing `macos_26_0` convention, plus the `FOUNDATION_MODELS_HAS_MACOS27_SDK` Swift define wired through `build.rs`.

`mapError` in `swift-bridge/Sources/FoundationModelsBridge/FoundationModels.swift` now recognises, ahead of the deprecated `GenerationError` cases:

| Apple error | Status code | `FMError` |
| --- | --- | --- |
| `LanguageModelError.contextSizeExceeded` | `CONTEXT_WINDOW_EXCEEDED` | `ContextWindowExceeded` |
| `LanguageModelError.rateLimited` | `RATE_LIMITED` | `RateLimited` |
| `LanguageModelError.guardrailViolation` | `GUARDRAIL_VIOLATION` | `GuardrailViolation` |
| `LanguageModelError.refusal` | `REFUSAL` | `Refusal` |
| `LanguageModelError.unsupportedCapability` | `UNSUPPORTED_CAPABILITY` | `UnsupportedCapability` |
| `LanguageModelError.unsupportedTranscriptContent` | `UNSUPPORTED_TRANSCRIPT_CONTENT` | `UnsupportedTranscriptContent` |
| `LanguageModelError.unsupportedGenerationGuide` | `UNSUPPORTED_GUIDE` | `UnsupportedGuide` |
| `LanguageModelError.unsupportedLanguageOrLocale` | `UNSUPPORTED_LANGUAGE` | `UnsupportedLanguage` |
| `LanguageModelError.timeout` | `TIMEOUT` | `Timeout` |
| `SystemLanguageModel.Error.assetsUnavailable` | `ASSETS_UNAVAILABLE` | `AssetsUnavailable` |
| `LanguageModelSession.Error.concurrentRequests` | `CONCURRENT_REQUESTS` | `ConcurrentRequests` |
| `LanguageModelSession.Error.transcriptMutationWhileResponding` | `TRANSCRIPT_MUTATION_WHILE_RESPONDING` | `TranscriptMutationWhileResponding` |
| `GeneratedContent.ParsingError` | `DECODING_FAILURE` | `DecodingFailure` |

Apple's typed payload fields travel with the error instead of being flattened into prose: `FMError::language_model_error_details()` reports `context_size`, `token_count` and `capability` — the facts a caller needs to report a quota failure with real numbers or name the capability it cannot use.

Four `FMError` variants and four `ffi::status` constants are added. `FMError` is `#[non_exhaustive]`, so this is additive for consumers.

Upstream value: closes the `COVERAGE.md` gap that recorded macOS 27 errors as mapping to `FMError::Unknown`.

## 5. `Send` futures for configured-model token counting

`ConfiguredSystemLanguageModel` holds a retained Swift object. Swift reference counting is thread-safe and Rust never dereferences the pointer, so the type now carries documented `Send`/`Sync` impls, matching `LanguageModelSession`. The `#[allow(clippy::future_not_send)]` attributes on `token_count`, `token_count_for_instructions`, `token_count_for_tools`, `token_count_for_schema` and `token_count_for_transcript` are removed: those futures are now `Send`, so a host can hold them across an `await` inside a spawned task.

## 6. Bridge-level tests

`swift-bridge/Tests/FoundationModelsBridgeTests/ErrorMappingTests.swift` builds _authentic_ `LanguageModelError`, `SystemLanguageModel.Error`, `LanguageModelSession.Error` and `GeneratedContent.ParsingError` values with Apple's public initializers and runs them through the bridge's real `mapError`. Apple only throws most of them under conditions a caller cannot provoke (rate limiting, timeouts, unsupported capabilities), so this is the only honest way to verify the translation. The package gains a test target; `swift build` — and therefore `cargo build` — never compiles it.

No production API exists for testing: there is no error factory, no probe export and no fake backend.

`TaskHandleTests.swift` pins the handle's lifecycle races through observable behaviour: a handle whose body already finished must not adopt a task attached afterwards, proven by cancelling the handle and showing that a live externally-owned task is left untouched; a cancellation requested before the task exists must still reach it; every settlement waiter runs exactly once; and a null handle answers immediately. `attach` and `complete` are module-internal rather than file-private so the test target can drive those races directly; neither is public.

## 7. Test policy for live and inventory assertions

- `tests/async_stream.rs` holds the model-dependent stream behaviour. Every one of those tests is `#[ignore]`d and _asserts_ model availability instead of returning early, because `cargo test` captures stdout on success and a printed `SKIP` would read as a pass. Run them deliberately with `-- --ignored`. The transport's deterministic behaviour — coalescing, rewrite preservation, terminals, bounds — is unit-tested in `src/stream.rs` and runs under an ordinary `cargo test` with no model and no FFI.
- The bridge's own live test (`StreamOutcomeTests`) follows the same rule in the form XCTest allows: with no model it reports _Skipped_, which Swift prints distinctly rather than as a pass, and a run that claims to be live evidence sets `FM_REQUIRE_MODEL=1`, which turns an unavailable model into a failure. A skip can therefore never be counted as native proof.
- Remove `tests/api_coverage.rs` and its otherwise-unused `regex-lite` dependency. The harness inferred coverage from symbol names appearing in source, not from native behavior. macOS 27 added `GenerationOptions.toolCallingMode`, `ResponseStream.Snapshot.usage`, and a real `ToolCallingMode`, invalidating three inventory assertions without changing this crate's behavior. The remaining assertions used the same unsound method, so the whole harness is removed rather than re-pinned or satisfied with out-of-scope APIs. Native behavior remains covered by the stream tests, typed error tests, and live smoke.
- `stream_text` was expected to reject a prompt containing an interior NUL. That imported a raw-C-string expectation into a JSON API: the request is serialised with `serde_json`, which escapes NUL as `\u0000`, so the C string handed to Swift contains no interior NUL and the prompt arrives intact. The input is encoded correctly, so the incorrect expectation was deleted rather than satisfied by adding a rejection nobody asked for. No JSON-serialiser test replaced it.
- `generation_error_metadata_round_trips` no longer clones the error to compare a derived `Clone` against itself. The clone was tautological, and `freed_errors_do_not_leak_metadata_into_new_ones` already proves that cloning carries metadata without aliasing it.

## Verification

Run on macOS 27 with the Xcode 27 SDK:

```text
swift test --package-path swift-bridge \
  -Xswiftc -DFOUNDATION_MODELS_HAS_MACOS26_SDK \
  -Xswiftc -DFOUNDATION_MODELS_HAS_MACOS27_SDK
FM_REQUIRE_MODEL=1 swift test --package-path swift-bridge \
  -Xswiftc -DFOUNDATION_MODELS_HAS_MACOS26_SDK \
  -Xswiftc -DFOUNDATION_MODELS_HAS_MACOS27_SDK
cargo test --features async,macos_26_0,macos_27_0
cargo test --features async,macos_26_0,macos_27_0 -- --ignored --test-threads=1
cargo run --release --features async,macos_26_0,macos_27_0 --example 10_async_stream_smoke
cargo clippy --all-features --all-targets -- -D warnings
```

`tests/async_stream.rs` is live evidence and is only run on request; `examples/10_async_stream_smoke.rs` is the same evidence as a program: it exits non-zero instead of skipping, and it proves the macOS 27 quota mapping against an error the framework really throws by overflowing the context window. The first `swift test` line is the deterministic suite plus a visible skip; the `FM_REQUIRE_MODEL=1` line is the live one and fails rather than skipping.
