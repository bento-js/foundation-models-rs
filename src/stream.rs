//! Asynchronous, cancellable generation streams.
//!
//! Enabled with the `async` Cargo feature. The implementation uses only `std`
//! wakers, so it works with any executor and adds no dependency.
//!
//! [`LanguageModelSession::stream_text`](crate::LanguageModelSession::stream_text)
//! and
//! [`stream_generated_async`](crate::LanguageModelSession::stream_generated_async)
//! return a [`Generation`]. Polling it parks no thread, dropping it cancels the
//! native work, and [`GenerationHandle::settled`] reports when that work has
//! really finished — which is later than the stream's last callback, because
//! `FoundationModels` still has to roll a cancelled request's transcript back.
//!
//! ```rust,no_run
//! use foundation_models::stream::GenerationUpdate;
//! use foundation_models::{GenerationOptions, LanguageModelSession};
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! pollster::block_on(async {
//!     let session = LanguageModelSession::new()?;
//!     let mut generation =
//!         session.stream_text("Name three Norse gods.", GenerationOptions::new())?;
//!     let handle = generation.handle();
//!
//!     while let Some(update) = generation.next().await {
//!         match update? {
//!             GenerationUpdate::Chunk(delta) => print!("{delta}"),
//!             GenerationUpdate::Replace(text) => println!("\n(rewritten) {text}"),
//!             other => println!("{other:?}"),
//!         }
//!     }
//!
//!     // Wait for the native task before reusing or releasing the session.
//!     handle.settled().await;
//!     Ok::<(), Box<dyn std::error::Error>>(())
//! })?;
//! # Ok(())
//! # }
//! ```

use core::ffi::{c_char, c_void};
use core::future::Future;
use core::pin::Pin;
use core::ptr;
use core::sync::atomic::{AtomicBool, Ordering};
use core::task::{Context, Poll, Waker};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use doom_fish_utils::panic_safe::catch_user_panic;

use crate::error::{from_swift_message, take_bridge_string, FMError};
use crate::ffi;
use crate::session::{
    BridgeStructuredSnapshot, BridgeTextStreamSnapshot, StructuredStreamSnapshot,
};
use crate::task::SwiftTask;

/// How many native snapshots the transport holds for a consumer that reads
/// slower than the model generates.
///
/// Apple's snapshots are cumulative, so a full buffer coalesces the newest
/// snapshot into the one it would have followed: memory stays bounded by a few
/// snapshots, no generated text is lost, and nothing native ever blocks on the
/// consumer.
///
/// Coalescing never hides a rewrite. Deltas are judged against the text the
/// consumer has actually been handed, and discarding a snapshot that did not
/// extend that text is remembered, so the next update is a
/// [`GenerationUpdate::Replace`] even if a later snapshot restores the prefix.
pub const BOUNDED_UPDATE_CAPACITY: usize = 4;

/// One update from a running generation.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum GenerationUpdate {
    /// Text appended since the previous update. Concatenating every `Chunk`
    /// after the latest `Replace` reconstructs the response.
    Chunk(String),
    /// The model rewrote text it had already emitted; this is the full
    /// replacement text, not a delta.
    Replace(String),
    /// A structured snapshot from a constrained generation. Snapshots can
    /// rewrite earlier content, so they are not append-only.
    Snapshot(StructuredStreamSnapshot),
}

enum Snapshot {
    Text(String),
    Structured(StructuredStreamSnapshot),
}

struct Queue {
    pending: VecDeque<Snapshot>,
    /// The text this consumer has actually been handed. Deltas and rewrites
    /// are judged against it, so coalescing cannot change what counts as a
    /// rewrite.
    delivered: String,
    /// Set when coalescing discarded a snapshot that did not extend
    /// `delivered`. Sticky until the next text update reports it.
    rewrote_delivered: bool,
    terminal: Option<Result<(), FMError>>,
    finished: bool,
    waker: Option<Waker>,
}

impl Queue {
    const fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            delivered: String::new(),
            rewrote_delivered: false,
            terminal: None,
            finished: false,
            waker: None,
        }
    }

    fn push(&mut self, snapshot: Snapshot) {
        if self.pending.len() >= BOUNDED_UPDATE_CAPACITY {
            // Cumulative snapshots supersede their predecessor, so dropping the
            // one this snapshot would have followed loses no characters.
            if let Some(discarded) = self.pending.pop_back() {
                self.note_discarded(&discarded);
            }
        }
        self.pending.push_back(snapshot);
    }

    /// Keep the one fact coalescing would otherwise destroy: that the model
    /// retracted text the consumer already holds. The discarded snapshot's
    /// characters are superseded, but a snapshot that does not extend
    /// `delivered` is evidence of a rewrite, and the consumer has to learn
    /// about it even if a later snapshot restores the prefix.
    ///
    /// Structured snapshots carry no append-only promise, so they are exempt.
    fn note_discarded(&mut self, discarded: &Snapshot) {
        if let Snapshot::Text(content) = discarded {
            if !content.starts_with(self.delivered.as_str()) {
                self.rewrote_delivered = true;
            }
        }
    }

    /// Turn one native snapshot into an update, or `None` when the snapshot
    /// added nothing the consumer has not already seen.
    fn consume(&mut self, snapshot: Snapshot) -> Option<GenerationUpdate> {
        match snapshot {
            Snapshot::Structured(snapshot) => Some(GenerationUpdate::Snapshot(snapshot)),
            Snapshot::Text(content) => {
                if self.rewrote_delivered {
                    self.rewrote_delivered = false;
                    return Some(self.replace_with(content));
                }
                if content.starts_with(self.delivered.as_str()) {
                    let appended_from = self.delivered.len();
                    if content.len() == appended_from {
                        return None;
                    }
                    self.delivered = content;
                    return Some(GenerationUpdate::Chunk(
                        self.delivered[appended_from..].to_owned(),
                    ));
                }
                // The model rewrote text the consumer already has. Report the
                // replacement instead of appending it as though it were a delta.
                Some(self.replace_with(content))
            }
        }
    }

    fn replace_with(&mut self, content: String) -> GenerationUpdate {
        self.delivered.clear();
        self.delivered.push_str(&content);
        GenerationUpdate::Replace(content)
    }

    fn settle(&mut self, result: Result<(), FMError>) {
        if self.terminal.is_none() {
            self.terminal = Some(result);
        }
    }
}

/// Settlement state plus one slot per live waiter.
///
/// A waiter owns its slot and clears it when its future is dropped, so a
/// caller that gives up — a host shutdown timeout, say — releases its waker
/// immediately instead of leaving it owned here until the native task
/// finishes. Slots are reused, so the vector stays as long as the number of
/// waiters that are alive at once, not the number that ever existed.
struct Settlement {
    settled: bool,
    waiters: Vec<Option<Waker>>,
}

impl Settlement {
    /// Take a free slot, or add one.
    ///
    /// A free slot holds no waker, so nothing is dropped here.
    fn register(&mut self, waker: Waker) -> usize {
        if let Some(index) = self.waiters.iter().position(Option::is_none) {
            self.waiters[index] = Some(waker);
            return index;
        }
        self.waiters.push(Some(waker));
        self.waiters.len() - 1
    }

    /// Give up a slot, handing its waker back to be dropped without this lock
    /// held: dropping a waker runs the consumer's code, and that code may ask
    /// the transport questions.
    #[must_use]
    fn release(&mut self, index: usize) -> Option<Waker> {
        let stale = self.waiters.get_mut(index).and_then(Option::take);
        // Only empty slots are popped, so no waker is dropped by trimming.
        while self.waiters.last().is_some_and(Option::is_none) {
            self.waiters.pop();
        }
        stale
    }
}

struct Shared {
    queue: Mutex<Queue>,
    settlement: Mutex<Settlement>,
    task: Mutex<Option<SwiftTask>>,
    cancelled: AtomicBool,
}

impl Shared {
    fn new() -> Self {
        Self {
            queue: Mutex::new(Queue::new()),
            settlement: Mutex::new(Settlement {
                settled: false,
                waiters: Vec::new(),
            }),
            task: Mutex::new(None),
            cancelled: AtomicBool::new(false),
        }
    }

    fn queue(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn task(&self) -> MutexGuard<'_, Option<SwiftTask>> {
        self.task.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn settlement(&self) -> MutexGuard<'_, Settlement> {
        self.settlement
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Adopt the task handle, honouring a cancellation that arrived while the
    /// request was still starting.
    fn attach(&self, task: Option<SwiftTask>) {
        let mut slot = self.task();
        if self.cancelled.load(Ordering::Acquire) {
            if let Some(task) = task.as_ref() {
                task.cancel();
            }
        }
        *slot = task;
    }

    fn task_ptr(&self) -> *mut c_void {
        self.task()
            .as_ref()
            .map_or_else(ptr::null_mut, SwiftTask::as_raw)
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(task) = self.task().as_ref() {
            task.cancel();
        }
    }

    fn deliver(
        &self,
        payload: Option<String>,
        done: bool,
        status: i32,
        parse: fn(&str) -> Result<Snapshot, FMError>,
    ) {
        let mut queue = self.queue();
        if status == ffi::status::OK {
            if let Some(payload) = payload {
                match parse(&payload) {
                    Ok(snapshot) => queue.push(snapshot),
                    Err(error) => queue.settle(Err(error)),
                }
            }
            if done {
                queue.settle(Ok(()));
            }
        } else {
            queue.settle(Err(from_swift_message(status, payload.unwrap_or_default())));
        }
        let waker = queue.waker.take();
        drop(queue);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn mark_settled(&self) {
        let mut settlement = self.settlement();
        settlement.settled = true;
        let waiters = core::mem::take(&mut settlement.waiters);
        drop(settlement);
        for waker in waiters.into_iter().flatten() {
            waker.wake();
        }
    }

    /// Poll on behalf of one waiter, which owns `slot` until it is dropped.
    ///
    /// Cloning and dropping a waker both run the consumer's code, so neither
    /// happens while this lock is held. Settlement is re-checked after the
    /// clone, because it can land in between.
    fn poll_settled(&self, slot: &mut Option<usize>, cx: &Context<'_>) -> Poll<()> {
        let already_settled = self.settlement().settled;
        if already_settled {
            // `mark_settled` already emptied every slot; forget ours so the
            // future's `Drop` does not release someone else's. Nothing is
            // cloned on this path.
            *slot = None;
            return Poll::Ready(());
        }

        let waker = cx.waker().clone();
        let mut stale = None;
        let mut settled = false;
        {
            let mut settlement = self.settlement();
            let registered = (*slot).filter(|index| *index < settlement.waiters.len());
            if settlement.settled {
                *slot = None;
                settled = true;
            } else if let Some(index) = registered {
                stale = settlement.waiters[index].replace(waker);
            } else {
                *slot = Some(settlement.register(waker));
            }
        }
        drop(stale);

        if settled {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    fn release_settlement_slot(&self, slot: usize) {
        let stale = self.settlement().release(slot);
        drop(stale);
    }
}

fn parse_text(payload: &str) -> Result<Snapshot, FMError> {
    serde_json::from_str::<BridgeTextStreamSnapshot>(payload)
        .map(|snapshot| Snapshot::Text(snapshot.content))
        .map_err(|error| FMError::DecodingFailure(error.to_string().into()))
}

fn parse_structured(payload: &str) -> Result<Snapshot, FMError> {
    serde_json::from_str::<BridgeStructuredSnapshot>(payload)
        .map(|snapshot| {
            Snapshot::Structured(StructuredStreamSnapshot {
                content_json: snapshot.content.json,
                raw_content_json: snapshot.raw_content.json,
                is_complete: snapshot.is_complete,
            })
        })
        .map_err(|error| FMError::DecodingFailure(error.to_string().into()))
}

// SAFETY: `context` is an `Arc<Shared>` raw pointer created by `start`. Swift
// delivers exactly one terminal callback (`done == true` or `status != OK`),
// and only that call releases the reference, so every earlier call sees live
// state. `chunk` is a heap-allocated C string this callback takes ownership of.
unsafe extern "C" fn text_trampoline(
    context: *mut c_void,
    chunk: *mut c_char,
    done: bool,
    status: i32,
) {
    unsafe { trampoline(context, chunk, done, status, parse_text) };
}

// SAFETY: Same invariants as `text_trampoline`.
unsafe extern "C" fn structured_trampoline(
    context: *mut c_void,
    chunk: *mut c_char,
    done: bool,
    status: i32,
) {
    unsafe { trampoline(context, chunk, done, status, parse_structured) };
}

unsafe fn trampoline(
    context: *mut c_void,
    chunk: *mut c_char,
    done: bool,
    status: i32,
    parse: fn(&str) -> Result<Snapshot, FMError>,
) {
    let payload = unsafe { take_bridge_string(chunk) };
    if context.is_null() {
        return;
    }
    let shared = context.cast_const().cast::<Shared>();
    catch_user_panic("stream callback", || {
        unsafe { &*shared }.deliver(payload, done, status, parse);
    });
    if done || status != ffi::status::OK {
        catch_user_panic("stream release", || unsafe { drop(Arc::from_raw(shared)) });
    }
}

// SAFETY: `context` is an `Arc<Shared>` raw pointer created by `start`. Swift
// invokes this callback exactly once, so the reference is released once.
unsafe extern "C" fn settled_trampoline(context: *mut c_void) {
    if context.is_null() {
        return;
    }
    let shared = context.cast_const().cast::<Shared>();
    catch_user_panic("stream settlement callback", || {
        unsafe { &*shared }.mark_settled();
    });
    catch_user_panic("stream settlement release", || unsafe {
        drop(Arc::from_raw(shared));
    });
}

fn start<F>(invoke: F, callback: ffi::FmStreamCallback) -> Generation
where
    F: FnOnce(*mut c_void, ffi::FmStreamCallback) -> *mut c_void,
{
    let shared = Arc::new(Shared::new());
    let context = Arc::into_raw(Arc::clone(&shared))
        .cast_mut()
        .cast::<c_void>();
    shared.attach(SwiftTask::from_raw(invoke(context, callback)));

    // Settlement is the end of the Swift task, not the stream's last callback:
    // a cancelled request still has to wait for the framework to roll its
    // transcript back. A request that failed before its task started reports a
    // null handle, which Swift settles immediately.
    let settlement_context = Arc::into_raw(Arc::clone(&shared))
        .cast_mut()
        .cast::<c_void>();
    unsafe {
        ffi::fm_task_await_settled(shared.task_ptr(), settlement_context, settled_trampoline);
    }

    Generation { shared }
}

pub(crate) fn start_text_stream<F>(invoke: F) -> Generation
where
    F: FnOnce(*mut c_void, ffi::FmStreamCallback) -> *mut c_void,
{
    start(invoke, text_trampoline)
}

pub(crate) fn start_structured_stream<F>(invoke: F) -> Generation
where
    F: FnOnce(*mut c_void, ffi::FmStreamCallback) -> *mut c_void,
{
    start(invoke, structured_trampoline)
}

/// A running generation.
///
/// Dropping a `Generation` cancels the native work without blocking the
/// dropping thread. Keep a [`GenerationHandle`] if you need to await the
/// native settlement afterwards.
pub struct Generation {
    shared: Arc<Shared>,
}

impl Generation {
    /// A cloneable handle for cancelling this generation and awaiting its
    /// native settlement. The handle outlives the `Generation`.
    #[must_use]
    pub fn handle(&self) -> GenerationHandle {
        GenerationHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Request cancellation. Idempotent; an already completed native response
    /// retains its successful outcome.
    pub fn cancel(&self) {
        self.shared.cancel();
    }

    /// True once cancellation has been requested, by [`cancel`](Self::cancel),
    /// a handle, or dropping a `Generation`.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.shared.cancelled.load(Ordering::Acquire)
    }

    /// How many native snapshots are waiting for this consumer. Never exceeds
    /// [`BOUNDED_UPDATE_CAPACITY`].
    #[must_use]
    pub fn buffered_updates(&self) -> usize {
        self.shared.queue().pending.len()
    }

    /// True once the native task has finished, including a cancelled request's
    /// transcript rollback.
    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.shared.settlement().settled
    }

    /// Wait for the native task to finish.
    ///
    /// Resolves immediately if it already has. This is the point at which the
    /// session may be reused or released.
    pub async fn settled(&self) {
        Settled {
            shared: &self.shared,
            slot: None,
        }
        .await;
    }

    /// The next update, or `None` once the generation ended.
    ///
    /// A failure is reported once, as `Some(Err(..))`; afterwards the stream
    /// stays `None`. Cancellation is reported as [`FMError::Cancelled`], not as
    /// an invented failure.
    pub async fn next(&mut self) -> Option<Result<GenerationUpdate, FMError>> {
        NextUpdate { generation: self }.await
    }

    fn poll_next(&self, cx: &Context<'_>) -> Poll<Option<Result<GenerationUpdate, FMError>>> {
        loop {
            // The common path reports what is already here and never touches
            // the caller's waker.
            if let Some(ready) = self.poll_ready() {
                return ready;
            }

            // Registering means cloning the waker, which runs the consumer's
            // code, so it happens with no lock held — as does dropping the
            // waker this one replaces.
            let waker = cx.waker().clone();
            let mut stale = None;
            let mut registered = false;
            {
                let mut queue = self.shared.queue();
                if !queue.finished && queue.pending.is_empty() && queue.terminal.is_none() {
                    stale = queue.waker.replace(waker);
                    drop(queue);
                    registered = true;
                }
            }
            drop(stale);

            if registered {
                return Poll::Pending;
            }
            // Something arrived while the waker was being cloned; report it.
        }
    }

    /// Report whatever the transport already holds, or `None` when the
    /// consumer has to wait.
    ///
    /// The delivered text lives in the transport, so the bridge callback and
    /// this consumer judge rewrites against exactly the same prefix.
    fn poll_ready(&self) -> Option<Poll<Option<Result<GenerationUpdate, FMError>>>> {
        let mut queue = self.shared.queue();
        if queue.finished {
            return Some(Poll::Ready(None));
        }
        while let Some(snapshot) = queue.pending.pop_front() {
            if let Some(update) = queue.consume(snapshot) {
                drop(queue);
                return Some(Poll::Ready(Some(Ok(update))));
            }
        }
        let outcome = match queue.terminal.take() {
            Some(Ok(())) => {
                queue.finished = true;
                Some(Poll::Ready(None))
            }
            Some(Err(error)) => {
                queue.finished = true;
                Some(Poll::Ready(Some(Err(error))))
            }
            None => None,
        };
        drop(queue);
        outcome
    }
}

impl Drop for Generation {
    fn drop(&mut self) {
        // A consumer that walks away must not leave native generation running.
        self.shared.cancel();
    }
}

impl core::fmt::Debug for Generation {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Generation")
            .field("buffered_updates", &self.buffered_updates())
            .field("cancelled", &self.is_cancelled())
            .field("settled", &self.is_settled())
            .finish()
    }
}

/// A cloneable handle to a [`Generation`]'s cancellation and settlement.
#[derive(Clone)]
pub struct GenerationHandle {
    shared: Arc<Shared>,
}

impl GenerationHandle {
    /// Request cancellation. Idempotent; an already completed native response
    /// retains its successful outcome.
    pub fn cancel(&self) {
        self.shared.cancel();
    }

    /// True once cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.shared.cancelled.load(Ordering::Acquire)
    }

    /// True once the native task has finished, including a cancelled request's
    /// transcript rollback.
    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.shared.settlement().settled
    }

    /// Wait for the native task to finish, even if the [`Generation`] is gone.
    ///
    /// Resolves immediately if it already has.
    pub async fn settled(&self) {
        Settled {
            shared: &self.shared,
            slot: None,
        }
        .await;
    }
}

impl core::fmt::Debug for GenerationHandle {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("GenerationHandle")
            .field("cancelled", &self.is_cancelled())
            .field("settled", &self.is_settled())
            .finish()
    }
}

/// Exclusive borrow of the stream: only one consumer may poll it, even though
/// polling needs no mutable state of its own.
struct NextUpdate<'a> {
    generation: &'a mut Generation,
}

impl Future for NextUpdate<'_> {
    type Output = Option<Result<GenerationUpdate, FMError>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.generation.poll_next(cx)
    }
}

/// One waiter for native settlement.
///
/// The waiter holds a slot in [`Settlement`] only while it is alive: dropping
/// it releases the registered waker immediately, so abandoning this future
/// never keeps waker-owned resources alive until the native task finishes.
struct Settled<'a> {
    shared: &'a Shared,
    slot: Option<usize>,
}

impl Future for Settled<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let waiter = self.get_mut();
        waiter.shared.poll_settled(&mut waiter.slot, cx)
    }
}

impl Drop for Settled<'_> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot {
            self.shared.release_settlement_slot(slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_text, Generation, GenerationHandle, GenerationUpdate, Shared, BOUNDED_UPDATE_CAPACITY,
    };
    use crate::error::FMError;
    use crate::ffi;
    use std::future::Future;
    use std::sync::mpsc::{channel, Receiver, Sender};
    use std::sync::{Arc, Mutex, Weak};
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Wake, Waker};
    use std::thread;
    use std::time::Duration;

    /// A transport with no Swift task behind it: every delivery below
    /// simulates one bridge callback, so these tests need no model and no FFI.
    fn transport() -> Generation {
        Generation {
            shared: Arc::new(Shared::new()),
        }
    }

    fn deliver(generation: &Generation, content: &str) {
        let payload = serde_json::json!({ "content": content }).to_string();
        generation
            .shared
            .deliver(Some(payload), false, ffi::status::OK, parse_text);
    }

    fn finish(generation: &Generation, status: i32) {
        generation.shared.deliver(None, true, status, parse_text);
    }

    fn next(generation: &mut Generation) -> Option<Result<GenerationUpdate, FMError>> {
        pollster::block_on(generation.next())
    }

    #[test]
    fn deltas_are_append_only_while_the_consumer_keeps_up() {
        let mut generation = transport();
        deliver(&generation, "Hel");
        assert_eq!(
            next(&mut generation),
            Some(Ok(GenerationUpdate::Chunk("Hel".to_owned())))
        );
        deliver(&generation, "Hello");
        assert_eq!(
            next(&mut generation),
            Some(Ok(GenerationUpdate::Chunk("lo".to_owned())))
        );
        finish(&generation, ffi::status::OK);
        assert_eq!(next(&mut generation), None);
        assert_eq!(next(&mut generation), None, "the terminal must be sticky");
    }

    #[test]
    fn an_unchanged_snapshot_emits_nothing() {
        let mut generation = transport();
        deliver(&generation, "Hello");
        assert_eq!(
            next(&mut generation),
            Some(Ok(GenerationUpdate::Chunk("Hello".to_owned())))
        );
        deliver(&generation, "Hello");
        finish(&generation, ffi::status::OK);
        assert_eq!(next(&mut generation), None);
    }

    #[test]
    fn a_direct_prefix_rewrite_is_reported_as_a_replacement() {
        let mut generation = transport();
        deliver(&generation, "alpha");
        assert_eq!(
            next(&mut generation),
            Some(Ok(GenerationUpdate::Chunk("alpha".to_owned())))
        );
        deliver(&generation, "beta");
        assert_eq!(
            next(&mut generation),
            Some(Ok(GenerationUpdate::Replace("beta".to_owned())))
        );
    }

    #[test]
    fn buffering_stays_within_the_bound() {
        let generation = transport();
        for index in 0..BOUNDED_UPDATE_CAPACITY * 4 {
            deliver(&generation, &"x".repeat(index + 1));
            assert!(
                generation.buffered_updates() <= BOUNDED_UPDATE_CAPACITY,
                "buffered {} updates",
                generation.buffered_updates()
            );
        }
    }

    #[test]
    fn coalescing_keeps_every_generated_character() {
        let mut generation = transport();
        // The consumer reads nothing while the model produces more snapshots
        // than the transport can hold.
        for index in 1..=20 {
            deliver(&generation, &"ab".repeat(index));
        }
        finish(&generation, ffi::status::OK);

        let mut text = String::new();
        while let Some(update) = next(&mut generation) {
            match update.expect("no failure was delivered") {
                GenerationUpdate::Chunk(delta) => text.push_str(&delta),
                GenerationUpdate::Replace(snapshot) => text = snapshot,
                other => panic!("unexpected {other:?}"),
            }
        }
        assert_eq!(text, "ab".repeat(20), "coalescing lost generated text");
    }

    /// A rewrite that is coalesced away must still be reported: the consumer
    /// has already been handed text that the model retracted, and Bento has to
    /// stop the operation instead of pretending the stream stayed append-only.
    #[test]
    fn a_coalesced_prefix_rewrite_still_surfaces() {
        let mut generation = transport();
        deliver(&generation, "a");
        assert_eq!(
            next(&mut generation),
            Some(Ok(GenerationUpdate::Chunk("a".to_owned()))),
            "the consumer has now been handed \"a\""
        );

        // Fill the transport with snapshots that extend what was delivered.
        for snapshot in ["aa", "aaa", "aaaa", "aaaaa"] {
            deliver(&generation, snapshot);
        }
        // The model rewrites the delivered prefix, then restores it. Both
        // snapshots are coalesced into the pending tail.
        deliver(&generation, "b");
        deliver(&generation, "aaaaaa");
        finish(&generation, ffi::status::OK);

        let first = next(&mut generation)
            .expect("an update must follow")
            .expect("no failure was delivered");
        assert!(
            matches!(first, GenerationUpdate::Replace(_)),
            "a rewrite of already-delivered text was swallowed by coalescing: got {first:?}"
        );
    }

    #[test]
    fn a_failure_is_reported_once_and_then_the_stream_ends() {
        let mut generation = transport();
        deliver(&generation, "partial");
        generation.shared.deliver(
            Some(String::new()),
            true,
            ffi::status::CANCELLED,
            parse_text,
        );

        assert_eq!(
            next(&mut generation),
            Some(Ok(GenerationUpdate::Chunk("partial".to_owned()))),
            "updates delivered before the failure must still arrive"
        );
        assert_eq!(next(&mut generation), Some(Err(FMError::Cancelled)));
        assert_eq!(next(&mut generation), None);
    }

    /// A waker whose liveness can be observed from outside.
    struct Observer;

    impl Wake for Observer {
        fn wake(self: Arc<Self>) {}
    }

    /// Register one settlement waiter and abandon it, returning a handle that
    /// reports whether anything still owns its waker.
    fn abandon_a_settlement_waiter(generation: &Generation) -> Weak<Observer> {
        let observer = Arc::new(Observer);
        let weak = Arc::downgrade(&observer);
        let waker = Waker::from(observer);
        let mut cx = Context::from_waker(&waker);
        {
            let mut settled = Box::pin(generation.settled());
            assert!(
                settled.as_mut().poll(&mut cx).is_pending(),
                "an unsettled transport must leave the waiter pending"
            );
        }
        drop(waker);
        weak
    }

    /// A caller that gives up on `settled()` — a shutdown timeout, say — must
    /// not leave its waker owned by the transport until the native task
    /// finishes. The rollback it was waiting for may still be pending.
    #[test]
    fn an_abandoned_settlement_waiter_releases_its_waker() {
        let generation = transport();
        let weak = abandon_a_settlement_waiter(&generation);

        assert!(
            !generation.is_settled(),
            "this transport has no task and must stay unsettled"
        );
        assert!(
            weak.upgrade().is_none(),
            "an abandoned settlement future kept its waker alive"
        );
    }

    /// Repeated give-up cycles must not accumulate: a host that polls with a
    /// timeout over and over still holds nothing once each attempt is gone.
    #[test]
    fn repeated_abandoned_waiters_do_not_accumulate() {
        let generation = transport();
        let abandoned: Vec<Weak<Observer>> = (0..64)
            .map(|_| abandon_a_settlement_waiter(&generation))
            .collect();

        assert!(!generation.is_settled());
        let retained = abandoned
            .iter()
            .filter(|observer| observer.upgrade().is_some())
            .count();
        assert_eq!(retained, 0, "{retained} abandoned wakers are still owned");
    }

    /// A waiter that is still alive must of course still be woken.
    #[test]
    fn a_live_settlement_waiter_is_woken_by_settlement() {
        let generation = transport();
        let observer = Arc::new(Observer);
        let weak = Arc::downgrade(&observer);
        let waker = Waker::from(observer);
        let mut cx = Context::from_waker(&waker);

        let mut settled = Box::pin(generation.settled());
        assert!(settled.as_mut().poll(&mut cx).is_pending());
        assert!(
            weak.upgrade().is_some(),
            "a live waiter's waker must stay registered"
        );

        generation.shared.mark_settled();
        assert!(settled.as_mut().poll(&mut cx).is_ready());
        assert!(generation.is_settled());
    }

    /// A waker that asks the transport a question while it is being dropped.
    ///
    /// A consumer's waker can own anything, and that something can call back
    /// into the handle it came from. The transport must never drop a waker
    /// while holding the lock such a call needs.
    struct ReentrantProbe {
        handle: GenerationHandle,
        dropped: Mutex<Sender<bool>>,
    }

    impl Wake for ReentrantProbe {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for ReentrantProbe {
        fn drop(&mut self) {
            let settled = self.handle.is_settled();
            if let Ok(reporter) = self.dropped.lock() {
                let _ = reporter.send(settled);
            }
        }
    }

    /// How long a probe gets to report before the transport is called stuck.
    const DROP_BUDGET: Duration = Duration::from_secs(5);

    /// Releasing an abandoned waiter must not drop its waker under the lock.
    #[test]
    fn dropping_an_abandoned_waiter_does_not_deadlock_the_transport() {
        let generation = transport();
        let handle = generation.handle();
        let (reporter, reports) = channel();
        let waker = Waker::from(Arc::new(ReentrantProbe {
            handle,
            dropped: Mutex::new(reporter),
        }));

        // The drop runs on a worker thread: a deadlocked transport parks that
        // thread forever, so it is joined only once the probe has reported.
        let worker = thread::spawn(move || {
            let mut settled = Box::pin(generation.settled());
            {
                let mut cx = Context::from_waker(&waker);
                assert!(settled.as_mut().poll(&mut cx).is_pending());
            }
            // The registered clone is now the probe's last owner, so releasing
            // the slot is what drops it.
            drop(waker);
            drop(settled);
        });

        assert!(
            reports.recv_timeout(DROP_BUDGET).is_ok(),
            "releasing a waiter deadlocked while dropping its waker"
        );
        worker.join().expect("the probe thread panicked");
    }

    /// Replacing a registered waker must not drop the old one under the lock.
    #[test]
    fn replacing_a_registered_waker_does_not_deadlock_the_transport() {
        let generation = transport();
        let handle = generation.handle();
        let (reporter, reports) = channel();
        let first = Waker::from(Arc::new(ReentrantProbe {
            handle,
            dropped: Mutex::new(reporter),
        }));
        let second = Waker::from(Arc::new(Observer));

        let worker = thread::spawn(move || {
            let mut settled = Box::pin(generation.settled());
            {
                let mut cx = Context::from_waker(&first);
                assert!(settled.as_mut().poll(&mut cx).is_pending());
            }
            drop(first);
            {
                // Polling with a different waker replaces the registered one,
                // which drops the probe.
                let mut cx = Context::from_waker(&second);
                assert!(settled.as_mut().poll(&mut cx).is_pending());
            }
            drop(settled);
        });

        assert!(
            reports.recv_timeout(DROP_BUDGET).is_ok(),
            "replacing a waiter's waker deadlocked while dropping the old one"
        );
        worker.join().expect("the probe thread panicked");
    }

    /// A raw waker whose clone and drop call back into the caller's code.
    ///
    /// Every `RawWakerVTable` entry is an external call-out: a real waker may
    /// clone, drop or wake into anything, including code that needs the
    /// transport's own locks. This probe reports that a call-out started and
    /// then waits for another thread to finish a native delivery, so a lock
    /// held across the call-out shows up as that delivery never completing.
    struct DeliveryProbe {
        started: Mutex<Sender<()>>,
        gate: Mutex<Gate>,
        on_clone: bool,
        on_drop: bool,
    }

    struct Gate {
        open: bool,
        opened: Receiver<()>,
    }

    impl DeliveryProbe {
        fn wait_for_delivery(&self) {
            if let Ok(callouts) = self.started.lock() {
                let _ = callouts.send(());
            }
            let Ok(mut gate) = self.gate.lock() else {
                return;
            };
            if gate.open {
                return;
            }
            if gate.opened.recv_timeout(DROP_BUDGET).is_ok() {
                gate.open = true;
            }
        }
    }

    unsafe fn delivery_clone(data: *const ()) -> RawWaker {
        let probe = unsafe { Arc::from_raw(data.cast::<DeliveryProbe>()) };
        if probe.on_clone {
            probe.wait_for_delivery();
        }
        let cloned = Arc::clone(&probe);
        let _ = Arc::into_raw(probe);
        RawWaker::new(Arc::into_raw(cloned).cast(), &DELIVERY_VTABLE)
    }

    unsafe fn delivery_wake(data: *const ()) {
        unsafe { delivery_drop(data) };
    }

    unsafe fn delivery_wake_by_ref(_data: *const ()) {}

    unsafe fn delivery_drop(data: *const ()) {
        let probe = unsafe { Arc::from_raw(data.cast::<DeliveryProbe>()) };
        // Only the last owner waits. Dropping the caller's own reference while
        // a copy is still registered must not block, or the fixture would
        // deadlock on itself instead of exercising the transport.
        if probe.on_drop && Arc::strong_count(&probe) == 1 {
            probe.wait_for_delivery();
        }
        drop(probe);
    }

    static DELIVERY_VTABLE: RawWakerVTable = RawWakerVTable::new(
        delivery_clone,
        delivery_wake,
        delivery_wake_by_ref,
        delivery_drop,
    );

    fn delivery_waker(probe: Arc<DeliveryProbe>) -> Waker {
        unsafe { Waker::from_raw(RawWaker::new(Arc::into_raw(probe).cast(), &DELIVERY_VTABLE)) }
    }

    /// How long the test gives a native delivery that is only waiting for a
    /// lock. The probe's own wait is five times longer, so a transport that
    /// holds the lock across the call-out fails here rather than hanging.
    const DELIVERY_BUDGET: Duration = Duration::from_secs(1);

    /// Registering an update waker clones it; that clone must not run under
    /// the transport lock, because it can wait for work needing the same lock.
    #[test]
    fn registering_an_update_waker_does_not_block_native_delivery() {
        let generation = transport();
        let shared = Arc::clone(&generation.shared);
        let (notify_callout, callouts) = channel();
        let (release_probe, completion) = channel();
        let waker = delivery_waker(Arc::new(DeliveryProbe {
            started: Mutex::new(notify_callout),
            gate: Mutex::new(Gate {
                open: false,
                opened: completion,
            }),
            on_clone: true,
            on_drop: false,
        }));
        let (reporter, delivered) = channel();

        let poller = thread::spawn(move || {
            let mut generation = generation;
            let mut update = Box::pin(generation.next());
            let mut cx = Context::from_waker(&waker);
            // The terminal lands during the clone, so the re-check under the
            // lock must report it rather than park this consumer.
            assert!(matches!(update.as_mut().poll(&mut cx), Poll::Ready(None)));
        });
        let native_task = thread::spawn(move || {
            // Deliver only once the call-out is under way.
            let _ = callouts.recv_timeout(DROP_BUDGET);
            shared.deliver(None, true, ffi::status::OK, parse_text);
            let _ = release_probe.send(());
            let _ = reporter.send(());
        });

        assert!(
            delivered.recv_timeout(DELIVERY_BUDGET).is_ok(),
            "a native terminal could not be delivered while an update waker was cloned"
        );
        poller.join().expect("the polling thread panicked");
        native_task.join().expect("the delivering thread panicked");
    }

    /// Replacing an update waker drops the old one; that drop must not run
    /// under the transport lock either.
    #[test]
    fn replacing_an_update_waker_does_not_block_native_delivery() {
        let generation = transport();
        let shared = Arc::clone(&generation.shared);
        let (notify_callout, callouts) = channel();
        let (release_probe, completion) = channel();
        let first = delivery_waker(Arc::new(DeliveryProbe {
            started: Mutex::new(notify_callout),
            gate: Mutex::new(Gate {
                open: false,
                opened: completion,
            }),
            on_clone: false,
            on_drop: true,
        }));
        let second = Waker::from(Arc::new(Observer));
        let (reporter, delivered) = channel();

        let poller = thread::spawn(move || {
            let mut generation = generation;
            {
                let mut update = Box::pin(generation.next());
                let mut cx = Context::from_waker(&first);
                assert!(update.as_mut().poll(&mut cx).is_pending());
            }
            // The registered clone is now the probe's last owner, so polling
            // with a different waker is what drops it.
            drop(first);
            let mut update = Box::pin(generation.next());
            let mut cx = Context::from_waker(&second);
            let _ = update.as_mut().poll(&mut cx);
        });
        let native_task = thread::spawn(move || {
            let _ = callouts.recv_timeout(DROP_BUDGET);
            shared.deliver(None, true, ffi::status::OK, parse_text);
            let _ = release_probe.send(());
            let _ = reporter.send(());
        });

        assert!(
            delivered.recv_timeout(DELIVERY_BUDGET).is_ok(),
            "a native terminal could not be delivered while an update waker was replaced"
        );
        poller.join().expect("the polling thread panicked");
        native_task.join().expect("the delivering thread panicked");
    }

    /// The same rule for the settlement side: cloning the waker must not run
    /// under the settlement lock that `mark_settled` needs.
    #[test]
    fn registering_a_settlement_waker_does_not_block_settlement() {
        let generation = transport();
        let shared = Arc::clone(&generation.shared);
        let (notify_callout, callouts) = channel();
        let (release_probe, completion) = channel();
        let waker = delivery_waker(Arc::new(DeliveryProbe {
            started: Mutex::new(notify_callout),
            gate: Mutex::new(Gate {
                open: false,
                opened: completion,
            }),
            on_clone: true,
            on_drop: false,
        }));
        let (reporter, settled_report) = channel();

        let poller = thread::spawn(move || {
            let mut settled = Box::pin(generation.settled());
            let mut cx = Context::from_waker(&waker);
            // Settlement lands during the clone, so the re-check must report
            // it rather than park this waiter.
            assert!(settled.as_mut().poll(&mut cx).is_ready());
        });
        let settler = thread::spawn(move || {
            let _ = callouts.recv_timeout(DROP_BUDGET);
            shared.mark_settled();
            let _ = release_probe.send(());
            let _ = reporter.send(());
        });

        assert!(
            settled_report.recv_timeout(DELIVERY_BUDGET).is_ok(),
            "settlement could not be marked while a settlement waker was cloned"
        );
        poller.join().expect("the polling thread panicked");
        settler.join().expect("the settling thread panicked");
    }
}
