import XCTest

@testable import FoundationModelsBridge

private final class Counter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0

    func increment() {
        lock.lock()
        count += 1
        lock.unlock()
    }

    var value: Int {
        lock.lock()
        defer { lock.unlock() }
        return count
    }
}

/// `@convention(c)` callbacks cannot capture, so the null-task check counts its
/// invocations through this file-private slot.
private nonisolated(unsafe) var settlementCalls = 0

/// Lifecycle races on `FMTaskHandle`. These need no model, no SDK version and
/// no FoundationModels import: the handle is the bridge's own bookkeeping.
final class TaskHandleTests: XCTestCase {
    /// `startBridgeTask` starts the task before it attaches the handle, so a
    /// short body can finish first. A handle whose body already completed must
    /// not take ownership of a task attached afterwards — otherwise a later
    /// `cancel()` would cancel work it does not own, and the handle would hold
    /// that task for the rest of the process.
    func testASettledHandleDoesNotAdoptOrCancelALaterTask() async {
        let handle = FMTaskHandle()
        handle.complete()

        // A live task owned by someone else, long enough that only an explicit
        // cancellation can end it.
        let task = Task.detached { _ = try? await Task.sleep(nanoseconds: 60_000_000_000) }
        handle.attach(task)

        handle.cancel()
        XCTAssertFalse(
            task.isCancelled,
            "a settled handle adopted and cancelled a task it does not own"
        )
        XCTAssertTrue(handle.isSettled)

        task.cancel()
        await task.value
    }

    /// Settlement is reported exactly once per waiter, whether the waiter
    /// registered before or after the body finished, and attaching afterwards
    /// neither repeats nor drops it.
    func testEverySettlementWaiterRunsExactlyOnce() {
        let handle = FMTaskHandle()
        let counter = Counter()

        handle.whenSettled { counter.increment() }
        XCTAssertEqual(counter.value, 0, "an unfinished task must not report settlement")

        handle.complete()
        XCTAssertEqual(counter.value, 1)

        handle.whenSettled { counter.increment() }
        XCTAssertEqual(counter.value, 2, "a settled handle answers immediately")

        handle.attach(Task.detached {})
        XCTAssertEqual(counter.value, 2, "attaching must not re-report settlement")
        XCTAssertTrue(handle.isSettled)
    }

    /// Cancellation requested before the task exists must still reach it: Rust
    /// can cancel as soon as it owns the handle pointer.
    func testCancelBeforeAttachStillCancelsTheTask() async {
        let handle = FMTaskHandle()
        handle.cancel()

        let cancelled = Counter()
        let task = Task.detached {
            try? await Task.sleep(nanoseconds: 2_000_000_000)
            if Task.isCancelled {
                cancelled.increment()
            }
        }
        handle.attach(task)
        await task.value

        XCTAssertEqual(
            cancelled.value,
            1,
            "a cancellation requested before attach must reach the task"
        )
    }

    /// A request that failed before its task started reports a null handle, and
    /// `fm_task_await_settled` must answer such a handle immediately — that is
    /// what makes `Generation::settled()` resolve for a failed start.
    func testNullTaskCountsAsSettled() {
        XCTAssertTrue(fm_task_is_settled(nil))

        settlementCalls = 0
        fm_task_await_settled(nil, nil) { _ in settlementCalls += 1 }
        XCTAssertEqual(settlementCalls, 1)
    }
}
