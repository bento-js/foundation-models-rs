import Foundation

#if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
import FoundationModels
#endif

final class FMTaskHandle: @unchecked Sendable {
    private let lock = NSLock()
    private var task: Task<Void, Never>?
    private var cancelRequested = false
    private var completed = false
    private var onSettled: [@Sendable () -> Void] = []

    /// Adopt the task running this request's body. Module-internal rather than
    /// file-private so the test target can drive the attach/complete race.
    func attach(_ task: Task<Void, Never>) {
        lock.lock()
        if completed {
            // The body already finished; storing the task now would only
            // reinstate the handle/task retain cycle.
            lock.unlock()
            return
        }
        self.task = task
        let cancel = cancelRequested
        lock.unlock()
        if cancel {
            task.cancel()
        }
    }

    func cancel() {
        lock.lock()
        cancelRequested = true
        let task = self.task
        lock.unlock()
        task?.cancel()
    }

    fileprivate var isCancelRequested: Bool {
        lock.lock()
        defer { lock.unlock() }
        return cancelRequested
    }

    /// Mark the task's body as finished and wake everything waiting on it.
    ///
    /// Dropping the `Task` reference here also breaks the retain cycle between
    /// this handle and the task that captures it.
    func complete() {
        lock.lock()
        completed = true
        task = nil
        let waiters = onSettled
        onSettled = []
        lock.unlock()
        for waiter in waiters {
            waiter()
        }
    }

    var isSettled: Bool {
        lock.lock()
        defer { lock.unlock() }
        return completed
    }

    /// Invoke `waiter` once the task's body has finished, immediately if it
    /// already has.
    func whenSettled(_ waiter: @escaping @Sendable () -> Void) {
        lock.lock()
        if completed {
            lock.unlock()
            waiter()
            return
        }
        onSettled.append(waiter)
        lock.unlock()
    }

    fileprivate func wait() async {
        // Scoped locking: the lock must not be held across the await below.
        let (task, done) = lock.withLock { (self.task, self.completed) }
        if done {
            return
        }
        await task?.value
    }
}

final class FMRequestGate: @unchecked Sendable {
    private let lock = NSLock()
    private var running: [ObjectIdentifier: FMTaskHandle] = [:]

    fileprivate func begin(_ handle: FMTaskHandle) -> [FMTaskHandle] {
        lock.lock()
        defer { lock.unlock() }
        let cancelled = running.values.filter { $0.isCancelRequested }
        running[ObjectIdentifier(handle)] = handle
        return cancelled
    }

    fileprivate func end(_ handle: FMTaskHandle) {
        lock.lock()
        running[ObjectIdentifier(handle)] = nil
        lock.unlock()
    }
}

func startBridgeTask(
    gate: FMRequestGate? = nil,
    _ body: @escaping @Sendable () async -> Void
) -> UnsafeMutableRawPointer {
    let handle = FMTaskHandle()
    let cancelledEarlier = gate?.begin(handle) ?? []
    let task = Task.detached {
        for earlier in cancelledEarlier {
            await earlier.wait()
        }
        await body()
        gate?.end(handle)
        handle.complete()
    }
    handle.attach(task)
    return Unmanaged.passRetained(handle).toOpaque()
}

@_cdecl("fm_task_cancel")
public func fm_task_cancel(_ taskPtr: UnsafeMutableRawPointer?) {
    guard let taskPtr else { return }
    Unmanaged<FMTaskHandle>.fromOpaque(taskPtr).takeUnretainedValue().cancel()
}

/// Register a settlement callback on a bridge task.
///
/// The callback runs once the task's body has finished — after a cancelled
/// request's transcript rollback and after the session's request gate has
/// released the handle — so a caller can tell when the native session is safe
/// to reuse or release. It is invoked exactly once, on an arbitrary thread,
/// and immediately if the task already finished. A null task counts as
/// settled, because the bridge only returns null when it already delivered a
/// terminal callback.
///
/// For a cancelled request the task body first waits for `FoundationModels` to
/// release it — `isResponding` false and the transcript rolled back — so this
/// acknowledgement is not merely "the bridge stopped looking". A caller that
/// needs a deadline must impose its own and report a timeout rather than
/// assume completion.
@_cdecl("fm_task_await_settled")
public func fm_task_await_settled(
    _ taskPtr: UnsafeMutableRawPointer?,
    _ context: UnsafeMutableRawPointer?,
    _ callback: @convention(c) (UnsafeMutableRawPointer?) -> Void
) {
    guard let taskPtr else {
        callback(context)
        return
    }
    let unmanaged = Unmanaged<FMTaskHandle>.fromOpaque(taskPtr)
    // Keep the handle alive until the waiter has run: Rust may release its own
    // reference as soon as this returns. Addresses, not pointers, are captured
    // so the waiter stays `Sendable`.
    _ = unmanaged.retain()
    let handleAddress = UInt(bitPattern: taskPtr)
    let contextAddress = UInt(bitPattern: context)
    unmanaged.takeUnretainedValue().whenSettled {
        callback(UnsafeMutableRawPointer(bitPattern: contextAddress))
        if let handle = UnsafeMutableRawPointer(bitPattern: handleAddress) {
            Unmanaged<FMTaskHandle>.fromOpaque(handle).release()
        }
    }
}

/// True if a bridge task's body has finished. A null task counts as settled.
@_cdecl("fm_task_is_settled")
public func fm_task_is_settled(_ taskPtr: UnsafeMutableRawPointer?) -> Bool {
    guard let taskPtr else { return true }
    return Unmanaged<FMTaskHandle>.fromOpaque(taskPtr).takeUnretainedValue().isSettled
}

@_cdecl("fm_object_retain")
public func fm_object_retain(_ ptr: UnsafeMutableRawPointer?) {
    guard let ptr else { return }
    _ = Unmanaged<AnyObject>.fromOpaque(ptr).retain()
}

#if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
@available(macOS 26.0, *)
final class SessionBox: @unchecked Sendable {
    let session: LanguageModelSession
    let gate = FMRequestGate()

    init(_ session: LanguageModelSession) {
        self.session = session
    }
}

@available(macOS 26.0, *)
func sessionBox(from ptr: UnsafeMutableRawPointer) -> SessionBox {
    Unmanaged<SessionBox>.fromOpaque(ptr).takeUnretainedValue()
}
#endif
