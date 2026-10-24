import XCTest

#if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
import FoundationModels
#endif

@testable import FoundationModelsBridge

#if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK

/// Collects what the bridge delivered through its C callback.
///
/// Reached through the callback's `context` pointer, so each test owns its own
/// recorder and nothing is shared between tests.
private final class TerminalRecorder: @unchecked Sendable {
    private struct TextSnapshot: Decodable {
        var content: String
    }

    private struct StructuredSnapshot: Decodable {
        struct Content: Decodable {
            var json: String
        }

        var content: Content
        var isComplete: Bool
    }

    private let lock = NSLock()
    private var payloads: [String] = []
    private var terminals: [Int32] = []

    func record(chunk: UnsafeMutablePointer<CChar>?, done: Bool, status: Int32) {
        var payload: String?
        if let chunk {
            payload = String(cString: chunk)
            fm_string_free(chunk)
        }
        lock.lock()
        defer { lock.unlock() }
        if done {
            terminals.append(status)
            return
        }
        if let payload {
            payloads.append(payload)
        }
    }

    private func decodeLast<T: Decodable>(_ type: T.Type) -> T? {
        lock.lock()
        let payload = payloads.last
        lock.unlock()
        guard let data = payload?.data(using: .utf8) else {
            return nil
        }
        return try? JSONDecoder().decode(type, from: data)
    }

    /// The content of the last text snapshot handed to the consumer.
    var text: String? {
        decodeLast(TextSnapshot.self)?.content
    }

    /// The JSON of the last structured snapshot handed to the consumer, and
    /// whether that snapshot was marked complete.
    var structured: (json: String, isComplete: Bool)? {
        decodeLast(StructuredSnapshot.self).map { ($0.content.json, $0.isComplete) }
    }

    /// Every structured snapshot handed to the consumer, in delivery order.
    var structuredSnapshots: [(json: String, isComplete: Bool)] {
        lock.lock()
        let recorded = payloads
        lock.unlock()
        return recorded.compactMap { payload in
            guard let data = payload.data(using: .utf8),
                  let snapshot = try? JSONDecoder().decode(StructuredSnapshot.self, from: data)
            else {
                return nil
            }
            return (snapshot.content.json, snapshot.isComplete)
        }
    }

    var terminalStatuses: [Int32] {
        lock.lock()
        defer { lock.unlock() }
        return terminals
    }
}

private let recordIntoContext: FMStreamCallback = { context, chunk, done, status in
    guard let context else {
        if let chunk {
            fm_string_free(chunk)
        }
        return
    }
    Unmanaged<TerminalRecorder>.fromOpaque(context)
        .takeUnretainedValue()
        .record(chunk: chunk, done: done, status: status)
}

/// `ResponseStream` is not `Sendable`; exactly one task touches the value this
/// box carries, and it is handed over before that task starts.
private struct StreamBox<Content>: @unchecked Sendable where Content: Generable {
    let stream: LanguageModelSession.ResponseStream<Content>
}

/// Lets the test cancel a task before its body does anything observable.
private actor Gate {
    private var continuation: CheckedContinuation<Void, Never>?
    private var opened = false

    func wait() async {
        if opened {
            return
        }
        await withCheckedContinuation { continuation = $0 }
    }

    func open() {
        opened = true
        continuation?.resume()
        continuation = nil
    }
}

/// Whether the bridge decides a stream was cancelled, and therefore owes a
/// transcript rollback, must come from the stream's own terminal result — not
/// from `Task.isCancelled` sampled after the loop. A cancellation delivered
/// after the native generation already completed leaves nothing to roll back:
/// the response is legitimately committed, so waiting for the transcript to
/// shrink waits forever.
///
/// This needs the on-device model. Ordinary CI, which has none, reports it as
/// skipped — visibly, not as a pass. A run that claims to be live evidence must
/// set `FM_REQUIRE_MODEL=1`, and then an unavailable model fails instead of
/// skipping:
///
/// ```text
/// FM_REQUIRE_MODEL=1 swift test --package-path swift-bridge \
///   -Xswiftc -DFOUNDATION_MODELS_HAS_MACOS26_SDK \
///   -Xswiftc -DFOUNDATION_MODELS_HAS_MACOS27_SDK
/// ```
final class StreamOutcomeTests: XCTestCase {
    /// Reported when a run that claims live evidence has no model: a thrown
    /// error fails the test, where `XCTSkip` would only skip it.
    private struct LiveModelUnavailable: LocalizedError {
        let availability: String

        var errorDescription: String? {
            "FM_REQUIRE_MODEL is set, so this run claims live evidence, but the "
                + "on-device model is unavailable: \(availability)"
        }
    }

    @available(macOS 26.0, *)
    private func requireModel() throws {
        if SystemLanguageModel.default.isAvailable {
            return
        }
        if ProcessInfo.processInfo.environment["FM_REQUIRE_MODEL"] != nil {
            throw LiveModelUnavailable(
                availability: "\(SystemLanguageModel.default.availability)"
            )
        }
        throw XCTSkip("the on-device model is unavailable; set FM_REQUIRE_MODEL=1 to require it")
    }

    @available(macOS 26.0, *)
    func testACancelDeliveredAfterNativeCompletionDoesNotWaitForARollback() async throws {
        try requireModel()

        let session = LanguageModelSession(instructions: "Answer in one short sentence.")
        let entriesBefore = session.transcript.count

        // Drive the generation to completion first, exactly as a consumer that
        // read every chunk would, and take Apple's cached terminal result.
        let stream = session.streamResponse(to: "Name one Norse god.")
        for try await _ in stream {}
        let response = try await stream.collect()
        let committed = session.transcript

        // Now hand that finished stream to the bridge from a task that is
        // already cancelled: the cancellation arrived after native completion.
        let recorder = TerminalRecorder()
        let context = UInt(bitPattern: Unmanaged.passRetained(recorder).toOpaque())
        let box = StreamBox(stream: stream)
        let gate = Gate()
        let returned = expectation(description: "the bridge helper returned")
        let probe = Task.detached {
            await gate.wait()
            await runTextStream(
                session: session,
                stream: box.stream,
                entriesBefore: entriesBefore,
                context: UnsafeMutableRawPointer(bitPattern: context),
                callback: recordIntoContext
            )
            if let pointer = UnsafeMutableRawPointer(bitPattern: context) {
                Unmanaged<TerminalRecorder>.fromOpaque(pointer).release()
            }
            returned.fulfill()
        }
        probe.cancel()
        await gate.open()

        // Never await the probe: when the bug reproduces it never returns, and
        // the recorder stays alive because that task still owns its reference.
        await fulfillment(of: [returned], timeout: 10)

        XCTAssertEqual(
            recorder.terminalStatuses,
            [FM_OK],
            "a generation that completed natively must report success, once"
        )
        XCTAssertEqual(
            recorder.text,
            response.content,
            "the delivered text must be the response Apple cached"
        )
        XCTAssertEqual(
            session.transcript,
            committed,
            "a completed response must not be rolled back"
        )
    }

    /// The same edge on the constrained path: a cancellation that lands after
    /// the structured generation completed owes no rollback either, and the
    /// consumer must still receive the complete, validated snapshot.
    @available(macOS 26.0, *)
    func testACancelAfterStructuredCompletionDoesNotWaitForARollback() async throws {
        try requireModel()

        let schema = try GenerationSchema(
            root: DynamicGenerationSchema(
                name: "Town",
                properties: [
                    DynamicGenerationSchema.Property(
                        name: "name",
                        schema: DynamicGenerationSchema(type: String.self)
                    )
                ]
            ),
            dependencies: []
        )

        let session = LanguageModelSession(instructions: "Answer with one town.")
        let entriesBefore = session.transcript.count

        let stream = session.streamResponse(to: "Name one seaside town.", schema: schema)
        for try await _ in stream {}
        let response = try await stream.collect()
        let committed = session.transcript

        let recorder = TerminalRecorder()
        let context = UInt(bitPattern: Unmanaged.passRetained(recorder).toOpaque())
        let box = StreamBox(stream: stream)
        let gate = Gate()
        let returned = expectation(description: "the bridge helper returned")
        let probe = Task.detached {
            await gate.wait()
            await runStructuredStream(
                session: session,
                stream: box.stream,
                entriesBefore: entriesBefore,
                context: UnsafeMutableRawPointer(bitPattern: context),
                callback: recordIntoContext
            )
            if let pointer = UnsafeMutableRawPointer(bitPattern: context) {
                Unmanaged<TerminalRecorder>.fromOpaque(pointer).release()
            }
            returned.fulfill()
        }
        probe.cancel()
        await gate.open()

        await fulfillment(of: [returned], timeout: 10)

        XCTAssertEqual(
            recorder.terminalStatuses,
            [FM_OK],
            "a structured generation that completed natively must report success, once"
        )
        let delivered = recorder.structured
        XCTAssertEqual(
            delivered?.json,
            response.content.jsonString,
            "the delivered snapshot must be the content Apple cached"
        )
        XCTAssertEqual(
            delivered?.isComplete,
            true,
            "the delivered snapshot must be marked complete"
        )
        XCTAssertEqual(
            session.transcript,
            committed,
            "a completed structured response must not be rolled back"
        )
    }

    /// A constrained generation whose root is a bare number must reach the
    /// consumer marked complete.
    ///
    /// Scalar roots settle their completeness late: the last partial can
    /// already carry the final bytes while `isComplete` is still false, and
    /// only the cached terminal result says the value is finished. A final
    /// snapshot withheld because its bytes repeat that partial therefore
    /// leaves the consumer holding nothing complete, and a valid answer is
    /// rejected as unfinished.
    ///
    /// The bridge drives the live stream here instead of taking one the test
    /// already drained: the edge belongs to the iteration that produces the
    /// partials, so a pre-drained stream would not exercise it.
    @available(macOS 26.0, *)
    func testAScalarNumberRootIsDeliveredAsACompleteSnapshot() async throws {
        try requireModel()

        let schema = try GenerationSchema(
            root: DynamicGenerationSchema(type: Int.self),
            dependencies: []
        )

        let session = LanguageModelSession(instructions: "Answer with one whole number.")
        let entriesBefore = session.transcript.count

        let recorder = TerminalRecorder()
        let context = UInt(bitPattern: Unmanaged.passRetained(recorder).toOpaque())
        defer {
            if let pointer = UnsafeMutableRawPointer(bitPattern: context) {
                Unmanaged<TerminalRecorder>.fromOpaque(pointer).release()
            }
        }

        let stream = session.streamResponse(
            to: "How many legs does a spider have?",
            schema: schema
        )
        await runStructuredStream(
            session: session,
            stream: stream,
            entriesBefore: entriesBefore,
            context: UnsafeMutableRawPointer(bitPattern: context),
            callback: recordIntoContext
        )

        XCTAssertEqual(
            recorder.terminalStatuses,
            [FM_OK],
            "a scalar generation that completed natively must report success, once"
        )

        let response = try await stream.collect()
        let snapshots = recorder.structuredSnapshots
        XCTAssertEqual(
            snapshots.last?.json,
            response.content.jsonString,
            "the last delivered snapshot must be the content Apple cached"
        )
        XCTAssertEqual(
            snapshots.last?.isComplete,
            true,
            "a scalar root must reach the consumer marked complete"
        )
        XCTAssertEqual(
            snapshots.filter { $0.isComplete && $0.json == response.content.jsonString }.count,
            1,
            "the complete snapshot must be delivered once, not repeated"
        )
        let decoded = try? JSONSerialization.jsonObject(
            with: Data(response.content.jsonString.utf8),
            options: [.fragmentsAllowed]
        )
        XCTAssertTrue(
            decoded is NSNumber,
            "an integer root must arrive as a bare JSON number, got \(response.content.jsonString)"
        )
    }
}

#endif
