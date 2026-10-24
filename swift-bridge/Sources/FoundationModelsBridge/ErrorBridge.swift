import Foundation

#if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
import FoundationModels
#endif

/// The C callback a streaming request delivers snapshots and its terminal to.
typealias FMStreamCallback = @convention(c) (
    UnsafeMutableRawPointer?,
    UnsafeMutablePointer<CChar>?,
    Bool,
    Int32
) -> Void

#if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
@available(macOS 26.0, *)
func writeErrorOut(
    _ errorOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?,
    _ payload: BridgeErrorPayload
) {
    errorOut?.pointee = ffiString(encodeErrorPayload(payload))
}

@available(macOS 26.0, *)
func generationErrorPayload(
    _ error: LanguageModelSession.GenerationError,
    lease: BridgeLease?
) -> BridgeErrorPayload {
    let message = error.localizedDescription
    let recoverySuggestion = error.recoverySuggestion
    let failureReason = error.failureReason

    switch error {
    case .guardrailViolation(let context),
         .exceededContextWindowSize(let context),
         .unsupportedLanguageOrLocale(let context),
         .assetsUnavailable(let context),
         .rateLimited(let context),
         .decodingFailure(let context),
         .concurrentRequests(let context),
         .unsupportedGuide(let context):
        return BridgeErrorPayload(
            message: message,
            recoverySuggestion: recoverySuggestion,
            failureReason: failureReason,
            generationErrorContext: BridgeErrorContext(debugDescription: context.debugDescription),
            refusal: nil,
            toolCallError: nil,
            adapterAssetErrorContext: nil,
            schemaErrorContext: nil
        )
    case .refusal(let refusal, let context):
        return BridgeErrorPayload(
            message: message,
            recoverySuggestion: recoverySuggestion,
            failureReason: failureReason,
            generationErrorContext: BridgeErrorContext(debugDescription: context.debugDescription),
            refusal: lease?.lend(refusal),
            toolCallError: nil,
            adapterAssetErrorContext: nil,
            schemaErrorContext: nil
        )
    @unknown default:
        return BridgeErrorPayload(
            message: message,
            recoverySuggestion: recoverySuggestion,
            failureReason: failureReason,
            generationErrorContext: nil,
            refusal: nil,
            toolCallError: nil,
            adapterAssetErrorContext: nil,
            schemaErrorContext: nil
        )
    }
}

@available(macOS 26.0, *)
func schemaErrorPayload(_ error: GenerationSchema.SchemaError) -> BridgeErrorPayload {
    let context: GenerationSchema.SchemaError.Context
    switch error {
    case .duplicateType(_, _, let errorContext),
         .duplicateProperty(_, _, let errorContext),
         .emptyTypeChoices(_, let errorContext),
         .undefinedReferences(_, _, let errorContext):
        context = errorContext
    @unknown default:
        return BridgeErrorPayload(
            message: error.localizedDescription,
            recoverySuggestion: error.recoverySuggestion,
            failureReason: nil,
            generationErrorContext: nil,
            refusal: nil,
            toolCallError: nil,
            adapterAssetErrorContext: nil,
            schemaErrorContext: nil
        )
    }

    return BridgeErrorPayload(
        message: error.localizedDescription,
        recoverySuggestion: error.recoverySuggestion,
        failureReason: nil,
        generationErrorContext: nil,
        refusal: nil,
        toolCallError: nil,
        adapterAssetErrorContext: nil,
        schemaErrorContext: BridgeErrorContext(debugDescription: context.debugDescription)
    )
}

@available(macOS 26.0, *)
func toolCallErrorPayload(_ error: LanguageModelSession.ToolCallError) -> BridgeErrorPayload {
    BridgeErrorPayload(
        message: error.localizedDescription,
        recoverySuggestion: nil,
        failureReason: nil,
        generationErrorContext: nil,
        refusal: nil,
        toolCallError: BridgeToolCallErrorPayload(
            tool: bridgeToolDefinition(from: error.tool),
            underlyingError: error.underlyingError.localizedDescription
        ),
        adapterAssetErrorContext: nil,
        schemaErrorContext: nil
    )
}

@available(macOS 26.0, *)
func assetErrorPayload(_ error: SystemLanguageModel.Adapter.AssetError) -> BridgeErrorPayload {
    let message = error.localizedDescription
    let recoverySuggestion = error.recoverySuggestion
    
    var context: BridgeErrorContext? = nil
    switch error {
    case .invalidAsset(let errorContext),
         .invalidAdapterName(let errorContext),
         .compatibleAdapterNotFound(let errorContext):
        context = BridgeErrorContext(debugDescription: errorContext.debugDescription)
    @unknown default:
        break
    }
    
    return BridgeErrorPayload(
        message: message,
        recoverySuggestion: recoverySuggestion,
        failureReason: nil,
        generationErrorContext: nil,
        refusal: nil,
        toolCallError: nil,
        adapterAssetErrorContext: context,
        schemaErrorContext: nil
    )
}

@available(macOS 26.0, *)
func encodedTextResponse(
    _ response: LanguageModelSession.Response<String>,
    lease: BridgeLease
) throws -> String {
    let transcriptJSON = try encodeTranscriptJSON(entries: response.transcriptEntries)
    let payload = BridgeTextResponse(
        content: response.content,
        rawContent: bridgeGeneratedContent(response.rawContent, lease: lease),
        transcriptJSON: transcriptJSON
    )
    return try encodeBridge(payload)
}

@available(macOS 26.0, *)
func finishStream(
    cancelled: Bool,
    context: UnsafeMutableRawPointer?,
    callback: @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafeMutablePointer<CChar>?,
        Bool,
        Int32
    ) -> Void
) {
    if cancelled {
        let (code, message) = mapError(CancellationError())
        callback(context, ffiString(message), true, code)
    } else {
        callback(context, nil, true, FM_OK)
    }
}

/// Wait for `FoundationModels` to release a cancelled request.
///
/// A request issued before the framework has rolled the transcript back traps
/// inside the framework, so this wait is required. Cancelling a stream makes
/// it return normally rather than throwing, and the rollback happens
/// afterwards: a probe on macOS 27.0 saw `isResponding` already false while
/// the transcript still held the request, with the entries disappearing about
/// ten milliseconds later. Both signals must therefore settle.
///
/// There is deliberately no deadline after which this reports a wind-down it
/// has not observed; a host that needs a bound applies its own shutdown
/// timeout and reports a timeout instead of claiming native completion.
@available(macOS 26.0, *)
func settleCancelledStream(_ session: LanguageModelSession, entriesBefore: Int) async {
    await Task.detached {
        while session.isResponding || session.transcript.count > entriesBefore {
            try? await Task.sleep(nanoseconds: 10_000_000)
        }
    }.value
}

@available(macOS 26.0, *)
@discardableResult
func streamTextResponse(
    _ stream: LanguageModelSession.ResponseStream<String>,
    context: UnsafeMutableRawPointer?,
    callback: @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafeMutablePointer<CChar>?,
        Bool,
        Int32
    ) -> Void
) async -> Bool {
    do {
        for try await snapshot in stream {
            let payload = BridgeTextStreamSnapshot(content: snapshot.content)
            callback(context, ffiString(try encodeBridge(payload)), false, FM_OK)
        }
        let cancelled = Task.isCancelled
        finishStream(cancelled: cancelled, context: context, callback: callback)
        return cancelled
    } catch {
        let cancelled = error is CancellationError || Task.isCancelled
        let lease = BridgeLease()
        defer { lease.end() }
        let (code, message) = mapError(error, lease: lease)
        callback(context, ffiString(message), true, code)
        return cancelled
    }
}

/// Run a text stream and perform whatever cleanup its outcome requires.
///
/// The cdecl and the bridge's own tests go through this one function, so the
/// decision about cleanup is exercised by tests rather than duplicated.
///
/// `Task.isCancelled` is not evidence that the framework aborted anything: a
/// cancellation delivered after the generation finished leaves a legitimately
/// committed response behind, and waiting for that transcript to shrink waits
/// forever. The stream's own cached terminal result decides the outcome.
@available(macOS 26.0, *)
func runTextStream(
    session: LanguageModelSession,
    stream: LanguageModelSession.ResponseStream<String>,
    entriesBefore: Int,
    context: UnsafeMutableRawPointer?,
    callback: FMStreamCallback
) async {
    var delivered: String?
    var encodingFailure: Error?
    do {
        for try await snapshot in stream {
            do {
                let payload = try encodeBridge(BridgeTextStreamSnapshot(content: snapshot.content))
                callback(context, ffiString(payload), false, FM_OK)
                delivered = snapshot.content
            } catch {
                // A bridge encoding failure is ours, not the model's; it must
                // not be reported as a native outcome or swallowed by one.
                encodingFailure = error
                break
            }
        }
    } catch {
        // Iteration errors are diagnosed below from the cached terminal.
    }

    do {
        let response = try await stream.collect()
        if let encodingFailure {
            report(encodingFailure, context: context, callback: callback)
            return
        }
        if delivered != response.content {
            // Iteration stopped before the last snapshot, so hand over the
            // complete answer: a consumer told "success" must not hold a prefix.
            do {
                let payload = try encodeBridge(BridgeTextStreamSnapshot(content: response.content))
                callback(context, ffiString(payload), false, FM_OK)
            } catch {
                report(error, context: context, callback: callback)
                return
            }
        }
        callback(context, nil, true, FM_OK)
    } catch {
        await failStream(
            reporting: encodingFailure ?? error,
            nativeOutcome: error,
            session: session,
            entriesBefore: entriesBefore,
            context: context,
            callback: callback
        )
    }
}

/// Run a structured stream and perform whatever cleanup its outcome requires.
///
/// Same outcome rule as [`runTextStream`]: the cached terminal result decides,
/// not `Task.isCancelled`.
@available(macOS 26.0, *)
func runStructuredStream(
    session: LanguageModelSession,
    stream: LanguageModelSession.ResponseStream<GeneratedContent>,
    entriesBefore: Int,
    context: UnsafeMutableRawPointer?,
    callback: FMStreamCallback
) async {
    var delivered: String?
    var deliveredComplete = false
    var encodingFailure: Error?
    do {
        for try await snapshot in stream {
            do {
                let payload = try encodeBridge(structuredSnapshot(
                    content: snapshot.content,
                    rawContent: snapshot.rawContent,
                    isComplete: snapshot.content.isComplete
                ))
                callback(context, ffiString(payload), false, FM_OK)
                delivered = snapshot.content.jsonString
                deliveredComplete = snapshot.content.isComplete
            } catch {
                encodingFailure = error
                break
            }
        }
    } catch {
        // Iteration errors are diagnosed below from the cached terminal.
    }

    do {
        let response = try await stream.collect()
        if let encodingFailure {
            report(encodingFailure, context: context, callback: callback)
            return
        }
        // Repeating the last partial's bytes is still worth a snapshot when
        // that partial was not complete: a scalar root keeps `isComplete`
        // false until the terminal result settles it, so bytes alone would
        // leave a consumer told "success" with nothing it may decode.
        if delivered != response.content.jsonString || !deliveredComplete {
            do {
                let payload = try encodeBridge(structuredSnapshot(
                    content: response.content,
                    rawContent: response.rawContent,
                    isComplete: true
                ))
                callback(context, ffiString(payload), false, FM_OK)
            } catch {
                report(error, context: context, callback: callback)
                return
            }
        }
        callback(context, nil, true, FM_OK)
    } catch {
        await failStream(
            reporting: encodingFailure ?? error,
            nativeOutcome: error,
            session: session,
            entriesBefore: entriesBefore,
            context: context,
            callback: callback
        )
    }
}

@available(macOS 26.0, *)
private func structuredSnapshot(
    content: GeneratedContent,
    rawContent: GeneratedContent,
    isComplete: Bool
) -> BridgeStructuredStreamSnapshot {
    BridgeStructuredStreamSnapshot(
        content: BridgeGeneratedContent(json: content.jsonString, generationID: nil),
        rawContent: BridgeGeneratedContent(json: rawContent.jsonString, generationID: nil),
        isComplete: isComplete
    )
}

/// Report a failure that did not come from the model, such as a bridge
/// encoding error. No native cleanup is owed for one.
@available(macOS 26.0, *)
private func report(
    _ error: Error,
    context: UnsafeMutableRawPointer?,
    callback: FMStreamCallback
) {
    let (code, message) = mapError(error)
    callback(context, ffiString(message), true, code)
}

/// Report a stream's terminal failure and wait out the cleanup it owes.
///
/// `reporting` is what the consumer is told, which is the bridge's own failure
/// when it had one — an encoding error is never swallowed by a native outcome.
/// `nativeOutcome` decides the cleanup: only a genuine `CancellationError`
/// leaves a rollback pending, and the bridge waits for it rather than guessing
/// a deadline.
@available(macOS 26.0, *)
private func failStream(
    reporting reported: Error,
    nativeOutcome: Error,
    session: LanguageModelSession,
    entriesBefore: Int,
    context: UnsafeMutableRawPointer?,
    callback: FMStreamCallback
) async {
    let lease = BridgeLease()
    let (code, message) = mapError(reported, lease: lease)
    callback(context, ffiString(message), true, code)
    lease.end()
    if nativeOutcome is CancellationError {
        await settleCancelledStream(session, entriesBefore: entriesBefore)
    }
}
#endif

@_cdecl("fm_generation_id_create")
public func fm_generation_id_create(
    _ tokenOut: UnsafeMutablePointer<UInt64>?,
    _ descriptionOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?,
    _ errorOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
) -> Int32 {
    #if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
    if #available(macOS 26.0, *) {
        guard let tokenOut else {
            writeErrorOut(errorOut, "generation ID token out-pointer is null")
            return FM_INVALID_ARGUMENT
        }
        let generationID = GenerationID()
        tokenOut.pointee = BridgeHandles.generationIDs.insert(generationID)
        descriptionOut?.pointee = ffiString(String(describing: generationID))
        return FM_OK
    }
    #endif
    writeErrorOut(errorOut, "FoundationModels requires macOS 26.0 or newer")
    return FM_MODEL_UNAVAILABLE
}

@_cdecl("fm_decimal_to_generated_content_json")
public func fm_decimal_to_generated_content_json(
    _ decimalString: UnsafePointer<CChar>,
    _ outputOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?,
    _ errorOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
) -> Int32 {
    #if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
    if #available(macOS 26.0, *) {
        guard let decimal = Decimal(string: String(cString: decimalString)) else {
            writeErrorOut(errorOut, BridgeErrorPayload(
                message: "decimal value is invalid",
                recoverySuggestion: nil,
                failureReason: nil,
                generationErrorContext: nil,
                refusal: nil,
                toolCallError: nil,
                adapterAssetErrorContext: nil,
                schemaErrorContext: nil
            ))
            return FM_INVALID_ARGUMENT
        }
        outputOut?.pointee = ffiString(decimal.generatedContent.jsonString)
        return FM_OK
    }
    #endif
    writeErrorOut(errorOut, "FoundationModels requires macOS 26.0 or newer")
    return FM_MODEL_UNAVAILABLE
}

@_cdecl("fm_decimal_from_generated_content_json")
public func fm_decimal_from_generated_content_json(
    _ generatedContentJSON: UnsafePointer<CChar>,
    _ outputOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?,
    _ errorOut: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
) -> Int32 {
    #if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
    if #available(macOS 26.0, *) {
        do {
            let content = try GeneratedContent(json: String(cString: generatedContentJSON))
            let decimal = try Decimal(content)
            outputOut?.pointee = ffiString(NSDecimalNumber(decimal: decimal).stringValue)
            return FM_OK
        } catch {
            let (code, message) = mapError(error)
            errorOut?.pointee = ffiString(message)
            return code
        }
    }
    #endif
    writeErrorOut(errorOut, "FoundationModels requires macOS 26.0 or newer")
    return FM_MODEL_UNAVAILABLE
}

@_cdecl("fm_refusal_explanation_json")
public func fm_refusal_explanation_json(
    _ refusalToken: UInt64,
    _ context: UnsafeMutableRawPointer?,
    _ callback: @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafeMutablePointer<CChar>?,
        UnsafeMutablePointer<CChar>?,
        Int32
    ) -> Void
) -> UnsafeMutableRawPointer? {
    #if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
    if #available(macOS 26.0, *) {
        guard let refusal = BridgeHandles.refusals.value(for: refusalToken) else {
            callback(context, nil, ffiString("unknown refusal token"), FM_INVALID_ARGUMENT)
            return nil
        }
        return startBridgeTask {
            let lease = BridgeLease()
            defer { lease.end() }
            do {
                try Task.checkCancellation()
                let response = try await refusal.explanation
                callback(context, ffiString(try encodedTextResponse(response, lease: lease)), nil, FM_OK)
            } catch {
                let (code, message) = mapError(error, lease: lease)
                callback(context, nil, ffiString(message), code)
            }
        }
    }
    #endif
    callback(context, nil, ffiString("FoundationModels requires macOS 26.0 or newer"), FM_MODEL_UNAVAILABLE)
    return nil
}

@_cdecl("fm_refusal_explanation_from_transcript_json")
public func fm_refusal_explanation_from_transcript_json(
    _ transcriptJSON: UnsafePointer<CChar>,
    _ context: UnsafeMutableRawPointer?,
    _ callback: @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafeMutablePointer<CChar>?,
        UnsafeMutablePointer<CChar>?,
        Int32
    ) -> Void
) -> UnsafeMutableRawPointer? {
    #if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
    if #available(macOS 26.0, *) {
        let transcriptJSONString = String(cString: transcriptJSON)
        return startBridgeTask {
            let lease = BridgeLease()
            defer { lease.end() }
            do {
                try Task.checkCancellation()
                let transcript = try decodeTranscript(from: transcriptJSONString)
                let refusal = LanguageModelSession.GenerationError.Refusal(
                    transcriptEntries: Array(transcript)
                )
                let response = try await refusal.explanation
                callback(context, ffiString(try encodedTextResponse(response, lease: lease)), nil, FM_OK)
            } catch {
                let (code, message) = mapError(error, lease: lease)
                callback(context, nil, ffiString(message), code)
            }
        }
    }
    #endif
    callback(context, nil, ffiString("FoundationModels requires macOS 26.0 or newer"), FM_MODEL_UNAVAILABLE)
    return nil
}

@_cdecl("fm_refusal_explanation_stream")
public func fm_refusal_explanation_stream(
    _ refusalToken: UInt64,
    _ context: UnsafeMutableRawPointer?,
    _ callback: @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafeMutablePointer<CChar>?,
        Bool,
        Int32
    ) -> Void
) -> UnsafeMutableRawPointer? {
    #if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
    if #available(macOS 26.0, *) {
        guard let refusal = BridgeHandles.refusals.value(for: refusalToken) else {
            callback(context, ffiString("unknown refusal token"), true, FM_INVALID_ARGUMENT)
            return nil
        }
        return startBridgeTask {
            await streamTextResponse(refusal.explanationStream, context: context, callback: callback)
        }
    }
    #endif
    callback(context, ffiString("FoundationModels requires macOS 26.0 or newer"), true, FM_MODEL_UNAVAILABLE)
    return nil
}

@_cdecl("fm_refusal_explanation_stream_from_transcript_json")
public func fm_refusal_explanation_stream_from_transcript_json(
    _ transcriptJSON: UnsafePointer<CChar>,
    _ context: UnsafeMutableRawPointer?,
    _ callback: @convention(c) (
        UnsafeMutableRawPointer?,
        UnsafeMutablePointer<CChar>?,
        Bool,
        Int32
    ) -> Void
) -> UnsafeMutableRawPointer? {
    #if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
    if #available(macOS 26.0, *) {
        let transcriptJSONString = String(cString: transcriptJSON)
        return startBridgeTask {
            do {
                try Task.checkCancellation()
                let transcript = try decodeTranscript(from: transcriptJSONString)
                let refusal = LanguageModelSession.GenerationError.Refusal(
                    transcriptEntries: Array(transcript)
                )
                await streamTextResponse(refusal.explanationStream, context: context, callback: callback)
            } catch {
                let (code, message) = mapError(error)
                callback(context, ffiString(message), true, code)
            }
        }
    }
    #endif
    callback(context, ffiString("FoundationModels requires macOS 26.0 or newer"), true, FM_MODEL_UNAVAILABLE)
    return nil
}
