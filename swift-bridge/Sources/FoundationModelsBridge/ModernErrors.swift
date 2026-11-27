import Foundation

#if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
import FoundationModels
#endif

#if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK && FOUNDATION_MODELS_HAS_MACOS27_SDK

/// Translate macOS 27's typed error families into bridge status codes.
///
/// macOS 27 deprecated `LanguageModelSession.GenerationError` and now throws
/// `LanguageModelError`, `SystemLanguageModel.Error`,
/// `LanguageModelSession.Error` and `GeneratedContent.ParsingError` instead.
/// Without this translation every one of them reaches Rust as
/// `FM_UNKNOWN`, which erases the quota, refusal, unsupported-capability and
/// availability categories a caller must distinguish.
///
/// Returns `nil` when `error` is not one of those families, so the caller can
/// fall through to the macOS 26 mapping.
@available(macOS 27.0, *)
func mapModernError(_ error: Error) -> (Int32, String)? {
    if let languageModelError = error as? LanguageModelError {
        return mapLanguageModelError(languageModelError)
    }
    if let modelError = error as? SystemLanguageModel.Error {
        switch modelError {
        case .assetsUnavailable:
            return (FM_ASSETS_UNAVAILABLE, modernPayload(modelError, debug: modelError.debugDescription))
        @unknown default:
            return nil
        }
    }
    if let sessionError = error as? LanguageModelSession.Error {
        switch sessionError {
        case .concurrentRequests:
            return (FM_CONCURRENT_REQUESTS, modernPayload(sessionError, debug: sessionError.debugDescription))
        case .transcriptMutationWhileResponding:
            return (
                FM_TRANSCRIPT_MUTATION_WHILE_RESPONDING,
                modernPayload(sessionError, debug: sessionError.debugDescription)
            )
        @unknown default:
            return nil
        }
    }
    if let parsingError = error as? GeneratedContent.ParsingError {
        return (FM_DECODING_FAILURE, modernPayload(parsingError, debug: parsingError.debugDescription))
    }
    return nil
}

@available(macOS 27.0, *)
private func mapLanguageModelError(_ error: LanguageModelError) -> (Int32, String) {
    switch error {
    case .contextSizeExceeded(let detail):
        return (
            FM_CONTEXT_WINDOW_EXCEEDED,
            modernPayload(
                error,
                debug: detail.debugDescription,
                details: BridgeLanguageModelErrorDetails(
                    contextSize: detail.contextSize,
                    tokenCount: detail.tokenCount
                )
            )
        )
    case .rateLimited(let detail):
        return (FM_RATE_LIMITED, modernPayload(error, debug: detail.debugDescription))
    case .guardrailViolation(let detail):
        return (FM_GUARDRAIL_VIOLATION, modernPayload(error, debug: detail.debugDescription))
    case .refusal(let detail):
        return (FM_REFUSAL, modernPayload(error, debug: detail.debugDescription))
    case .unsupportedCapability(let detail):
        return (
            FM_UNSUPPORTED_CAPABILITY,
            modernPayload(
                error,
                debug: detail.debugDescription,
                details: BridgeLanguageModelErrorDetails(capability: capabilityName(detail.capability))
            )
        )
    case .unsupportedTranscriptContent(let detail):
        return (FM_UNSUPPORTED_TRANSCRIPT_CONTENT, modernPayload(error, debug: detail.debugDescription))
    case .unsupportedGenerationGuide(let detail):
        return (FM_UNSUPPORTED_GUIDE, modernPayload(error, debug: detail.debugDescription))
    case .unsupportedLanguageOrLocale(let detail):
        return (FM_UNSUPPORTED_LANGUAGE, modernPayload(error, debug: detail.debugDescription))
    case .timeout(let detail):
        return (FM_TIMEOUT, modernPayload(error, debug: detail.debugDescription))
    @unknown default:
        return (FM_UNKNOWN, modernPayload(error, debug: error.debugDescription))
    }
}

/// Apple's `Capability` is an opaque `Hashable` value with no public name, so
/// the known members are matched explicitly rather than parsed out of prose.
@available(macOS 27.0, *)
private func capabilityName(_ capability: LanguageModelCapabilities.Capability) -> String {
    switch capability {
    case .vision:
        return "vision"
    case .guidedGeneration:
        return "guidedGeneration"
    case .reasoning:
        return "reasoning"
    case .toolCalling:
        return "toolCalling"
    default:
        return "unknown"
    }
}

/// Build the error payload, keeping Apple's localized text, its recovery and
/// failure strings, and its debug description, which Rust surfaces as the
/// generation-error context.
@available(macOS 27.0, *)
private func modernPayload(
    _ error: Error,
    debug: String,
    details: BridgeLanguageModelErrorDetails? = nil
) -> String {
    let localized = error.localizedDescription
    let localizedError = error as? LocalizedError
    return encodeErrorPayload(BridgeErrorPayload(
        message: localized.isEmpty ? debug : localized,
        recoverySuggestion: localizedError?.recoverySuggestion,
        failureReason: localizedError?.failureReason,
        generationErrorContext: BridgeErrorContext(debugDescription: debug),
        refusal: nil,
        toolCallError: nil,
        adapterAssetErrorContext: nil,
        schemaErrorContext: nil,
        languageModelError: details
    ))
}

#endif
