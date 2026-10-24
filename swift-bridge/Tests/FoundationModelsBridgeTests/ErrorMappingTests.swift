import XCTest

#if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK
import FoundationModels
#endif

@testable import FoundationModelsBridge

/// Exercises the bridge's real `mapError` with authentic macOS 27 error values
/// built from Apple's public initializers.
///
/// Apple throws most of these only under conditions a test cannot provoke —
/// rate limiting, request timeouts, an unsupported capability — so running the
/// genuine error values through the genuine mapping function is the only way
/// to verify the translation. Nothing here fakes a model or re-implements the
/// mapping.
///
/// Run with:
/// ```text
/// swift test --package-path swift-bridge \
///   -Xswiftc -DFOUNDATION_MODELS_HAS_MACOS26_SDK \
///   -Xswiftc -DFOUNDATION_MODELS_HAS_MACOS27_SDK
/// ```
final class ErrorMappingTests: XCTestCase {
    #if canImport(FoundationModels) && FOUNDATION_MODELS_HAS_MACOS26_SDK && FOUNDATION_MODELS_HAS_MACOS27_SDK

    /// Status codes the bridge must report for the macOS 27-only categories.
    /// Pinned as numbers so a missing production constant surfaces as the
    /// mapping failure it is, not as a compile error.
    private static let expectedUnsupportedCapability: Int32 = -17
    private static let expectedUnsupportedTranscriptContent: Int32 = -18
    private static let expectedTimeout: Int32 = -19
    private static let expectedTranscriptMutationWhileResponding: Int32 = -20

    private struct MappedPayload: Decodable {
        struct LanguageModelErrorDetails: Decodable {
            var contextSize: Int?
            var tokenCount: Int?
            var capability: String?
        }

        var message: String
        var languageModelError: LanguageModelErrorDetails?
    }

    private func map(_ error: Error) throws -> (code: Int32, payload: MappedPayload) {
        let (code, message) = mapError(error)
        let data = try XCTUnwrap(message.data(using: .utf8), "payload must be UTF-8")
        let payload = try JSONDecoder().decode(MappedPayload.self, from: data)
        XCTAssertFalse(payload.message.isEmpty, "every mapped error needs a diagnostic message")
        return (code, payload)
    }

    private func requireMacOS27() throws {
        guard #available(macOS 27.0, *) else {
            throw XCTSkip(
                "FOUNDATION_MODELS_HAS_MACOS27_SDK was defined but this runtime is older than macOS 27"
            )
        }
    }

    @available(macOS 27.0, *)
    func testContextSizeExceededMapsToContextWindowExceeded() throws {
        try requireMacOS27()
        let error = LanguageModelError.contextSizeExceeded(
            .init(contextSize: 8192, tokenCount: 9001, debugDescription: "probe: context size exceeded")
        )
        let mapped = try map(error)
        XCTAssertEqual(mapped.code, FM_CONTEXT_WINDOW_EXCEEDED)
        XCTAssertEqual(mapped.payload.languageModelError?.contextSize, 8192)
        XCTAssertEqual(mapped.payload.languageModelError?.tokenCount, 9001)
    }

    @available(macOS 27.0, *)
    func testRateLimitedMapsToRateLimited() throws {
        try requireMacOS27()
        let error = LanguageModelError.rateLimited(
            .init(resetDate: nil, debugDescription: "probe: rate limited")
        )
        XCTAssertEqual(try map(error).code, FM_RATE_LIMITED)
    }

    @available(macOS 27.0, *)
    func testGuardrailViolationAndRefusalStayDistinct() throws {
        try requireMacOS27()
        let guardrail = LanguageModelError.guardrailViolation(
            .init(debugDescription: "probe: guardrail violation")
        )
        XCTAssertEqual(try map(guardrail).code, FM_GUARDRAIL_VIOLATION)

        let refusal = LanguageModelError.refusal(
            .init(explanation: "probe refusal", debugDescription: "probe: refusal")
        )
        XCTAssertEqual(try map(refusal).code, FM_REFUSAL)
    }

    @available(macOS 27.0, *)
    func testUnsupportedCapabilityMapsToItsOwnCategoryAndNamesTheCapability() throws {
        try requireMacOS27()
        let error = LanguageModelError.unsupportedCapability(
            .init(capability: .vision, debugDescription: "probe: unsupported capability")
        )
        let mapped = try map(error)
        XCTAssertEqual(mapped.code, Self.expectedUnsupportedCapability)
        XCTAssertEqual(mapped.payload.languageModelError?.capability, "vision")
    }

    @available(macOS 27.0, *)
    func testUnsupportedTranscriptContentMapsToItsOwnCategory() throws {
        try requireMacOS27()
        let error = LanguageModelError.unsupportedTranscriptContent(
            .init(unsupportedContent: [], debugDescription: "probe: unsupported transcript content")
        )
        XCTAssertEqual(try map(error).code, Self.expectedUnsupportedTranscriptContent)
    }

    @available(macOS 27.0, *)
    func testUnsupportedGenerationGuideMapsToUnsupportedGuide() throws {
        try requireMacOS27()
        let error = LanguageModelError.unsupportedGenerationGuide(
            .init(schemaName: "Town", debugDescription: "probe: unsupported generation guide")
        )
        XCTAssertEqual(try map(error).code, FM_UNSUPPORTED_GUIDE)
    }

    @available(macOS 27.0, *)
    func testUnsupportedLanguageOrLocaleMapsToUnsupportedLanguage() throws {
        try requireMacOS27()
        let error = LanguageModelError.unsupportedLanguageOrLocale(
            .init(languageCode: Locale.LanguageCode("fr"), debugDescription: "probe: unsupported locale")
        )
        XCTAssertEqual(try map(error).code, FM_UNSUPPORTED_LANGUAGE)
    }

    @available(macOS 27.0, *)
    func testTimeoutMapsToItsOwnCategory() throws {
        try requireMacOS27()
        let error = LanguageModelError.timeout(.init(debugDescription: "probe: timeout"))
        XCTAssertEqual(try map(error).code, Self.expectedTimeout)
    }

    @available(macOS 27.0, *)
    func testAssetsUnavailableMapsToAssetsUnavailable() throws {
        try requireMacOS27()
        let error = SystemLanguageModel.Error.assetsUnavailable(
            .init(debugDescription: "probe: assets unavailable")
        )
        XCTAssertEqual(try map(error).code, FM_ASSETS_UNAVAILABLE)
    }

    @available(macOS 27.0, *)
    func testSessionStateErrorsMapToTheirOwnCategories() throws {
        try requireMacOS27()
        XCTAssertEqual(try map(LanguageModelSession.Error.concurrentRequests).code, FM_CONCURRENT_REQUESTS)
        XCTAssertEqual(
            try map(LanguageModelSession.Error.transcriptMutationWhileResponding).code,
            Self.expectedTranscriptMutationWhileResponding
        )
    }

    @available(macOS 27.0, *)
    func testParsingErrorMapsToDecodingFailure() throws {
        try requireMacOS27()
        let error = GeneratedContent.ParsingError(
            rawContent: "{\"name\":",
            debugDescription: "probe: parsing error"
        )
        XCTAssertEqual(try map(error).code, FM_DECODING_FAILURE)
    }

    @available(macOS 27.0, *)
    func testNoModernErrorDegradesToUnknown() throws {
        try requireMacOS27()
        let errors: [Error] = [
            LanguageModelError.contextSizeExceeded(
                .init(contextSize: 8192, tokenCount: 9001, debugDescription: "probe")
            ),
            LanguageModelError.rateLimited(.init(resetDate: nil, debugDescription: "probe")),
            LanguageModelError.guardrailViolation(.init(debugDescription: "probe")),
            LanguageModelError.refusal(.init(explanation: "probe", debugDescription: "probe")),
            LanguageModelError.unsupportedCapability(
                .init(capability: .toolCalling, debugDescription: "probe")
            ),
            LanguageModelError.unsupportedTranscriptContent(
                .init(unsupportedContent: [], debugDescription: "probe")
            ),
            LanguageModelError.unsupportedGenerationGuide(
                .init(schemaName: nil, debugDescription: "probe")
            ),
            LanguageModelError.unsupportedLanguageOrLocale(
                .init(languageCode: Locale.LanguageCode("fr"), debugDescription: "probe")
            ),
            LanguageModelError.timeout(.init(debugDescription: "probe")),
            SystemLanguageModel.Error.assetsUnavailable(.init(debugDescription: "probe")),
            LanguageModelSession.Error.concurrentRequests,
            LanguageModelSession.Error.transcriptMutationWhileResponding,
            GeneratedContent.ParsingError(rawContent: "{", debugDescription: "probe"),
        ]
        for error in errors {
            let (code, _) = mapError(error)
            XCTAssertNotEqual(
                code,
                FM_UNKNOWN,
                "\(type(of: error)) \(error) still degrades to FM_UNKNOWN"
            )
        }
    }

    @available(macOS 27.0, *)
    func testCancellationStillMapsToCancelled() throws {
        try requireMacOS27()
        XCTAssertEqual(mapError(CancellationError()).0, FM_CANCELLED)
    }

    #endif
}
