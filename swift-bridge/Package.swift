// swift-tools-version:5.9
import PackageDescription

// Swift compiler defines (FOUNDATION_MODELS_HAS_MACOS26_SDK,
// FOUNDATION_MODELS_HAS_MACOS27_SDK) are passed via -Xswiftc flags from
// build.rs based on Cargo feature flags (macos_26_0, macos_27_0).
//
// The test target is never built by `swift build`, so `cargo build` does not
// compile or link it; run it with `swift test` and the same defines.

let package = Package(
    name: "FoundationModelsBridge",
    platforms: [
        .macOS("26.0")
    ],
    products: [
        .library(
            name: "FoundationModelsBridge",
            type: .static,
            targets: ["FoundationModelsBridge"])
    ],
    targets: [
        .target(
            name: "FoundationModelsBridge",
            path: "Sources/FoundationModelsBridge"),
        .testTarget(
            name: "FoundationModelsBridgeTests",
            dependencies: ["FoundationModelsBridge"],
            path: "Tests/FoundationModelsBridgeTests")
    ]
)
