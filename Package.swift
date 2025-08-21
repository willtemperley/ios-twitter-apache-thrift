// swift-tools-version:6.2
// The swift-tools-version declares the minimum version of Swift required to build this package.
//
// Copyright 2020 Twitter, Inc.
// Licensed under the Apache License, Version 2.0
// http://www.apache.org/licenses/LICENSE-2.0

import PackageDescription

let package = Package(
    name: "TwitterApacheThrift",
    platforms: [
        .macOS("15.0")
    ],
    products: [
        .library(
            name: "TwitterApacheThrift",
            type: .static,
            targets: [
                "TwitterApacheThrift"
            ]
        )
    ],
    dependencies: [
      .package(
          url: "https://github.com/willtemperley/swift-binary-parsing.git",
          branch: "main"
      ),
    ],
    targets: [
        .target(
            name: "TwitterApacheThrift",
            dependencies: [
              .product(name: "BinaryParsing", package: "swift-binary-parsing"),
            ]
        ),
        .testTarget(
            name: "TwitterApacheThriftTests",
            dependencies: [
                "TwitterApacheThrift"
            ],
            exclude:[
                "Fixture.thrift"
            ]
        )
    ]
)
