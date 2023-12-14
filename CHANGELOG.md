# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.3] - 2023-12-14
### Fixed
- `to_processing` metric when job requeued.

## [0.1.2] - 2023-12-13
### Fixed
- version not incremented in Cargo.toml.

## [0.1.1] - 2023-12-13
### Fixed
- `to_processing` metric using wrong `updated_at`.

## [0.1.0] - 2023-12-04
### Added
- Initial rework of relay-rs(v1) https://github.com/rust-playground/relay-rs with real world improvements after using in production for over a year.

[Unreleased]: https://github.com/relay-io/relay/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/relay-io/relay/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/relay-io/relay/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/relay-io/relay/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/relay-io/relay/compare/70cfed7...v0.1.0
