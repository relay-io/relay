# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2023-03-02

### Changed

- Updated Runner trait to use rust 1.75 impl trait instead of async_trait.
- Upgraded dependencies including to Axum 0.7.x, latest rustls and tokio.

### Fixed

- Documentation around raw JSON payload giving the impression of it needing to be a string.
- Updated retry HTTP client logic to only apply max retries to non retryable errors.

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

- Initial rework of relay-rs(v1) https://github.com/rust-playground/relay-rs with real world improvements after using in
  production for over a year.

[Unreleased]: https://github.com/relay-io/relay/compare/v0.2.0...HEAD

[0.2.0]: https://github.com/relay-io/relay/compare/v0.1.3...v0.2.0

[0.1.3]: https://github.com/relay-io/relay/compare/v0.1.2...v0.1.3

[0.1.2]: https://github.com/relay-io/relay/compare/v0.1.1...v0.1.2

[0.1.1]: https://github.com/relay-io/relay/compare/v0.1.0...v0.1.1

[0.1.0]: https://github.com/relay-io/relay/compare/70cfed7...v0.1.0
