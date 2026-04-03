# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-03-01

### Added

- Exponential backoff retries for failed ingest uploads (1s, 4s, 16s).
- `sdk_telemetry` metadata in CE batch payloads.
- Drop-after-retries warning log behavior.
- Local pre-arm triggers: `slow_success`, `in_flight_pileup`, `retry_onset`.
- Downstream edge key resolution with tiered quality levels.
- Ring buffer with pre-alert capture ordering.
- Serverless support with flush-before-exit.
- Transport wrapping for outbound HTTP trace propagation.

## [0.1.0] - 2026-02-01

### Added

- Initial release.
- `Client` with API key auth and configurable ingest URL.
- `net/http` middleware for automatic request instrumentation.
- Chi, Gin, Echo router adapters.
- gRPC unary and stream interceptors.
- Queue integration (publish/consume instrumentation).
- DB integration for query instrumentation.
- HTTP outbound integration for automatic context propagation.
- Default integration set with auto-registration.
- Causal event types: HTTP_IN, HTTP_OUT, QUEUE_PUBLISH, QUEUE_CONSUME, INTERNAL.
- Event vocabulary helpers for queue, job, and webhook operations.
- Trace context propagation via `x-incidentary-trace-id` and `x-incidentary-parent-ce` headers.
