# Changelog

## 0.2.0 - 2026-03-01

- Added exponential backoff retries for failed ingest uploads (1s, 4s, 16s).
- Added `sdk_telemetry` metadata in CE batch payloads.
- Added drop-after-retries warning log behavior.
