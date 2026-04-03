# @incidentary/sdk-go

Go SDK for Incidentary.

## What pre-arm does

The SDK runs local anomaly triggers and can move capture mode from `NORMAL` to `PRE_ARMED` before external alerts:

- `slow_success`: successes remain successful but become much slower.
- `in_flight_pileup`: in-flight work rises faster than completion.
- `retry_onset`: outbound retry behavior starts climbing.
- existing `5xx` trigger remains active.

Pre-arm is silent:

- no local paging
- no local incident creation
- only richer local capture and metadata while risk is elevated

If no incident binds, pre-arm expires and returns to `NORMAL`.

## Quick Start

```go
cfg := incidentary.DefaultConfig(os.Getenv("INCIDENTARY_API_KEY"), "my-service")
client := incidentary.New(cfg)

http.Handle("/", incidentary.Middleware(client, yourHandler))
```

## Event Vocabulary Helpers (Queue + Job + Webhook)

```go
status202 := 202
client.RecordQueuePublish(incidentary.RecordEventOptions{
 EventAttrs: map[string]interface{}{"topic": "payments.jobs"},
})
client.RecordQueueConsume(incidentary.RecordEventOptions{})
client.RecordJobStart(incidentary.RecordEventOptions{ParentCeID: parentCeID})
client.RecordJobEnd(incidentary.RecordEventOptions{})
client.RecordWebhookIn(incidentary.RecordEventOptions{})
client.RecordWebhookOut(incidentary.RecordEventOptions{Status: &status202})
```

Generic emitter:

```go
client.RecordEvent(incidentary.EventJobStart, incidentary.RecordEventOptions{
 EventAttrs: map[string]interface{}{"worker": "invoice-sync"},
})
```

## Outbound instrumentation (retry-aware)

```go
attempt := 2
req, _ := http.NewRequest(http.MethodPost, "https://billing.internal/charges/123/capture", bytes.NewReader([]byte("{}")))

resp, err := incidentary.InstrumentedDo(
    client,
    http.DefaultClient,
    &incidentary.TraceContext{TraceID: traceID, CeID: parentCeID},
    req,
    &incidentary.OutboundRetryMetadata{
        RetryAttempt:      &attempt,
        RouteTemplate:     "/charges/:id/capture",
        DownstreamService: "billing",
    },
)
```

Retry identity quality priority:

1. explicit retry metadata
2. route template
3. logical edge
4. normalized URL fallback
5. unknown

`GetPreArmDebugState()` exposes per-quality usage and normalized URL fallback rate.

## Enhanced detail capture

Go SDK keeps one CE envelope and attaches optional `detail` only in elevated modes:

- `NORMAL`: base CE only
- `PRE_ARMED` / `INCIDENT`: base CE + optional detail metadata

Detail currently includes route metadata, selected headers, request/response byte estimates, retry/downstream metadata, and timeout/cancel classification. Payload snippets are disabled by default and gated by redaction + truncation config.

## Key defaults

### State machine

- `PreArmThresholdHigh=10.0`
- `PreArmThresholdLow=2.0`
- `PreArmMinDurationMs=60000`
- `PreArmTTLMs=300000`
- `PreArmCooldownMs=30000`

### Slow-success

- `PreArmEnableSlowSuccess=true`
- `PreArmSlowMinMs=250`
- `PreArmSlowMultiplier=2.0`
- `PreArmSlowAlpha=0.1`
- `PreArmSlowSuccessRateHigh=0.20`
- `PreArmSlowSuccessRateMild=0.10`
- `PreArmSlowMinSamples=50`
- `PreArmSlowInclude4xxAsSuccessLike=true`

### In-flight pileup

- `PreArmEnableInFlight=true`
- `PreArmInflightMinAbs=32`
- `PreArmInflightMultiplier=2.0`
- `PreArmInflightNetGrowthMin=16`
- `PreArmInflightHoldSecs=3`
- `PreArmInflightMildHoldSecs=2`

### Retry onset

- `PreArmEnableRetry=true`
- `PreArmRetryWindowMs=5000`
- `PreArmRetryRateHigh=0.10`
- `PreArmRetryRateMild=0.05`
- `PreArmRetryMinTotal=20`
- `PreArmRetryTableSize=4096`

## Debug state

`client.GetPreArmDebugState()` exposes:

- trigger counters (`prearm_trigger_*`)
- lifecycle counters (`prearm_enter_total`, `prearm_bind_total`, `prearm_expire_total`)
- in-flight / slow-success / retry gauges
- retry key quality distribution and fallback rate
- last trigger record and active/recent pre-arm windows

## Benchmark

Run trigger benchmark suite:

```bash
go test ./incidentary -bench Benchmark -benchmem
```

## Flush behavior

- Uploads are asynchronous and non-blocking.
- Retry backoff on failure: `1s`, `4s`, `16s`.
- After final retry failure, batch is dropped with warning log output.
- Upload `capture_mode` is `SKELETON` in `NORMAL` and `FULL` in `PRE_ARMED` / `INCIDENT`.

## Troubleshooting

1. 401 from ingest: verify workspace API key.
2. No traces in UI: ensure middleware is attached before handlers.
3. 426 version rejection: upgrade to sdk-go `0.2.0` or newer.
4. Retry logs increasing: verify API reachability and TLS/network config.
5. Queue growth/drop behavior: tune flush cadence and traffic profile.
