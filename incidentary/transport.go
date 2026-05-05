package incidentary

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

const (
	sdkVersion = "1.0.0"
)

type ceBatch struct {
	SpecVersion string            `json:"specversion"`
	Resource    batchResource     `json:"resource"`
	Agent       batchAgent        `json:"agent"`
	FlushedAt   int64             `json:"flushed_at"`
	CaptureMode IngestCaptureMode `json:"capture_mode"`
	Events      []*SkeletonCe     `json:"events"`
}

type batchResource struct {
	ServiceName string `json:"service.name"`
	Environment string `json:"deployment.environment"`
}

type batchAgent struct {
	Type        string                 `json:"type"`
	Version     string                 `json:"version"`
	Language    string                 `json:"language"`
	WorkspaceID string                 `json:"workspace_id"`
	Telemetry   map[string]interface{} `json:"telemetry,omitempty"`
}

type ingestResponse struct {
	Accepted    int               `json:"accepted"`
	Dropped     int               `json:"dropped"`
	DropReasons map[string]int    `json:"drop_reasons,omitempty"`
}

// Transport is a fail-open background uploader.
type Transport struct {
	mu              sync.Mutex
	baseURL         string
	apiKey          string
	serviceName     string
	environment     string
	workspaceID     string
	timeoutMs       int64
	onError         func(error)
	backendHealthy  bool
	consecutiveFail int
	circuitOpenTill time.Time
	quotaPauseUntil time.Time
}

func NewTransport(baseURL, apiKey, serviceName, environment string, timeoutMs int64, onError func(error)) *Transport {
	if timeoutMs <= 0 {
		timeoutMs = 5_000
	}
	transport := &Transport{
		baseURL:        stringsTrim(baseURL),
		apiKey:         apiKey,
		serviceName:    serviceName,
		environment:    environment,
		timeoutMs:      timeoutMs,
		onError:        onError,
		backendHealthy: true,
	}
	if transport.baseURL == "" {
		transport.warnMissingBaseURL()
	}
	return transport
}

// UploadBatch preserves previous API and sends in skeleton mode.
func (t *Transport) UploadBatch(events []*SkeletonCe) {
	t.UploadBatchWithMode(events, ModeNormal, "")
}

func (t *Transport) UploadBatchWithMode(events []*SkeletonCe, mode CaptureMode, incidentID string) {
	t.UploadBatchWithModeAndTelemetry(events, mode, incidentID, nil, nil)
}

// UploadBatchWithModeAndTelemetry uploads a batch with optional agent telemetry
// and an optional callback invoked with a FlushResult on successful upload.
// The FlushResult carries round-trip latency and any server-requested capture
// mode (from the X-Capture-Mode-Requested response header).
func (t *Transport) UploadBatchWithModeAndTelemetry(events []*SkeletonCe, mode CaptureMode, incidentID string, telemetry map[string]interface{}, onFlush func(FlushResult)) {
	defer func() { _ = recover() }()
	if len(events) == 0 || !t.canAttemptRequest() {
		return
	}

	agent := batchAgent{
		Type:        "sdk",
		Version:     sdkVersion,
		Language:    "go",
		WorkspaceID: t.workspaceID,
		Telemetry:   telemetry,
	}

	body, err := json.Marshal(ceBatch{
		SpecVersion: "2",
		Resource: batchResource{
			ServiceName: t.serviceName,
			Environment: t.environment,
		},
		Agent:       agent,
		FlushedAt:   time.Now().UnixNano(),
		CaptureMode: toIngestCaptureMode(mode),
		Events:      events,
	})
	if err != nil {
		return
	}

	go func() {
		flushStart := time.Now()
		client := &http.Client{Timeout: time.Duration(t.timeoutMs) * time.Millisecond}
		delays := []time.Duration{1 * time.Second, 4 * time.Second, 16 * time.Second}

		for attempt := 0; attempt <= len(delays); attempt++ {
			resp, reqErr := t.doJSONRequest(client, "/api/v2/ingest", body, incidentID)
			if reqErr == nil && resp != nil {
				data, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode == http.StatusUpgradeRequired {
					log.Printf(
						`{"event":"incidentary_agent_version_rejected","status":426,"payload":%q}`,
						string(data),
					)
					t.onSuccess()
					return
				}
				if resp.StatusCode == http.StatusTooManyRequests && t.pauseOnFreeCELimit(data) {
					return
				}
				if resp.StatusCode >= 200 && resp.StatusCode < 300 {
					t.parseIngestResponse(data)
					capMode := resp.Header.Get("X-Capture-Mode-Requested")
					if capMode != "" {
						log.Printf(`{"event":"incidentary_capture_mode_requested","mode":%q}`, capMode)
					}
					t.onSuccess()
					if onFlush != nil {
						latencyMs := float64(time.Since(flushStart).Microseconds()) / 1000.0
						onFlush(FlushResult{
							LatencyMs:            latencyMs,
							RequestedCaptureMode: capMode,
						})
					}
					return
				}
			}

			if attempt >= len(delays) {
				t.onFailure(newTransportError("Incidentary upload failed"))
				log.Printf(`{"event":"incidentary_flush_drop_after_retries","attempts":%d}`, len(delays)+1)
				return
			}
			time.Sleep(delays[attempt])
		}
	}()
}

func (t *Transport) NotifyBackend(event string, serviceID string, metadata map[string]interface{}) {
	defer func() { _ = recover() }()
	if stringsTrim(event) == "" || stringsTrim(serviceID) == "" || !t.canAttemptRequest() {
		return
	}

	payload, err := json.Marshal(map[string]interface{}{
		"service_id": serviceID,
		"event":      event,
		"metadata":   metadata,
	})
	if err != nil {
		return
	}

	go func() {
		client := &http.Client{Timeout: time.Duration(t.timeoutMs) * time.Millisecond}
		resp, reqErr := t.doJSONRequest(client, "/api/v1/services/events", payload, "")
		if reqErr == nil && resp != nil {
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				t.onSuccess()
				return
			}
		}
		t.onFailure(newTransportError("Incidentary service event upload failed"))
	}()
}

func (t *Transport) IsHealthy() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.baseURL == "" {
		return false
	}
	if !t.quotaPauseUntil.IsZero() && time.Now().Before(t.quotaPauseUntil) {
		return false
	}
	return t.backendHealthy || time.Now().After(t.circuitOpenTill)
}

func (t *Transport) canAttemptRequest() bool {
	if t.baseURL == "" {
		t.warnMissingBaseURL()
		return false
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.quotaPauseUntil.IsZero() {
		if time.Now().Before(t.quotaPauseUntil) {
			return false
		}
		t.quotaPauseUntil = time.Time{}
	}
	if t.backendHealthy {
		return true
	}
	if time.Now().After(t.circuitOpenTill) {
		t.backendHealthy = true
		t.consecutiveFail = 0
		return true
	}
	return false
}

func (t *Transport) doJSONRequest(client *http.Client, path string, body []byte, incidentID string) (*http.Response, error) {
	if t.baseURL == "" {
		return nil, nil
	}
	req, err := http.NewRequest(http.MethodPost, t.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+t.apiKey)
	if path == "/api/v2/ingest" {
		req.Header.Set(AgentVersionHeader, sdkVersion)
		if stringsTrim(incidentID) != "" {
			req.Header.Set("X-Incidentary-Incident-Id", incidentID)
		}
	}
	return client.Do(req)
}

func (t *Transport) onSuccess() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.backendHealthy = true
	t.consecutiveFail = 0
	t.circuitOpenTill = time.Time{}
}

func (t *Transport) onFailure(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.consecutiveFail++
	if t.consecutiveFail >= 3 {
		t.backendHealthy = false
		t.circuitOpenTill = time.Now().Add(60 * time.Second)
	}
	if t.onError != nil {
		go func(cb func(error), callbackErr error) {
			defer func() { _ = recover() }()
			cb(callbackErr)
		}(t.onError, err)
	}
}

func (t *Transport) warnMissingBaseURL() {
	t.mu.Lock()
	alreadyWarned := !t.backendHealthy && t.baseURL == ""
	if t.baseURL == "" {
		t.backendHealthy = false
	}
	t.mu.Unlock()
	if alreadyWarned {
		return
	}
	message := "Incidentary transport disabled because baseURL is not configured. Pass BaseURL explicitly when constructing the SDK client."
	log.Print(message)
	if t.onError != nil {
		go func() {
			defer func() { _ = recover() }()
			t.onError(newTransportError(message))
		}()
	}
}

func (t *Transport) pauseOnFreeCELimit(body []byte) bool {
	var payload struct {
		Error     string `json:"error"`
		LimitType string `json:"limit_type"`
		Plan      string `json:"plan"`
		Limit     *int   `json:"limit"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return false
	}
	if payload.Error != "ce_limit_reached" || payload.LimitType != "ce" || payload.Plan != "free" {
		return false
	}

	resetAt := nextUTCMonthStart(time.Now().UTC())
	t.mu.Lock()
	t.quotaPauseUntil = resetAt
	t.mu.Unlock()

	event := map[string]interface{}{
		"event":    "incidentary_ce_limit_reached",
		"plan":     "free",
		"reset_at": resetAt.Format(time.RFC3339),
	}
	if payload.Limit != nil {
		event["limit"] = *payload.Limit
	}
	if encoded, err := json.Marshal(event); err == nil {
		log.Print(string(encoded))
	}
	if t.onError != nil {
		go func() {
			defer func() { _ = recover() }()
			t.onError(newTransportError("Incidentary CE limit reached for the free plan. Pausing ingest until next billing cycle."))
		}()
	}
	return true
}

func (t *Transport) parseIngestResponse(data []byte) {
	var resp ingestResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return
	}
	if resp.Dropped > 0 {
		encoded, err := json.Marshal(map[string]interface{}{
			"event":        "incidentary_events_dropped",
			"dropped":      resp.Dropped,
			"accepted":     resp.Accepted,
			"drop_reasons": resp.DropReasons,
		})
		if err == nil {
			log.Print(string(encoded))
		}
	}
}

func nextUTCMonthStart(now time.Time) time.Time {
	year, month, _ := now.Date()
	if month == time.December {
		return time.Date(year+1, time.January, 1, 0, 0, 0, 0, time.UTC)
	}
	return time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC)
}

func newTransportError(message string) error {
	return transportError(message)
}

type transportError string

func (e transportError) Error() string { return string(e) }

func toIngestCaptureMode(mode CaptureMode) IngestCaptureMode {
	if mode == ModeNormal {
		return IngestModeSkeleton
	}
	return IngestModeFull
}

func stringsTrim(value string) string {
	trimmed := value
	for len(trimmed) > 0 && (trimmed[0] == ' ' || trimmed[0] == '\t' || trimmed[0] == '\n' || trimmed[0] == '\r') {
		trimmed = trimmed[1:]
	}
	for len(trimmed) > 0 {
		last := trimmed[len(trimmed)-1]
		if last == ' ' || last == '\t' || last == '\n' || last == '\r' {
			trimmed = trimmed[:len(trimmed)-1]
			continue
		}
		break
	}
	return trimmed
}
