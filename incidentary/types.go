package incidentary

const (
	TraceIDHeader    = "x-incidentary-trace-id"
	ParentCeHeader   = "x-incidentary-parent-ce"
	SDKVersionHeader = "X-Incidentary-SDK-Version"
)

type CaptureMode string

const (
	ModeNormal   CaptureMode = "NORMAL"
	ModePreArmed CaptureMode = "PRE_ARMED"
	ModeIncident CaptureMode = "INCIDENT"
)

type IngestCaptureMode string

const (
	IngestModeSkeleton IngestCaptureMode = "SKELETON"
	IngestModeFull     IngestCaptureMode = "FULL"
)

type CeKind string

const (
	KindHTTPIn       CeKind = "HTTP_IN"
	KindHTTPOut      CeKind = "HTTP_OUT"
	KindQueuePublish CeKind = "QUEUE_PUBLISH"
	KindQueueConsume CeKind = "QUEUE_CONSUME"
	KindInternal     CeKind = "INTERNAL"
)

type IncidentaryEventType string

const (
	EventHTTPIn       IncidentaryEventType = "http_in"
	EventHTTPOut      IncidentaryEventType = "http_out"
	EventQueuePublish IncidentaryEventType = "queue_publish"
	EventQueueConsume IncidentaryEventType = "queue_consume"
	EventJobStart     IncidentaryEventType = "job_start"
	EventJobEnd       IncidentaryEventType = "job_end"
	EventWebhookIn    IncidentaryEventType = "webhook_in"
	EventWebhookOut   IncidentaryEventType = "webhook_out"
	EventInternalTask IncidentaryEventType = "internal_task"
	EventDBQuery      IncidentaryEventType = "db_query"
	EventGRPCIn       IncidentaryEventType = "grpc_in"
	EventGRPCOut      IncidentaryEventType = "grpc_out"
)

// CeDetail is optional richer event detail attached in PRE_ARMED/INCIDENT modes.
type CeDetail struct {
	Method                   string                 `json:"method,omitempty"`
	RouteKey                 string                 `json:"route_key,omitempty"`
	RouteTemplate            string                 `json:"route_template,omitempty"`
	RequestBytes             int64                  `json:"request_bytes,omitempty"`
	ResponseBytes            int64                  `json:"response_bytes,omitempty"`
	RequestHeaders           map[string]string      `json:"request_headers,omitempty"`
	ResponseHeaders          map[string]string      `json:"response_headers,omitempty"`
	Retry                    map[string]interface{} `json:"retry,omitempty"`
	Downstream               map[string]interface{} `json:"downstream,omitempty"`
	LocalErrorClassification string                 `json:"local_error_classification,omitempty"`
	PayloadSnippet           string                 `json:"payload_snippet,omitempty"`
}

// SkeletonCe is the minimal causal event captured in the ring buffer.
type SkeletonCe struct {
	CeID       string    `json:"ce_id"`
	TraceID    string    `json:"trace_id"`
	ParentCeID string    `json:"parent_ce_id,omitempty"` // empty = no parent
	ServiceID  string    `json:"service_id"`
	WallTsNs   int64     `json:"wall_ts_ns"`
	CapturedBeforeAlert *bool    `json:"captured_before_alert,omitempty"`
	RingBufferSeq       *int64   `json:"ring_buffer_seq,omitempty"`
	Kind       CeKind    `json:"kind"`
	EventType  string    `json:"event_type,omitempty"`
	EventClass string    `json:"event_class,omitempty"`
	EventAttrs any       `json:"event_attrs,omitempty"`
	Status     int       `json:"status"`
	DurationNs int64     `json:"duration_ns"`
	SdkVersion string    `json:"sdk_version"`
	Detail     *CeDetail `json:"detail,omitempty"`
}

type RecordRequestOptions struct {
	Kind                  CeKind
	DurationNs            int64
	Cancelled             bool
	TimedOut              bool
	OutboundRetryKeyHash  uint64
	OutboundRetryQuality  DownstreamEdgeKeyQuality
	ExplicitRetryObserved *bool
}

type RecordEventOptions struct {
	TraceID    string
	ParentCeID string
	Status     *int
	DurationNs int64
	WallTsNs   int64
	EventAttrs map[string]interface{}
}

type PreArmTriggerReason struct {
	TriggerType    TriggerType            `json:"trigger_type"`
	Severity       TriggerSeverity        `json:"severity"`
	ObservedValue  float64                `json:"observed_value"`
	ThresholdValue float64                `json:"threshold_value"`
	ObservedLabel  string                 `json:"observed_label"`
	ThresholdLabel string                 `json:"threshold_label"`
	FiredAtUnixMs  int64                  `json:"fired_at_unix_ms"`
	Summary        string                 `json:"summary"`
	Details        map[string]interface{} `json:"details"`
}

type PreArmWindow struct {
	ID              string                `json:"id"`
	StartedAtMs     int64                 `json:"started_at_ms"`
	ExpiresAtMs     int64                 `json:"expires_at_ms"`
	Reasons         []PreArmTriggerReason `json:"reasons"`
	BoundIncidentID string                `json:"bound_incident_id,omitempty"`
	ClosedAtMs      int64                 `json:"closed_at_ms,omitempty"`
	CloseReason     string                `json:"close_reason,omitempty"`
}

type PreArmDebugState struct {
	Counters              map[string]uint64      `json:"counters"`
	Gauges                map[string]interface{} `json:"gauges"`
	RetryKeyQuality10s    map[string]int64       `json:"retry_key_quality_10s"`
	RetryKeyQualityTotal  map[string]int64       `json:"retry_key_quality_total"`
	LastTrigger           map[string]interface{} `json:"last_trigger"`
	ActivePreArmWindow    *PreArmWindow          `json:"active_prearm_window"`
	RecentPreArmWindows   []PreArmWindow         `json:"recent_prearm_windows"`
	TriggerEngineDisabled map[string]bool        `json:"trigger_engine_disabled"`
}
