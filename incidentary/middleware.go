package incidentary

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type traceContextKey string

const (
	traceIDCtxKey traceContextKey = "incidentary.trace_id"
	ceIDCtxKey    traceContextKey = "incidentary.ce_id"
)

type TraceContext struct {
	TraceID string
	CeID    string
}

type OutboundRetryMetadata struct {
	RetryAttempt      *int
	IsRetry           *bool
	RetryGroupID      string
	IdempotencyKey    string
	OperationKey      string
	RetryKey          string
	RouteTemplate     string
	RouteKey          string
	EdgeKey           string
	DownstreamService string
	OperationName     string
}

var downstreamEdgeResolver = DownstreamEdgeKeyResolver{}

// Middleware returns an http.Handler that instruments inbound requests.
func Middleware(client *Client, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get(TraceIDHeader)
		parentCe := r.Header.Get(ParentCeHeader)
		if traceID == "" {
			traceID = randomUUID()
		}
		ceID := randomUUID()

		rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()

		ctx := r.Context()
		r = r.WithContext(setTraceContext(ctx, traceID, ceID))

		if client != nil {
			client.RecordRequestStart(KindHTTPIn)
		}

		next.ServeHTTP(rw, r)

		if client == nil {
			return
		}

		durationNs := time.Since(start).Nanoseconds()
		client.RecordRequestWithOptions(rw.status, RecordRequestOptions{
			Kind:       KindHTTPIn,
			DurationNs: durationNs,
		})

			ce := &SkeletonCe{
				CeID:       ceID,
				TraceID:    traceID,
				ParentCeID: parentCe,
				ServiceID:  client.ServiceName,
				WallTsNs:   time.Now().UnixNano(),
				Kind:       KindHTTPIn,
				EventType:  string(EventHTTPIn),
				EventClass: "causal",
				Status:     rw.status,
				DurationNs: durationNs,
				SdkVersion: "0.2.0",
			}
		detail := buildInboundDetail(client, r, rw)
		ce = client.AttachDetailToEvent(ce, detail)
		client.WriteEvent(ce)
	})
}

// InstrumentedDo wraps outbound HTTP calls with retry-aware trigger signals.
func InstrumentedDo(
	client *Client,
	httpClient *http.Client,
	parent *TraceContext,
	req *http.Request,
	metadata *OutboundRetryMetadata,
) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("incidentary: request is nil")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	traceID := ""
	parentCe := ""
	if parent != nil {
		traceID = parent.TraceID
		parentCe = parent.CeID
	}
	if traceID == "" {
		traceID = req.Header.Get(TraceIDHeader)
	}
	if traceID == "" {
		traceID = randomUUID()
	}

	ceID := randomUUID()
	clonedReq := req.Clone(req.Context())
	InjectTraceContext(clonedReq, traceID, ceID)

	method := strings.ToUpper(strings.TrimSpace(clonedReq.Method))
	if method == "" {
		method = http.MethodGet
	}

	resolution := downstreamEdgeResolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID:  traceID,
		Method:   method,
		URL:      clonedReq.URL.String(),
		Metadata: metadataToDownstreamMetadata(metadata),
	})

	explicitRetry := extractExplicitRetryObserved(metadata)
	retryHash := hashRetryIdentity(resolution.KeyForHash)

	status := 0
	cancelled := false
	timedOut := false
	responseHeaders := http.Header{}
	start := time.Now()

	if client != nil {
		client.RecordRequestStart(KindHTTPOut)
	}

	resp, err := httpClient.Do(clonedReq)
	if err == nil && resp != nil {
		status = resp.StatusCode
		responseHeaders = resp.Header.Clone()
	} else if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			timedOut = true
			cancelled = true
		}
	}

	if client != nil {
		durationNs := time.Since(start).Nanoseconds()
		client.RecordRequestWithOptions(status, RecordRequestOptions{
			Kind:                  KindHTTPOut,
			DurationNs:            durationNs,
			Cancelled:             cancelled,
			TimedOut:              timedOut,
			OutboundRetryKeyHash:  retryHash,
			OutboundRetryQuality:  resolution.KeyQuality,
			ExplicitRetryObserved: explicitRetry,
		})

			ce := &SkeletonCe{
				CeID:       ceID,
				TraceID:    traceID,
				ParentCeID: parentCe,
				ServiceID:  client.ServiceName,
				WallTsNs:   time.Now().UnixNano(),
				Kind:       KindHTTPOut,
				EventType:  string(EventHTTPOut),
				EventClass: "causal",
				Status:     status,
				DurationNs: durationNs,
				SdkVersion: "0.2.0",
			}
		detail := buildOutboundDetail(client, clonedReq, responseHeaders, resolution, metadata, explicitRetry, cancelled, timedOut)
		ce = client.AttachDetailToEvent(ce, detail)
		client.WriteEvent(ce)
	}

	return resp, err
}

// InjectTraceContext injects trace propagation headers for outbound requests.
func InjectTraceContext(req *http.Request, traceID, ceID string) {
	if req == nil {
		return
	}
	req.Header.Set(TraceIDHeader, traceID)
	req.Header.Set(ParentCeHeader, ceID)
}

// ContextTrace returns trace and ce ids stored in request context.
func ContextTrace(ctx context.Context) (traceID string, ceID string) {
	traceValue, _ := ctx.Value(traceIDCtxKey).(string)
	ceValue, _ := ctx.Value(ceIDCtxKey).(string)
	return traceValue, ceValue
}

func setTraceContext(ctx context.Context, traceID, ceID string) context.Context {
	ctx = context.WithValue(ctx, traceIDCtxKey, traceID)
	ctx = context.WithValue(ctx, ceIDCtxKey, ceID)
	return ctx
}

func randomUUID() string {
	buf := make([]byte, 16)
	_, _ = rand.Read(buf)
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	hexv := hex.EncodeToString(buf)
	parts := []string{hexv[0:8], hexv[8:12], hexv[12:16], hexv[16:20], hexv[20:32]}
	return strings.Join(parts, "-")
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func buildInboundDetail(client *Client, req *http.Request, rw *responseWriter) *CeDetail {
	if client == nil || !client.ShouldCaptureDetailForCurrentMode() {
		return nil
	}

	resolution := downstreamEdgeResolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID: "local",
		Method:  req.Method,
		URL:     req.URL.Path,
	})

	requestBytes := req.ContentLength
	if requestBytes < 0 {
		requestBytes = 0
	}

	detail := &CeDetail{
		Method:                   strings.ToUpper(req.Method),
		RouteKey:                 resolution.RouteKey,
		RequestBytes:             requestBytes,
		ResponseBytes:            parseContentLength(rw.Header().Get("content-length")),
		RequestHeaders:           filterHeaders(req.Header, client.GetDetailRequestHeaderAllowlist()),
		ResponseHeaders:          filterHeaders(rw.Header(), client.GetDetailResponseHeaderAllowlist()),
		LocalErrorClassification: "none",
	}
	return detail
}

func buildOutboundDetail(
	client *Client,
	req *http.Request,
	respHeaders http.Header,
	resolution DownstreamEdgeKeyResolution,
	metadata *OutboundRetryMetadata,
	explicitRetry *bool,
	cancelled bool,
	timedOut bool,
) *CeDetail {
	if client == nil || !client.ShouldCaptureDetailForCurrentMode() {
		return nil
	}

	requestBytes := req.ContentLength
	if requestBytes < 0 {
		requestBytes = 0
	}

	classification := "none"
	if timedOut {
		classification = "timeout"
	} else if cancelled {
		classification = "cancelled"
	}

	var payload string
	if req.GetBody != nil {
		if body, err := req.GetBody(); err == nil {
			defer body.Close()
			buf := make([]byte, 4_096)
			n, _ := body.Read(buf)
			if n > 0 {
				payload = string(buf[:n])
			}
		}
	}

	detail := &CeDetail{
		Method:          strings.ToUpper(req.Method),
		RouteKey:        resolution.RouteKey,
		RouteTemplate:   metadataField(metadata, func(m *OutboundRetryMetadata) string { return m.RouteTemplate }),
		RequestBytes:    requestBytes,
		ResponseBytes:   parseContentLength(respHeaders.Get("content-length")),
		RequestHeaders:  filterHeaders(req.Header, client.GetDetailRequestHeaderAllowlist()),
		ResponseHeaders: filterHeaders(respHeaders, client.GetDetailResponseHeaderAllowlist()),
		Retry: map[string]interface{}{
			"explicit_observed": explicitRetryValue(explicitRetry),
			"key_quality":       resolution.KeyQuality,
			"edge_key":          resolution.EdgeKey,
			"operation_key":     resolution.OperationKey,
		},
		Downstream: map[string]interface{}{
			"edge_key":       resolution.EdgeKey,
			"service":        metadataField(metadata, func(m *OutboundRetryMetadata) string { return m.DownstreamService }),
			"operation_name": metadataField(metadata, func(m *OutboundRetryMetadata) string { return m.OperationName }),
			"key_quality":    resolution.KeyQuality,
		},
		LocalErrorClassification: classification,
		PayloadSnippet:           payload,
	}
	return detail
}

func filterHeaders(headers http.Header, allowlist []string) map[string]string {
	if len(allowlist) == 0 {
		return nil
	}
	out := make(map[string]string)
	for _, key := range allowlist {
		value := headers.Get(key)
		if strings.TrimSpace(value) != "" {
			out[strings.ToLower(key)] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func parseContentLength(value string) int64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}

func metadataToDownstreamMetadata(metadata *OutboundRetryMetadata) *DownstreamEdgeMetadata {
	if metadata == nil {
		return nil
	}
	return &DownstreamEdgeMetadata{
		RetryGroupID:      metadata.RetryGroupID,
		IdempotencyKey:    metadata.IdempotencyKey,
		OperationKey:      metadata.OperationKey,
		RetryKey:          metadata.RetryKey,
		RouteTemplate:     metadata.RouteTemplate,
		RouteKey:          metadata.RouteKey,
		EdgeKey:           metadata.EdgeKey,
		DownstreamService: metadata.DownstreamService,
		OperationName:     metadata.OperationName,
	}
}

func metadataField(metadata *OutboundRetryMetadata, getter func(*OutboundRetryMetadata) string) string {
	if metadata == nil {
		return ""
	}
	return getter(metadata)
}

func extractExplicitRetryObserved(metadata *OutboundRetryMetadata) *bool {
	if metadata == nil {
		return nil
	}
	if metadata.RetryAttempt != nil {
		value := *metadata.RetryAttempt >= 2
		return &value
	}
	if metadata.IsRetry != nil {
		value := *metadata.IsRetry
		return &value
	}
	return nil
}

func explicitRetryValue(value *bool) interface{} {
	if value == nil {
		return nil
	}
	return *value
}

func hashRetryIdentity(identity string) uint64 {
	var hash uint64 = 0xcbf29ce484222325
	for i := 0; i < len(identity); i++ {
		hash ^= uint64(identity[i])
		hash *= 0x100000001b3
	}
	return hash
}
