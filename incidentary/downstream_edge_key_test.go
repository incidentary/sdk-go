package incidentary

import (
	"strings"
	"testing"
)

func TestDownstreamEdgeResolverPrefersExplicitMetadata(t *testing.T) {
	resolver := DownstreamEdgeKeyResolver{}
	resolved := resolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID: "trace-1",
		Method:  "POST",
		URL:     "https://billing.internal/charges/123/capture?expand=true",
		Metadata: &DownstreamEdgeMetadata{
			RetryGroupID:      "retry-group-77",
			RouteTemplate:     "/charges/:id/capture",
			DownstreamService: "billing",
		},
	})

	if resolved.KeyQuality != RetryKeyQualityExplicit {
		t.Fatalf("expected explicit quality, got %s", resolved.KeyQuality)
	}
	if resolved.OperationKey != "retry-group-77" {
		t.Fatalf("unexpected operation key: %s", resolved.OperationKey)
	}
}

func TestDownstreamEdgeResolverRouteTemplateStability(t *testing.T) {
	resolver := DownstreamEdgeKeyResolver{}
	a := resolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID: "trace-2",
		Method:  "POST",
		URL:     "https://billing.internal/charges/123/capture",
		Metadata: &DownstreamEdgeMetadata{
			RouteTemplate:     "/charges/:id/capture",
			DownstreamService: "billing",
		},
	})
	b := resolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID: "trace-2",
		Method:  "POST",
		URL:     "https://billing.internal/charges/456/capture",
		Metadata: &DownstreamEdgeMetadata{
			RouteTemplate:     "/charges/:id/capture",
			DownstreamService: "billing",
		},
	})

	if a.KeyQuality != RetryKeyQualityRouteTemplate || b.KeyQuality != RetryKeyQualityRouteTemplate {
		t.Fatalf("expected route_template quality")
	}
	if a.KeyForHash != b.KeyForHash {
		t.Fatalf("expected stable key across dynamic ids")
	}
}

func TestDownstreamEdgeResolverNormalizedURLFallback(t *testing.T) {
	resolver := DownstreamEdgeKeyResolver{}
	a := resolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID: "trace-4",
		Method:  "GET",
		URL:     "https://orders.internal/users/123/orders/550e8400-e29b-41d4-a716-446655440000?state=open",
	})
	b := resolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID: "trace-4",
		Method:  "GET",
		URL:     "https://orders.internal/users/456/orders/550e8400-e29b-41d4-a716-446655440111?state=closed",
	})

	if a.KeyQuality != RetryKeyQualityNormalizedURL {
		t.Fatalf("expected normalized_url quality")
	}
	if a.RouteKey != "/users/:id/orders/:id" {
		t.Fatalf("unexpected normalized route: %s", a.RouteKey)
	}
	if a.KeyForHash != b.KeyForHash {
		t.Fatalf("expected equivalent normalized key")
	}
}

func TestDownstreamEdgeResolverDistinctRouteTemplatesStayDistinct(t *testing.T) {
	resolver := DownstreamEdgeKeyResolver{}
	capture := resolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID: "trace-5",
		Method:  "POST",
		URL:     "https://billing.internal/charges/123/capture",
		Metadata: &DownstreamEdgeMetadata{
			RouteTemplate:     "/charges/:id/capture",
			DownstreamService: "billing",
		},
	})
	refund := resolver.Resolve(ResolveDownstreamEdgeInput{
		TraceID: "trace-5",
		Method:  "POST",
		URL:     "https://billing.internal/charges/123/refund",
		Metadata: &DownstreamEdgeMetadata{
			RouteTemplate:     "/charges/:id/refund",
			DownstreamService: "billing",
		},
	})

	if capture.KeyForHash == refund.KeyForHash {
		t.Fatalf("expected different keys for distinct operations")
	}
}

func TestDownstreamEdgeResolverEdgeCases(t *testing.T) {
	resolver := DownstreamEdgeKeyResolver{}

	tests := []struct {
		name            string
		input           ResolveDownstreamEdgeInput
		wantKeyQuality  DownstreamEdgeKeyQuality
		wantEdgeKey     string
		wantRouteKey    string
		wantMethodInOp  string // if non-empty, assert operationKey starts with this
	}{
		{
			name:           "nil metadata",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "GET", URL: "https://svc/api", Metadata: nil},
			wantKeyQuality: RetryKeyQualityNormalizedURL,
			wantEdgeKey:    "svc",
			wantRouteKey:   "/api",
		},
		{
			name:           "empty URL",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "GET", URL: ""},
			wantKeyQuality: RetryKeyQualityUnknown,
			wantEdgeKey:    "unknown",
			wantRouteKey:   "/unknown",
		},
		{
			name:           "whitespace URL",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "GET", URL: "   "},
			wantKeyQuality: RetryKeyQualityUnknown,
			wantEdgeKey:    "unknown",
			wantRouteKey:   "/unknown",
		},
		{
			name:           "empty method defaults to GET",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "", URL: "https://svc.internal/api"},
			wantKeyQuality: RetryKeyQualityNormalizedURL,
			wantMethodInOp: "GET ",
		},
		{
			name:           "scheme-only URL",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "GET", URL: "https://"},
			wantKeyQuality: RetryKeyQualityUnknown,
			wantEdgeKey:    "unknown",
		},
		{
			name:           "URL with no path component",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "GET", URL: "https://service.internal"},
			wantKeyQuality: RetryKeyQualityNormalizedURL,
			wantEdgeKey:    "service.internal",
			wantRouteKey:   "/",
		},
		{
			name:           "all-whitespace metadata fields ignored",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "GET", URL: "https://svc.internal/api", Metadata: &DownstreamEdgeMetadata{RetryGroupID: "  ", RouteTemplate: "  "}},
			wantKeyQuality: RetryKeyQualityNormalizedURL,
			wantEdgeKey:    "svc.internal",
		},
		{
			name:           "path-only URL without scheme",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "GET", URL: "/api/v1/users/123"},
			wantKeyQuality: RetryKeyQualityNormalizedURL,
			wantEdgeKey:    "local",
			wantRouteKey:   "/api/v1/users/:id",
		},
		{
			name:           "URL with query only no path",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "GET", URL: "https://svc?foo=bar"},
			wantKeyQuality: RetryKeyQualityNormalizedURL,
			wantEdgeKey:    "svc",
			wantRouteKey:   "/",
		},
		{
			name:           "very long URL does not panic",
			input:          ResolveDownstreamEdgeInput{TraceID: "t", Method: "GET", URL: "https://svc.internal/" + strings.Repeat("a", 10000)},
			wantKeyQuality: RetryKeyQualityNormalizedURL,
			wantEdgeKey:    "svc.internal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved := resolver.Resolve(tt.input)
			if resolved.KeyQuality != tt.wantKeyQuality {
				t.Fatalf("keyQuality: got %q, want %q", resolved.KeyQuality, tt.wantKeyQuality)
			}
			if tt.wantEdgeKey != "" && resolved.EdgeKey != tt.wantEdgeKey {
				t.Fatalf("edgeKey: got %q, want %q", resolved.EdgeKey, tt.wantEdgeKey)
			}
			if tt.wantRouteKey != "" && resolved.RouteKey != tt.wantRouteKey {
				t.Fatalf("routeKey: got %q, want %q", resolved.RouteKey, tt.wantRouteKey)
			}
			if tt.wantMethodInOp != "" && !strings.HasPrefix(resolved.OperationKey, tt.wantMethodInOp) {
				t.Fatalf("operationKey: got %q, want prefix %q", resolved.OperationKey, tt.wantMethodInOp)
			}
		})
	}
}
