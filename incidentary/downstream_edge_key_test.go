package incidentary

import "testing"

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
