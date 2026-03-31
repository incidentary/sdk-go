package incidentary

import (
	"regexp"
	"strings"
)

// Downstream edge key quality priority: explicit > route_template > logical_edge > normalized_url > unknown.
type DownstreamEdgeKeyQuality string

const (
	RetryKeyQualityExplicit      DownstreamEdgeKeyQuality = "explicit"
	RetryKeyQualityRouteTemplate DownstreamEdgeKeyQuality = "route_template"
	RetryKeyQualityLogicalEdge   DownstreamEdgeKeyQuality = "logical_edge"
	RetryKeyQualityNormalizedURL DownstreamEdgeKeyQuality = "normalized_url"
	RetryKeyQualityUnknown       DownstreamEdgeKeyQuality = "unknown"
)

type DownstreamEdgeMetadata struct {
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

type DownstreamEdgeKeyResolution struct {
	EdgeKey      string
	RouteKey     string
	KeyQuality   DownstreamEdgeKeyQuality
	OperationKey string
	KeyForHash   string
}

type ResolveDownstreamEdgeInput struct {
	TraceID  string
	Method   string
	URL      string
	Metadata *DownstreamEdgeMetadata
}

type DownstreamEdgeKeyResolver struct{}

var (
	uuidSegmentRE    = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	numericSegmentRE = regexp.MustCompile(`^\d+$`)
	longHexSegmentRE = regexp.MustCompile(`(?i)^[0-9a-f]{16,}$`)
)

func (DownstreamEdgeKeyResolver) Resolve(input ResolveDownstreamEdgeInput) DownstreamEdgeKeyResolution {
	method := normalizeToken(strings.ToUpper(input.Method), "GET")
	normalizedEdge, normalizedRoute := normalizeURLTarget(input.URL)
	md := input.Metadata

	explicitKey := firstNonEmpty(
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.RetryGroupID }),
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.IdempotencyKey }),
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.OperationKey }),
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.RetryKey }),
	)

	if explicitKey != "" {
		edgeKey := firstNonEmpty(
			valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.EdgeKey }),
			valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.DownstreamService }),
			normalizedEdge,
		)
		routeKey := firstNonEmpty(
			valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.RouteTemplate }),
			valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.RouteKey }),
			normalizedRoute,
		)
		if edgeKey == "" {
			edgeKey = "unknown"
		}
		if routeKey == "" {
			routeKey = "/"
		}

		return DownstreamEdgeKeyResolution{
			EdgeKey:      edgeKey,
			RouteKey:     routeKey,
			KeyQuality:   RetryKeyQualityExplicit,
			OperationKey: explicitKey,
			KeyForHash:   input.TraceID + "|" + edgeKey + "|" + method + "|explicit:" + explicitKey,
		}
	}

	routeTemplate := firstNonEmpty(
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.RouteTemplate }),
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.RouteKey }),
	)
	if routeTemplate != "" {
		edgeKey := firstNonEmpty(
			valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.EdgeKey }),
			valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.DownstreamService }),
			normalizedEdge,
		)
		if edgeKey == "" {
			edgeKey = "unknown"
		}
		canonicalRoute := canonicalizeRoute(routeTemplate)
		operationKey := method + " " + canonicalRoute
		return DownstreamEdgeKeyResolution{
			EdgeKey:      edgeKey,
			RouteKey:     canonicalRoute,
			KeyQuality:   RetryKeyQualityRouteTemplate,
			OperationKey: operationKey,
			KeyForHash:   input.TraceID + "|" + edgeKey + "|" + operationKey,
		}
	}

	logicalEdge := firstNonEmpty(
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.DownstreamService }),
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.EdgeKey }),
	)
	operationName := firstNonEmpty(
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.OperationName }),
		valueOrEmpty(md, func(v *DownstreamEdgeMetadata) string { return v.OperationKey }),
	)
	if logicalEdge != "" || operationName != "" {
		edgeKey := logicalEdge
		if edgeKey == "" {
			edgeKey = normalizedEdge
		}
		op := operationName
		if op == "" {
			op = method + " " + normalizedRoute
		}
		return DownstreamEdgeKeyResolution{
			EdgeKey:      edgeKey,
			RouteKey:     normalizedRoute,
			KeyQuality:   RetryKeyQualityLogicalEdge,
			OperationKey: op,
			KeyForHash:   input.TraceID + "|" + edgeKey + "|logical:" + op,
		}
	}

	if normalizedEdge != "unknown" || normalizedRoute != "/unknown" {
		op := method + " " + normalizedRoute
		return DownstreamEdgeKeyResolution{
			EdgeKey:      normalizedEdge,
			RouteKey:     normalizedRoute,
			KeyQuality:   RetryKeyQualityNormalizedURL,
			OperationKey: op,
			KeyForHash:   input.TraceID + "|" + normalizedEdge + "|" + op,
		}
	}

	return DownstreamEdgeKeyResolution{
		EdgeKey:      "unknown",
		RouteKey:     "/unknown",
		KeyQuality:   RetryKeyQualityUnknown,
		OperationKey: method + " unknown",
		KeyForHash:   input.TraceID + "|unknown|" + method + "|unknown",
	}
}

func valueOrEmpty(md *DownstreamEdgeMetadata, getter func(*DownstreamEdgeMetadata) string) string {
	if md == nil {
		return ""
	}
	return getter(md)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func normalizeURLTarget(raw string) (string, string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "unknown", "/unknown"
	}

	schemeIndex := strings.Index(trimmed, "://")
	if schemeIndex >= 0 {
		hostStart := schemeIndex + 3
		if hostStart >= len(trimmed) {
			return "unknown", "/unknown"
		}

		pathStart := indexOfFirst(trimmed, hostStart, '/', '?', '#')
		edgeRaw := ""
		pathRaw := "/"
		if pathStart >= 0 {
			edgeRaw = trimmed[hostStart:pathStart]
			pathRaw = trimmed[pathStart:]
		} else {
			edgeRaw = trimmed[hostStart:]
		}

		edge := normalizeToken(edgeRaw, "unknown")
		route := canonicalizeRoute(pathRaw)
		return edge, route
	}

	return "local", canonicalizeRoute(trimmed)
}

func canonicalizeRoute(route string) string {
	cleaned := route
	if cleaned == "" {
		cleaned = "/"
	}

	if idx := strings.IndexByte(cleaned, '?'); idx >= 0 {
		cleaned = cleaned[:idx]
	}
	if idx := strings.IndexByte(cleaned, '#'); idx >= 0 {
		cleaned = cleaned[:idx]
	}
	if cleaned == "" {
		cleaned = "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}

	parts := strings.Split(cleaned, "/")
	for i, part := range parts {
		if part == "" {
			continue
		}
		if numericSegmentRE.MatchString(part) || uuidSegmentRE.MatchString(part) || longHexSegmentRE.MatchString(part) {
			parts[i] = ":id"
		}
	}

	normalized := strings.Join(parts, "/")
	if normalized == "" {
		return "/"
	}
	return normalized
}

func normalizeToken(value, fallback string) string {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return fallback
	}
	return normalized
}

func indexOfFirst(input string, start int, chars ...byte) int {
	min := -1
	for i := start; i < len(input); i++ {
		for _, char := range chars {
			if input[i] == char {
				if min < 0 || i < min {
					min = i
				}
				break
			}
		}
		if min >= 0 {
			return min
		}
	}
	return min
}
