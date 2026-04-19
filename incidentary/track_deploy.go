package incidentary

// Record a deployment with Incidentary. Fail-open by design — the
// function never returns an error because a broken deploy tracker
// must never break a deploy. Transport and HTTP errors surface as
// nil-return + an entry in the configured logger.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	trackDeployEndpoint       = "/api/v1/deploys"
	trackDeployDefaultTimeout = 5 * time.Second
)

// TrackDeployConfig is the minimal transport surface needed to call
// the deploy-tracking endpoint. Kept separate from Config so CI
// scripts can track a deploy without constructing a full SDK client.
type TrackDeployConfig struct {
	BaseURL    string
	APIKey     string
	Timeout    time.Duration       // defaults to 5s
	HTTPClient *http.Client        // defaults to a timeout-bound client
	Logger     TrackDeployLogger   // optional — stdlib log.Default() if nil
	Context    context.Context     // optional — used for the outbound request
}

// TrackDeployLogger is a minimal interface so callers can plug in
// their own logger without pulling in a logging library dep.
type TrackDeployLogger interface {
	Warnf(format string, args ...interface{})
}

// TrackDeployOptions describes a single deploy event. Only Service
// is required; unset optional fields are omitted from the request
// body rather than sent as JSON null.
type TrackDeployOptions struct {
	Service         string
	Version         string
	CommitSHA       string
	CommitMessage   string
	Branch          string
	DeployedByName  string
	DeployedByEmail string
	DeployedAt      time.Time
	Environment     string
	DiffURL         string
	Metadata        map[string]interface{}
}

// TrackDeploy POSTs a deploy record to Incidentary. It never returns
// a non-nil error — the error return exists purely for future
// extension and is always nil today. Failures are logged via the
// configured logger (or log.Default() if none).
func TrackDeploy(cfg TrackDeployConfig, opts TrackDeployOptions) error {
	warnf := func(format string, args ...interface{}) {
		if cfg.Logger != nil {
			cfg.Logger.Warnf(format, args...)
			return
		}
		log.Printf("incidentary.TrackDeploy: "+format, args...)
	}

	if strings.TrimSpace(opts.Service) == "" {
		warnf("service is required — skipping")
		return nil
	}

	url := strings.TrimRight(cfg.BaseURL, "/") + trackDeployEndpoint
	body := buildDeployBody(opts)
	payload, err := json.Marshal(body)
	if err != nil {
		warnf("failed to marshal body: %v", err)
		return nil
	}

	ctx := cfg.Context
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		warnf("failed to build request: %v", err)
		return nil
	}
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	req.Header.Set("Content-Type", "application/json")

	client := cfg.HTTPClient
	if client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = trackDeployDefaultTimeout
		}
		client = &http.Client{Timeout: timeout}
	}

	resp, err := client.Do(req)
	if err != nil {
		warnf("failed: %v", err)
		return nil
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		warnf("failed: %s", fmt.Sprintf("HTTP %d", resp.StatusCode))
	}
	return nil
}

func buildDeployBody(opts TrackDeployOptions) map[string]interface{} {
	deployedAt := opts.DeployedAt
	if deployedAt.IsZero() {
		deployedAt = time.Now().UTC()
	}
	environment := opts.Environment
	if environment == "" {
		environment = "production"
	}
	metadata := opts.Metadata
	if metadata == nil {
		metadata = map[string]interface{}{}
	}

	body := map[string]interface{}{
		"service_name":  opts.Service,
		"deploy_source": "sdk",
		"environment":   environment,
		"deployed_at":   deployedAt.UTC().Format(time.RFC3339Nano),
		"metadata":      metadata,
	}

	set := func(key, value string) {
		if value != "" {
			body[key] = value
		}
	}
	set("version", opts.Version)
	set("commit_sha", opts.CommitSHA)
	set("commit_message", opts.CommitMessage)
	set("branch", opts.Branch)
	set("deployed_by_name", opts.DeployedByName)
	set("deployed_by_email", opts.DeployedByEmail)
	set("diff_url", opts.DiffURL)

	return body
}
