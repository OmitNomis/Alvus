package main

// Shared key-rotation retry loop.
//
// The OpenAI pass-through (proxyHandler) and the Anthropic translation layer
// (anthropicHandler) both need the same behaviour: pick a healthy key, send,
// and on a rate-limit / auth / upstream failure cool down or disable that key
// and try the next one. That logic was duplicated in both handlers, which is
// how they drifted apart. It lives here once now; the handlers keep only the
// parts that genuinely differ (how they build the request and how they render
// a response).

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// responseHeaderTimeout bounds how long an upstream may sit on a request before
// sending response headers. It deliberately does NOT bound the body: see
// upstreamClient.
const responseHeaderTimeout = 120 * time.Second

// upstreamClient is shared across every request so connections pool.
//
// It has no http.Client.Timeout on purpose. That timeout covers reading the
// response body too, so a 120s value silently truncates any SSE stream still
// running after two minutes — which is exactly what a long agentic turn looks
// like. ResponseHeaderTimeout instead bounds the real hang risk (an upstream
// that accepts the connection and never answers) while letting a live stream
// run as long as it wants. Non-streaming calls get a per-attempt deadline via
// context instead; see rotateOpts.PerAttemptTimeout.
var upstreamClient = &http.Client{
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
	},
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// nonStreamTimeout caps a single non-streaming attempt end to end.
const nonStreamTimeout = 120 * time.Second

// wantsStream reports whether a proxied request asked for an SSE response,
// either through the OpenAI "stream" flag or an explicit Accept header. The
// generic proxy forwards arbitrary bodies, so this is a best-effort probe —
// guessing wrong only costs a deadline, never correctness.
func wantsStream(body []byte, h http.Header) bool {
	if strings.Contains(h.Get("Accept"), "text/event-stream") {
		return true
	}
	if len(body) == 0 {
		return false
	}
	var probe struct {
		Stream bool `json:"stream"`
	}
	return json.Unmarshal(body, &probe) == nil && probe.Stream
}

// buildFunc produces the upstream request for one attempt, with the pooled key
// already applied. It is called once per attempt because the body reader has
// to be fresh each time.
type buildFunc func(key string) (*http.Request, error)

type rotateOpts struct {
	// PerAttemptTimeout caps a single attempt end to end, body included. Zero
	// means no cap, which is what streaming responses need.
	PerAttemptTimeout time.Duration
}

// rotateResult is a response the caller now owns: it must close resp.Body and
// then call release.
type rotateResult struct {
	resp    *http.Response
	idx     int
	key     string
	attempt int
	// release tears down the attempt's context. It must not be called until
	// the body has been read, or an in-flight stream would be cancelled.
	release func()
}

// rotateError is a terminal failure, pre-classified so each handler can render
// it in its own wire format.
type rotateError struct {
	status int
	msg    string
}

// classify decides what to do with an upstream status.
//
//	retry    — cool the key down and try the next one
//	disable  — the key is bad, drop it from the pool permanently
//	terminal — hand the response back to the caller as-is
type disposition int

const (
	dispTerminal disposition = iota
	dispRetryCooldown
	dispRetryPlain
	dispDisable
)

func classify(status int) disposition {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable:
		return dispRetryCooldown
	case http.StatusUnauthorized, http.StatusForbidden:
		return dispDisable
	}
	if status >= 500 {
		return dispRetryPlain
	}
	return dispTerminal
}

// cooldownFor honours an upstream Retry-After when present, falling back to the
// configured fixed cooldown.
func cooldownFor(resp *http.Response, cfg Config) time.Duration {
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		if secs, err := strconv.Atoi(ra); err == nil {
			return time.Duration(secs+2) * time.Second
		}
	}
	return time.Duration(cfg.CooldownSec) * time.Second
}

// sleepCtx waits for d, or bails out early if the client goes away.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// backoffFor spaces out retries against an upstream that is failing, so a run
// of 5xxs doesn't fire MaxRetries requests as fast as the network allows.
func backoffFor(attempt int) time.Duration {
	d := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
	if d > 2*time.Second {
		d = 2 * time.Second
	}
	return d
}

// withKeyRotation runs build against pooled keys until one produces a response
// worth returning, or the retry budget is spent.
func withKeyRotation(ctx context.Context, cfg Config, pool *KeyPool, opts rotateOpts, tag string, build buildFunc) (*rotateResult, *rotateError) {
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, &rotateError{http.StatusServiceUnavailable, "client went away"}
		}

		idx, key, ok := pool.Next()
		if !ok {
			// Nothing will ever free up if every key has been disabled, so
			// don't burn the whole retry budget sleeping.
			if pool.ActiveCount() == 0 {
				return nil, &rotateError{http.StatusServiceUnavailable, "all keys are invalid or revoked"}
			}
			wait := pool.TimeUntilAvailable()
			if wait < 0 {
				wait = 0
			}
			log.Printf("⏳ %sNo key free — waiting %s (attempt %d/%d) | %s", tag, wait.Round(time.Second), attempt+1, cfg.MaxRetries, pool.Status())
			if !sleepCtx(ctx, wait+500*time.Millisecond) {
				return nil, &rotateError{http.StatusServiceUnavailable, "client went away"}
			}
			continue
		}

		attemptCtx, cancel := ctx, func() {}
		if opts.PerAttemptTimeout > 0 {
			attemptCtx, cancel = context.WithTimeout(ctx, opts.PerAttemptTimeout)
		}

		req, err := build(key)
		if err != nil {
			cancel()
			return nil, &rotateError{http.StatusInternalServerError, "failed to build upstream request"}
		}
		// Tying the upstream call to the caller's context means a client that
		// hangs up takes the upstream request down with it instead of leaving
		// it running against a key nobody is waiting on.
		req = req.WithContext(attemptCtx)

		// Count the request against the key here — at dispatch, once per
		// attempt. The upstream's rate limit counts what we send, not what
		// succeeds, so counting only the winning attempt (as the handlers used
		// to) undercounts every retried request and reports a stream minutes
		// after it started.
		pool.IncrementRequestCount(idx)

		resp, err := upstreamClient.Do(req)
		if err != nil {
			cancel()
			if ctx.Err() != nil {
				return nil, &rotateError{http.StatusServiceUnavailable, "client went away"}
			}
			log.Printf("⚠️ %sKey [%d] network error: %v", tag, idx, err)
			pool.Cooldown(idx, time.Duration(cfg.CooldownSec)*time.Second)
			continue
		}

		switch classify(resp.StatusCode) {
		case dispRetryCooldown:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			cooldown := cooldownFor(resp, cfg)
			log.Printf("🚫 %sKey [%d] %d — cooldown %s | %s", tag, idx, resp.StatusCode, cooldown, pool.Status())
			log.Printf("   body: %s", body)
			pool.Cooldown(idx, cooldown)
			cancel()
			continue

		case dispDisable:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("🔑 %sKey [%d] %d — disabled. body: %s", tag, idx, resp.StatusCode, body)
			pool.Disable(idx)
			cancel()
			if pool.ActiveCount() == 0 {
				return nil, &rotateError{http.StatusServiceUnavailable, "all keys are invalid or revoked"}
			}
			continue

		case dispRetryPlain:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			cancel()
			log.Printf("⚠️ %sUpstream %d: %s (retrying in %s)", tag, resp.StatusCode, body, backoffFor(attempt))
			if !sleepCtx(ctx, backoffFor(attempt)) {
				return nil, &rotateError{http.StatusServiceUnavailable, "client went away"}
			}
			continue
		}

		return &rotateResult{resp: resp, idx: idx, key: key, attempt: attempt + 1, release: cancel}, nil
	}

	return nil, &rotateError{http.StatusServiceUnavailable, "exhausted all retries"}
}
