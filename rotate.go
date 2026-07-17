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
	"io"
	"log"
	"net/http"
	"strconv"
	"time"
)

// buildFunc produces the upstream request for one attempt, with the pooled key
// already applied. It is called once per attempt because the body reader has
// to be fresh each time.
type buildFunc func(key string) (*http.Request, error)

// rotateResult is a response the caller now owns — it must close resp.Body.
type rotateResult struct {
	resp    *http.Response
	idx     int
	key     string
	attempt int
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

// withKeyRotation runs build against pooled keys until one produces a response
// worth returning, or the retry budget is spent.
func withKeyRotation(ctx context.Context, cfg Config, pool *KeyPool, client *http.Client, tag string, build buildFunc) (*rotateResult, *rotateError) {
	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		idx, key, ok := pool.Next()
		if !ok {
			wait := pool.TimeUntilAvailable()
			log.Printf("⏳ %sAll keys cooling — waiting %s (attempt %d/%d)", tag, wait.Round(time.Second), attempt+1, cfg.MaxRetries)
			time.Sleep(wait + 500*time.Millisecond)
			continue
		}

		req, err := build(key)
		if err != nil {
			return nil, &rotateError{http.StatusInternalServerError, "failed to build upstream request"}
		}

		resp, err := client.Do(req)
		if err != nil {
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
			continue

		case dispDisable:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("🔑 %sKey [%d] %d — disabled. body: %s", tag, idx, resp.StatusCode, body)
			pool.Disable(idx)
			if pool.ActiveCount() == 0 {
				return nil, &rotateError{http.StatusServiceUnavailable, "all keys are invalid or revoked"}
			}
			continue

		case dispRetryPlain:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("⚠️ %sUpstream %d: %s (Retrying...)", tag, resp.StatusCode, body)
			continue
		}

		return &rotateResult{resp: resp, idx: idx, key: key, attempt: attempt + 1}, nil
	}

	return nil, &rotateError{http.StatusServiceUnavailable, "exhausted all retries"}
}
