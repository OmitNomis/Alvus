package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func maskKey(key string) string {
	if len(key) <= 12 {
		return "****"
	}
	return key[:8] + "..." + key[len(key)-4:]
}

type LogEntry struct {
	Timestamp string `json:"timestamp"`
	Key string `json:"key"`
	KeyIndex int `json:"key_index"`
	Method string `json:"method"`
	URL string `json:"url"`
	Status int `json:"status"`
	RequestBodySize int `json:"request_body_size"`
}

var (
	usageLogs = []LogEntry{}
	usageMu   sync.Mutex
)

// ── Key Pool ──────────────────────────────────

type KeyPool struct {
	counter        uint64
	keys           []string
	cooldowns      []time.Time
	disabled       []bool
	requestHistory [][]time.Time // timestamps of requests in the last 60s per key
	lastUsed       []time.Time
	mu             sync.Mutex
}

func NewKeyPool(keys []string) *KeyPool {
	return &KeyPool{
		keys:           keys,
		cooldowns:      make([]time.Time, len(keys)),
		disabled:       make([]bool, len(keys)),
		requestHistory: make([][]time.Time, len(keys)),
		lastUsed:       make([]time.Time, len(keys)),
	}
}

func (p *KeyPool) TimeUntilAvailable() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var soonest time.Duration = -1
	for i, cd := range p.cooldowns {
		if p.disabled[i] {
			continue
		}
		if now.After(cd) {
			return 0
		}
		if wait := cd.Sub(now); soonest < 0 || wait < soonest {
			soonest = wait
		}
	}
	return soonest
}

func (p *KeyPool) Next() (int, string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := len(p.keys)
	start := int(atomic.AddUint64(&p.counter, 1)-1) % n
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if !p.disabled[idx] && time.Now().After(p.cooldowns[idx]) {
			return idx, p.keys[idx], true
		}
	}
	return -1, "", false
}

// requestsInLastMinute returns the number of requests made by a key in the last 60 seconds
func (p *KeyPool) requestsInLastMinute(idx int) int {
	cutoff := time.Now().Add(-60 * time.Second)
	count := 0
	for _, t := range p.requestHistory[idx] {
		if t.After(cutoff) {
			count++
		}
	}
	return count
}

// cleanupOldRequests removes request timestamps older than 60 seconds
func (p *KeyPool) cleanupOldRequests(idx int) {
	cutoff := time.Now().Add(-60 * time.Second)
	var filtered []time.Time
	for _, t := range p.requestHistory[idx] {
		if t.After(cutoff) {
			filtered = append(filtered, t)
		}
	}
	p.requestHistory[idx] = filtered
}

func (p *KeyPool) Cooldown(idx int, d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if until := time.Now().Add(d); p.cooldowns[idx].Before(until) {
		p.cooldowns[idx] = until
	}
	log.Printf("🧊 Key [%d] on cooldown for %s", idx, d)
}

func (p *KeyPool) Disable(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.disabled[idx] = true
}

func (p *KeyPool) ActiveCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for i := range p.keys {
		if !p.disabled[i] {
			n++
		}
	}
	return n
}

func (p *KeyPool) keyStatusLabel(i int, now time.Time) string {
	cd := p.cooldowns[i]
	switch {
	case p.disabled[i]:
		return "disabled"
	case now.After(cd):
		return "ready"
	default:
		return fmt.Sprintf("cooling(%.0fs)", cd.Sub(now).Seconds())
	}
}

func (p *KeyPool) Status() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	parts := make([]string, len(p.keys))
	for i := range p.keys {
		parts[i] = fmt.Sprintf("[%d]:%s", i, p.keyStatusLabel(i, now))
	}
	return strings.Join(parts, " ")
}

// GetKeyDetails returns detailed status for each key in the pool
func (p *KeyPool) GetKeyDetails() []map[string]interface{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	details := make([]map[string]interface{}, len(p.keys))
	for i := range p.keys {
		p.cleanupOldRequests(i)
		keyDetail := map[string]interface{}{
			"index": i,
			"key": maskKey(p.keys[i]),
			"disabled": p.disabled[i],
			"requests_per_minute": p.requestsInLastMinute(i),
			"last_used": p.lastUsed[i].Format(time.RFC3339),
			"cooldown_until": p.cooldowns[i].Format(time.RFC3339),
		}
		keyDetail["status"] = p.keyStatusLabel(i, now)
		details[i] = keyDetail
	}
	return details
}

// IncrementRequestCount records a request timestamp for a key and updates its lastUsed timestamp
func (p *KeyPool) IncrementRequestCount(idx int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleanupOldRequests(idx)
	p.requestHistory[idx] = append(p.requestHistory[idx], time.Now())
	p.lastUsed[idx] = time.Now()
}

// ── Config ────────────────────────────────────

type Config struct {
	TargetBase   string
	GenaiBase    string
	Port         string
	MaxRetries   int
	CooldownSec  int
	ProxyKey     string
}

func parseKeysFromEnv() ([]string, error) {
	raw := os.Getenv("API_KEYS")
	if raw == "" {
		return nil, fmt.Errorf("API_KEYS is required")
	}
	var keys []string
	for _, k := range strings.Split(raw, ",") {
		if k = strings.TrimSpace(k); k != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no valid API keys found in API_KEYS")
	}
	return keys, nil
}

func buildConfig() (Config, *KeyPool, error) {
	keys, err := parseKeysFromEnv()
	if err != nil {
		return Config{}, nil, err
	}
	cfg := Config{
		TargetBase:  strings.TrimRight(getenv("TARGET_BASE_URL", "https://integrate.api.nvidia.com/v1"), "/"),
		GenaiBase:   strings.TrimRight(getenv("GENAI_BASE_URL", "https://ai.api.nvidia.com"), "/"),
		Port:        getenv("PORT", "3000"),
		MaxRetries:  10,
		CooldownSec: 60,
		ProxyKey:    getenv("PROXY_KEY", ""),
	}
	return cfg, NewKeyPool(keys), nil
}

func loadConfig() (Config, *KeyPool) {
	cfg, pool, err := buildConfig()
	if err != nil {
		log.Fatalf("❌ %v", err)
	}
	return cfg, pool
}

func reloadConfig() (Config, *KeyPool, error) {
	for _, k := range []string{"API_KEYS", "TARGET_BASE_URL", "GENAI_BASE_URL", "PORT", "COOLDOWN_SEC", "PROXY_KEY"} {
		os.Unsetenv(k)
	}
	loadDotEnv(".env")
	return buildConfig()
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ── Server ────────────────────────────────────

type ServerState struct {
	mu   sync.RWMutex
	cfg  Config
	pool *KeyPool
	mux  *http.ServeMux
}

func newServerState(cfg Config, pool *KeyPool) *ServerState {
	s := &ServerState{cfg: cfg, pool: pool, mux: http.NewServeMux()}
	s.mux.HandleFunc("/health", s.healthHandler)
	s.mux.HandleFunc("/logs", s.logsHandler)
	s.mux.HandleFunc("/dashboard", s.dashboardHandler)
	s.mux.HandleFunc("/clear", s.clearHandler)
	s.mux.HandleFunc("/api/config", s.configHandler)
	// Block service worker requests to prevent 404s and unnecessary upstream proxying
	s.mux.HandleFunc("/sw.js", s.swHandler)
	s.mux.HandleFunc("/", s.proxyHandler)
	return s
}

func (s *ServerState) isAuthorized(r *http.Request) bool {
	s.mu.RLock()
	proxyKey := s.cfg.ProxyKey
	s.mu.RUnlock()

	if proxyKey == "" {
		return true
	}

	auth := r.Header.Get("Authorization")
	if auth == "" {
		return false
	}

	const prefix = "Bearer "
	if strings.HasPrefix(auth, prefix) {
		token := strings.TrimSpace(auth[len(prefix):])
		return token == proxyKey
	}
	return strings.TrimSpace(auth) == proxyKey
}

type ConfigPayload struct {
	TargetBase      string   `json:"targetBase"`
	GenaiBase       string   `json:"genaiBase"`
	Keys            []string `json:"keys"`
	CurrentProxyKey string   `json:"currentProxyKey"`
	NewProxyKey     string   `json:"newProxyKey"`
	ChangeProxyKey  bool     `json:"changeProxyKey"`
}

type ConfigResponse struct {
	TargetBase  string   `json:"targetBase"`
	GenaiBase   string   `json:"genaiBase"`
	Keys        []string `json:"keys"`
	HasProxyKey bool     `json:"hasProxyKey"`
}

func (s *ServerState) configHandler(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="Alvus"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.mu.RLock()
	cfg := s.cfg
	keys := s.pool.keys
	s.mu.RUnlock()

	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "application/json")
		maskedKeys := make([]string, len(keys))
		for i, k := range keys {
			maskedKeys[i] = maskKey(k)
		}
		json.NewEncoder(w).Encode(ConfigResponse{
			TargetBase:  cfg.TargetBase,
			GenaiBase:   cfg.GenaiBase,
			Keys:        maskedKeys,
			HasProxyKey: cfg.ProxyKey != "",
		})
		return
	}

	if r.Method == http.MethodPost {
		var payload ConfigPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		payload.TargetBase = strings.TrimSpace(payload.TargetBase)
		payload.GenaiBase = strings.TrimSpace(payload.GenaiBase)

		s.mu.RLock()
		currentKeys := s.pool.keys
		s.mu.RUnlock()

		nextProxyKey := cfg.ProxyKey
		if payload.ChangeProxyKey {
			if cfg.ProxyKey != "" && payload.CurrentProxyKey != cfg.ProxyKey {
				http.Error(w, "invalid current proxy key", http.StatusUnauthorized)
				return
			}
			nextProxyKey = strings.TrimSpace(payload.NewProxyKey)
		}

		reclaimed := make(map[int]bool)
		for i := range payload.Keys {
			k := strings.TrimSpace(payload.Keys[i])
			if k == "" {
				continue
			}
			// If the key is masked (contains "..." or is "****"), try to restore it from the current pool
			if strings.Contains(k, "...") || k == "****" {
				for j, ck := range currentKeys {
					if !reclaimed[j] && maskKey(ck) == k {
						k = ck
						reclaimed[j] = true
						break
					}
				}
			}
			payload.Keys[i] = k
		}
		payload.Keys = filterEmpty(payload.Keys)

		if payload.TargetBase == "" {
			http.Error(w, "targetBase is required", http.StatusBadRequest)
			return
		}
		if payload.GenaiBase == "" {
			http.Error(w, "genaiBase is required", http.StatusBadRequest)
			return
		}
		if len(payload.Keys) == 0 {
			http.Error(w, "at least one API key is required", http.StatusBadRequest)
			return
		}

		envLines := []string{
			fmt.Sprintf("TARGET_BASE_URL=%s", payload.TargetBase),
			fmt.Sprintf("GENAI_BASE_URL=%s", payload.GenaiBase),
			fmt.Sprintf("API_KEYS=%s", strings.Join(payload.Keys, ",")),
			fmt.Sprintf("PORT=%s", cfg.Port),
			fmt.Sprintf("COOLDOWN_SEC=%d", cfg.CooldownSec),
			fmt.Sprintf("PROXY_KEY=%s", nextProxyKey),
		}

		if err := os.WriteFile(".env", []byte(strings.Join(envLines, "\n")), 0600); err != nil {
			log.Printf("❌ Failed to write .env: %v", err)
			http.Error(w, "failed to save config", http.StatusInternalServerError)
			return
		}

		log.Printf("📝 Configuration updated via API")

		newCfg, newPool, err := reloadConfig()
		if err != nil {
			log.Printf("⚠️ Immediate reload failed: %v", err)
			w.WriteHeader(http.StatusAccepted)
			return
		}

		s.mu.Lock()
		s.cfg = newCfg
		s.pool = newPool
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "reloaded"})
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func filterEmpty(ss []string) []string {
	filtered := make([]string, 0, len(ss))
	for _, s := range ss {
		if s != "" {
			filtered = append(filtered, s)
		}
	}
	return filtered
}

func (s *ServerState) healthHandler(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="Alvus"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.mu.RLock()
	pool := s.pool
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")

	details := pool.GetKeyDetails()
	jsonDetails, err := json.Marshal(details)
	if err != nil {
		http.Error(w, "failed to marshal key details", http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, `{"status":"ok","keys":%d,"details":%s}`, len(pool.keys), jsonDetails)
}

func (s *ServerState) proxyHandler(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="Alvus"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	s.mu.RLock()
	cfg := s.cfg
	pool := s.pool
	s.mu.RUnlock()

	client := &http.Client{
		Timeout: 120 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	var bodyBytes []byte
	if r.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}
	}

	// Route /genai/ paths to GenaiBase, everything else to TargetBase
	var target string
	if strings.Contains(r.URL.Path, "/genai/") {
		target = cfg.GenaiBase + r.URL.Path
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
	} else {
		path := r.URL.Path
		if strings.HasSuffix(cfg.TargetBase, "/v1") && strings.HasPrefix(path, "/v1") {
			path = path[3:]
		}
		if r.URL.RawQuery != "" {
			path += "?" + r.URL.RawQuery
		}
		target = cfg.TargetBase + path
	}

	log.Printf("→ %s %s (%d bytes)", r.Method, target, len(bodyBytes))

	for attempt := 0; attempt < cfg.MaxRetries; attempt++ {
		idx, key, ok := pool.Next()
		if !ok {
			wait := pool.TimeUntilAvailable()
			log.Printf("⏳ All keys cooling — waiting %s (attempt %d/%d)", wait.Round(time.Second), attempt+1, cfg.MaxRetries)
			time.Sleep(wait + 500*time.Millisecond)
			continue
		}

		req, err := http.NewRequest(r.Method, target, bytes.NewReader(bodyBytes))
		if err != nil {
			http.Error(w, "proxy: failed to build upstream request", http.StatusInternalServerError)
			return
		}
		for k, vals := range r.Header {
			for _, v := range vals {
				req.Header.Add(k, v)
			}
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			log.Printf("⚠️ Key [%d] network error: %v", idx, err)
			pool.Cooldown(idx, time.Duration(cfg.CooldownSec)*time.Second)
			continue
		}

		switch resp.StatusCode {
		case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			cooldown := time.Duration(cfg.CooldownSec) * time.Second
			if ra := resp.Header.Get("Retry-After"); ra != "" {
				if secs, err := strconv.Atoi(ra); err == nil {
					cooldown = time.Duration(secs+2) * time.Second
				}
			}
			log.Printf("🚫 Key [%d] %d — cooldown %s | %s", idx, resp.StatusCode, cooldown, pool.Status())
			log.Printf("   body: %s", body)
			pool.Cooldown(idx, cooldown)
			continue

		case http.StatusUnauthorized, http.StatusForbidden:
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("🔑 Key [%d] %d — disabled. body: %s", idx, resp.StatusCode, body)
			pool.Disable(idx)
			if pool.ActiveCount() == 0 {
				http.Error(w, "alvus: all keys are invalid or revoked", http.StatusServiceUnavailable)
				return
			}
			continue
		}

		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			for k, vals := range resp.Header {
				for _, v := range vals {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			resp.Body.Close()

			pool.IncrementRequestCount(idx)
			usageMu.Lock()
			usageLogs = append(usageLogs, LogEntry{Timestamp: time.Now().Format(time.RFC3339), Key: key, KeyIndex: idx + 1, Method: r.Method, URL: target, Status: resp.StatusCode, RequestBodySize: len(bodyBytes)})
			if len(usageLogs) > 1000 {
				usageLogs = usageLogs[1:]
			}
			usageMu.Unlock()
			log.Printf("⚠️ %s %s → %d (Terminal Client Error, no retry)", r.Method, target, resp.StatusCode)
			return
		}

		if resp.StatusCode >= 500 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			log.Printf("⚠️ Upstream %d: %s (Retrying...)", resp.StatusCode, body)
			resp.Body = io.NopCloser(bytes.NewReader(body))
			continue
		}

		for k, vals := range resp.Header {
			for _, v := range vals {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)

		if f, ok := w.(http.Flusher); ok {
			buf := make([]byte, 4096)
			for {
				n, rerr := resp.Body.Read(buf)
				if n > 0 {
					w.Write(buf[:n])
					f.Flush()
				}
				if rerr != nil {
					break
				}
			}
		} else {
			io.Copy(w, resp.Body)
		}
		resp.Body.Close()

		pool.IncrementRequestCount(idx)
		usageMu.Lock()
		usageLogs = append(usageLogs, LogEntry{Timestamp: time.Now().Format(time.RFC3339), Key: key, KeyIndex: idx + 1, Method: r.Method, URL: target, Status: resp.StatusCode, RequestBodySize: len(bodyBytes)})
		if len(usageLogs) > 1000 {
			usageLogs = usageLogs[1:]
		}
		usageMu.Unlock()
		log.Printf("✅ %s %s → %d (key[%d], attempt %d)", r.Method, target, resp.StatusCode, idx, attempt+1)
		return
	}

	http.Error(w, "alvus: exhausted all retries", http.StatusServiceUnavailable)
}

func (s *ServerState) logsHandler(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthorized(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	usageMu.Lock()
	masked := make([]LogEntry, len(usageLogs))
	for i, entry := range usageLogs {
		masked[i] = LogEntry{
			Timestamp: entry.Timestamp,
			Key: maskKey(entry.Key),
			KeyIndex: entry.KeyIndex,
			Method: entry.Method,
			URL: entry.URL,
			Status: entry.Status,
			RequestBodySize: entry.RequestBodySize,
		}
	}
	data, _ := json.Marshal(masked)
	usageMu.Unlock()
	w.Write(data)
}

func (s *ServerState) dashboardHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(dashboardHTML))
}

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Alvus | Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #0a0a0c;
            --glass-bg: rgba(255, 255, 255, 0.03);
            --glass-border: rgba(255, 255, 255, 0.08);
            --accent-primary: #00d2ff;
            --accent-secondary: #3a7bd5;
            --text-main: #e0e0e0;
            --text-dim: #a0a0a0;
            --status-ready: #00ff88;
            --status-cooling: #ffcc00;
            --status-disabled: #ff4444;
            --card-blur: 12px;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
            font-family: 'Inter', sans-serif;
        }

        body {
            background: var(--bg-color);
            background-image: 
                radial-gradient(circle at 20% 20%, rgba(0, 210, 255, 0.05) 0%, transparent 40%),
                radial-gradient(circle at 80% 80%, rgba(58, 123, 213, 0.05) 0%, transparent 40%);
            color: var(--text-main);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            overflow-x: hidden;
        }

        /* --- Layout --- */
        .container {
            max-width: 1200px;
            width: 100%;
            margin: 0 auto;
            padding: 2rem;
            flex-grow: 1;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 3rem;
        }

        .logo {
            font-size: 1.5rem;
            font-weight: 700;
            letter-spacing: -0.5px;
            background: linear-gradient(to right, var(--accent-primary), var(--accent-secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        /* --- Navigation --- */
        nav {
            display: flex;
            gap: 1rem;
            background: var(--glass-bg);
            border: 1px solid var(--glass-border);
            padding: 0.5rem;
            border-radius: 12px;
            backdrop-filter: blur(var(--card-blur));
        }

        .nav-btn {
            background: transparent;
            border: none;
            color: var(--text-dim);
            padding: 0.6rem 1.2rem;
            border-radius: 8px;
            cursor: pointer;
            font-weight: 600;
            transition: all 0.2s;
        }

        .nav-btn.active {
            background: rgba(255, 255, 255, 0.1);
            color: white;
        }

        .nav-btn:hover:not(.active) {
            color: white;
            background: rgba(255, 255, 255, 0.05);
        }

        /* --- Content Panels --- */
        .panel {
            display: none;
            animation: fadeIn 0.3s ease-out;
        }

        .panel.active {
            display: block;
        }

        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }

        /* --- Cards & Components --- */
        .card {
            background: var(--glass-bg);
            border: 1px solid var(--glass-border);
            border-radius: 16px;
            padding: 1.5rem;
            backdrop-filter: blur(var(--card-blur));
            margin-bottom: 1.5rem;
        }

        h2 {
            font-size: 1.25rem;
            margin-bottom: 1.5rem;
            color: white;
        }

        /* --- Status Grid --- */
        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 1.5rem;
        }

        .key-card {
            position: relative;
            overflow: hidden;
        }

        .key-card::before {
            content: '';
            position: absolute;
            top: 0; left: 0; width: 4px; height: 100%;
        }

        .key-card.ready::before { background: var(--status-ready); }
        .key-card.cooling::before { background: var(--status-cooling); }
        .key-card.disabled::before { background: var(--status-disabled); }

        .key-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 0.5rem;
        }

        .key-val {
            font-family: monospace;
            color: var(--text-dim);
            font-size: 0.85rem;
            word-break: break-all;
        }

        .badge {
            font-size: 0.7rem;
            text-transform: uppercase;
            font-weight: 700;
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
        }

        .badge-ready { background: rgba(0, 255, 136, 0.1); color: var(--status-ready); }
        .badge-cooling { background: rgba(255, 204, 0, 0.1); color: var(--status-cooling); }
        .badge-disabled { background: rgba(255, 68, 68, 0.1); color: var(--status-disabled); }

        .stat-row {
            display: flex;
            justify-content: space-between;
            margin-top: 1rem;
            font-size: 0.85rem;
            color: var(--text-dim);
        }

        .stat-val { color: white; font-weight: 600; }

        /* --- Table --- */
        .table-container {
            width: 100%;
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }

        th {
            text-align: left;
            color: var(--text-dim);
            font-weight: 600;
            padding: 1rem;
            border-bottom: 1px solid var(--glass-border);
        }

        td {
            padding: 1rem;
            border-bottom: 1px solid var(--glass-border);
            color: var(--text-main);
        }

        tr:last-child td { border-bottom: none; }

        tr:hover td {
            background: rgba(255, 255, 255, 0.02);
        }

        .status-tag {
            display: inline-block;
            padding: 0.2rem 0.4rem;
            border-radius: 4px;
            font-weight: 700;
            font-size: 0.75rem;
        }

        .status-tag.ok { background: rgba(0, 255, 136, 0.1); color: var(--status-ready); }
        .status-tag.err { background: rgba(255, 68, 68, 0.1); color: var(--status-disabled); }

        /* --- Forms --- */
        .form-group {
            margin-bottom: 1.5rem;
        }

        label {
            display: block;
            margin-bottom: 0.5rem;
            font-size: 0.9rem;
            color: var(--text-dim);
        }

        input[type="text"], input[type="number"], textarea {
            width: 100%;
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--glass-border);
            border-radius: 8px;
            padding: 0.75rem;
            color: white;
            font-size: 0.95rem;
            outline: none;
            transition: border-color 0.2s;
        }

        input:focus {
            border-color: var(--accent-primary);
        }

        .btn {
            background: linear-gradient(to right, var(--accent-primary), var(--accent-secondary));
            color: white;
            border: none;
            padding: 0.75rem 1.5rem;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.1s, opacity 0.2s;
        }

        .btn:hover { opacity: 0.9; }
        .btn:active { transform: scale(0.98); }

        .btn-outline {
            background: transparent;
            border: 1px solid var(--glass-border);
            color: var(--text-main);
        }

        .btn-outline:hover { background: rgba(255, 255, 255, 0.05); }

        .key-list-item {
            display: flex;
            gap: 10px;
            margin-bottom: 10px;
        }

        .remove-key-btn {
            background: rgba(255, 68, 68, 0.1);
            color: var(--status-disabled);
            border: 1px solid rgba(255, 68, 68, 0.2);
            padding: 0.5rem;
            border-radius: 8px;
            cursor: pointer;
        }

        /* --- Search & Filters --- */
        .toolbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.5rem;
            gap: 1rem;
        }

        .search-input {
            max-width: 300px;
        }

        footer {
            padding: 2rem;
            text-align: center;
            font-size: 0.8rem;
            color: var(--text-dim);
            border-top: 1px solid var(--glass-border);
            margin-top: 3rem;
        }

        /* --- Login Overlay --- */
        .login-overlay {
            position: fixed;
            top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(10, 10, 12, 0.85);
            backdrop-filter: blur(16px);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 1000;
            opacity: 0;
            pointer-events: none;
            transition: opacity 0.3s ease;
        }

        .login-overlay.active {
            opacity: 1;
            pointer-events: auto;
        }

        .login-card {
            background: var(--glass-bg);
            border: 1px solid var(--glass-border);
            border-radius: 20px;
            padding: 2.5rem;
            width: 100%;
            max-width: 420px;
            box-shadow: 0 20px 50px rgba(0, 0, 0, 0.3);
            text-align: center;
            backdrop-filter: blur(var(--card-blur));
        }

        .login-title {
            font-size: 1.3rem;
            font-weight: 700;
            margin-bottom: 0.5rem;
            background: linear-gradient(to right, var(--accent-primary), var(--accent-secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            letter-spacing: 0.5px;
        }

        .login-subtitle {
            font-size: 0.85rem;
            color: var(--text-dim);
            margin-bottom: 2rem;
        }

        .login-hint {
            margin-top: 1.5rem;
            font-size: 0.8rem;
            color: var(--text-dim);
            line-height: 1.4;
        }

        .login-hint code {
            background: rgba(255, 255, 255, 0.08);
            padding: 0.1rem 0.3rem;
            border-radius: 4px;
            font-family: monospace;
            color: var(--accent-primary);
        }

        .login-error {
            color: var(--status-disabled);
            font-size: 0.85rem;
            margin-top: 0.75rem;
            display: none;
        }

        /* Responsive */
        @media (max-width: 768px) {
            header { flex-direction: column; gap: 1.5rem; align-items: flex-start; }
            .toolbar { flex-direction: column; align-items: stretch; }
            .search-input { max-width: none; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="logo">ALVUS DASHBOARD</div>
            <nav>
                <button class="nav-btn active" data-panel="overview">Overview</button>
                <button class="nav-btn" data-panel="logs">Logs</button>
                <button class="nav-btn" data-panel="settings">Settings</button>
            </nav>
        </header>

        <!-- Overview Panel -->
        <main id="overview" class="panel active">
            <div class="status-grid" id="statusGrid">
                <!-- Key cards will be injected here -->
            </div>
        </main>

        <!-- Logs Panel -->
        <main id="logs" class="panel">
            <div class="card">
                <div class="toolbar">
                    <h2>Recent Activity</h2>
                    <div style="display: flex; gap: 0.5rem;">
                        <input type="text" id="logSearch" class="search-input" placeholder="Search logs...">
                        <button id="clearLogsBtn" class="btn btn-outline">Clear</button>
                    </div>
                </div>
                <div class="table-container">
                    <table id="logTable">
                        <thead>
                            <tr>
                                <th>Timestamp</th>
                                <th>Method</th>
                                <th>Endpoint</th>
                                <th>Status</th>
                                <th>Key</th>
                                <th>Size</th>
                            </tr>
                        </thead>
                        <tbody>
                            <!-- Logs will be injected here -->
                        </tbody>
                    </table>
                </div>
            </div>
        </main>

        <!-- Settings Panel -->
        <main id="settings" class="panel">
            <div class="card">
                <h2>System Configuration</h2>
                <form id="configForm">
                    <div class="form-group">
                        <label for="targetBase">Target Base URL (NVIDIA API)</label>
                        <input type="text" id="targetBase" name="targetBase" placeholder="https://integrate.api.nvidia.com/v1">
                    </div>
                    <div class="form-group">
                        <label for="genaiBase">GenAI Base URL</label>
                        <input type="text" id="genaiBase" name="genaiBase" placeholder="https://ai.api.nvidia.com">
                    </div>
                    
                    <div class="form-group">
                        <label>API Keys Pool</label>
                        <div id="keyInputsContainer">
                            <!-- Dynamic key inputs will be injected here -->
                        </div>
                        <button type="button" id="addKeyBtn" class="btn btn-outline" style="margin-top: 10px; width: 100%;">+ Add Another Key</button>
                    </div>

                    <div style="margin-top: 2rem; border-top: 1px solid var(--glass-border); padding-top: 1.5rem;">
                        <h3 style="font-size: 1.1rem; color: white; margin-bottom: 1.2rem;">Authorization Settings</h3>
                        <div class="form-group" style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 1.5rem;">
                            <input type="checkbox" id="changeProxyKey" name="changeProxyKey" style="width: auto; margin-right: 0.25rem; transform: scale(1.1); cursor: pointer;">
                            <label for="changeProxyKey" style="display: inline; margin-bottom: 0; cursor: pointer; user-select: none;">Change Proxy Access Key</label>
                        </div>
                        <div id="proxyKeyChangeSection" style="display: none; animation: fadeIn 0.2s ease-out;">
                            <div class="form-group" id="currentProxyKeyGroup">
                                <label for="currentProxyKey">Current Proxy Key</label>
                                <input type="password" id="currentProxyKey" name="currentProxyKey" placeholder="Leave empty if none was set" style="width: 100%; background: rgba(255, 255, 255, 0.05); border: 1px solid var(--glass-border); border-radius: 8px; padding: 0.75rem; color: white; font-size: 0.95rem; outline: none; transition: border-color 0.2s;">
                            </div>
                            <div class="form-group">
                                <label for="newProxyKey">New Proxy Key</label>
                                <input type="password" id="newProxyKey" name="newProxyKey" placeholder="Enter new proxy key (leave empty to disable authorization)" style="width: 100%; background: rgba(255, 255, 255, 0.05); border: 1px solid var(--glass-border); border-radius: 8px; padding: 0.75rem; color: white; font-size: 0.95rem; outline: none; transition: border-color 0.2s;">
                            </div>
                        </div>
                    </div>

                    <div style="margin-top: 2rem;">
                        <button type="submit" class="btn" style="width: 100%;">Save & Apply Configuration</button>
                    </div>
                </form>
            </div>
        </main>
    </div>

    <!-- Login Overlay -->
    <div id="loginOverlay" class="login-overlay">
        <div class="login-card">
            <div class="login-title">ALVUS AUTHENTICATION</div>
            <div class="login-subtitle">Enter your proxy key to access the dashboard</div>
            <form id="loginForm">
                <div class="form-group" style="text-align: left; margin-bottom: 1.5rem;">
                    <label for="loginKey">Proxy Key</label>
                    <input type="password" id="loginKey" placeholder="Enter proxy key..." required style="width: 100%; background: rgba(255, 255, 255, 0.05); border: 1px solid var(--glass-border); border-radius: 8px; padding: 0.75rem; color: white; font-size: 0.95rem; outline: none; transition: border-color 0.2s;">
                    <div id="loginError" class="login-error">Invalid proxy key. Please try again.</div>
                </div>
                <button type="submit" class="btn" style="width: 100%;">Authenticate</button>
            </form>
            <div class="login-hint">
                💡 Forgot your key? Look for <code>PROXY_KEY</code> in your <code>.env</code> file.
            </div>
        </div>
    </div>

    <footer>
        &copy; 2026 Alvus Multi-Key Proxy
    </footer>

    <script>
        // --- Auth Wrapper ---
        function fetchWithAuth(url, options) {
            options = options || {};
            options.headers = options.headers || {};
            const key = localStorage.getItem('alvus_proxy_key');
            if (key) {
                options.headers['Authorization'] = 'Bearer ' + key;
            }
            return fetch(url, options).then(res => {
                if (res.status === 401) {
                    showLoginOverlay();
                    throw new Error('Unauthorized');
                }
                return res;
            });
        }

        function showLoginOverlay() {
            document.getElementById('loginOverlay').classList.add('active');
            const existingKey = localStorage.getItem('alvus_proxy_key');
            if (existingKey) {
                document.getElementById('loginError').style.display = 'block';
            } else {
                document.getElementById('loginError').style.display = 'none';
            }
        }

        function hideLoginOverlay() {
            document.getElementById('loginOverlay').classList.remove('active');
        }

        document.getElementById('loginForm').addEventListener('submit', function(e) {
            e.preventDefault();
            const key = document.getElementById('loginKey').value;
            localStorage.setItem('alvus_proxy_key', key);
            hideLoginOverlay();
            
            // Reload whatever active view is current
            if (document.getElementById('overview').classList.contains('active')) {
                updateStatus();
            } else if (document.getElementById('logs').classList.contains('active')) {
                updateLogs();
            } else if (document.getElementById('settings').classList.contains('active')) {
                loadConfig();
            }
        });

        // --- Navigation ---
        const navBtns = document.querySelectorAll('.nav-btn');
        const panels = document.querySelectorAll('.panel');

        navBtns.forEach(btn => {
            btn.addEventListener('click', () => {
                const panelId = btn.getAttribute('data-panel');
                
                navBtns.forEach(b => b.classList.remove('active'));
                panels.forEach(p => p.classList.remove('active'));
                
                btn.classList.add('active');
                document.getElementById(panelId).classList.add('active');

                if(panelId === 'settings') loadConfig();
            });
        });

        // --- Data Fetching ---
        function updateStatus() {
            if (!document.getElementById('overview').classList.contains('active')) return;

            fetchWithAuth('/health')
                .then(res => res.json())
                .then(data => {
                    const grid = document.getElementById('statusGrid');
                    grid.innerHTML = '';
                    
                    data.details.forEach(key => {
                        const card = document.createElement('div');
                        card.className = "card key-card " + (key.status.startsWith('cooling') ? 'cooling' : key.status);
                        
                        const statusClass = key.status === 'ready' ? 'ready' : (key.status === 'disabled' ? 'disabled' : 'cooling');
                        
                        card.innerHTML = '<div class="key-header"><span class="badge badge-' + statusClass + '">' + key.status + '</span><span style="font-size: 0.75rem; color: var(--text-dim)">#' + (key.index + 1) + '</span></div><div class="key-val">' + key.key + '</div><div class="stat-row"><span>RPM</span><span class="stat-val">' + key.requests_per_minute + '</span></div><div class="stat-row"><span>Cooldown Until</span><span class="stat-val">' + (key.cooldown_until !== '0001-01-01T00:00:00Z' ? new Date(key.cooldown_until).toLocaleTimeString() : 'N/A') + '</span></div><div class="stat-row"><span>Last Used</span><span class="stat-val">' + (key.last_used !== '0001-01-01T00:00:00Z' ? new Date(key.last_used).toLocaleTimeString() : 'Never') + '</span></div>';
                        grid.appendChild(card);
                    });
                }).catch(err => {});
        }

        function updateLogs() {
            if (!document.getElementById('logs').classList.contains('active')) return;

            fetchWithAuth('/logs')
                .then(res => res.json())
                .then(data => {
                    const tbody = document.querySelector('#logTable tbody');
                    const search = document.getElementById('logSearch').value.toLowerCase();
                    tbody.innerHTML = '';
                    
                    data.slice().reverse().forEach(log => {
                        if (search && !JSON.stringify(log).toLowerCase().includes(search)) return;

                        const tr = document.createElement('tr');
                        const statusClass = log.status < 400 ? 'ok' : 'err';
                        
                        tr.innerHTML = '<td style="color: var(--text-dim); font-size: 0.8rem;">' + new Date(log.timestamp).toLocaleTimeString() + '</td><td><span style="font-weight: 700; color: var(--accent-primary)">' + log.method + '</span></td><td style="max-width: 250px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">' + log.url + '</td><td><span class="status-tag ' + statusClass + '">' + log.status + '</span></td><td class="key-val">Key #' + log.key_index + '</td><td style="color: var(--text-dim)">' + (log.request_body_size / 1024).toFixed(1) + ' KB</td>';
                        tbody.appendChild(tr);
                    });
                }).catch(err => {});
        }

        // --- Settings Management ---
        document.getElementById('changeProxyKey').addEventListener('change', function(e) {
            document.getElementById('proxyKeyChangeSection').style.display = e.target.checked ? 'block' : 'none';
        });

        function addKeyInput(val) {
            val = val || '';
            const container = document.getElementById('keyInputsContainer');
            const div = document.createElement('div');
            div.className = 'key-list-item';
            div.innerHTML = '<input type="text" name="apiKeys[]" value="' + val + '" placeholder="nvapi-..." required><button type="button" class="remove-key-btn">&times;</button>';
            container.appendChild(div);

            div.querySelector('.remove-key-btn').addEventListener('click', function() {
                if (container.children.length > 1) {
                    div.remove();
                } else {
                    div.querySelector('input').value = '';
                }
            });
        }

        function loadConfig() {
            fetchWithAuth('/api/config')
                .then(function(res) { return res.json(); })
                .then(function(data) {
                    document.getElementById('targetBase').value = data.targetBase;
                    document.getElementById('genaiBase').value = data.genaiBase;
                    
                    if (data.hasProxyKey) {
                        document.getElementById('currentProxyKeyGroup').style.display = 'block';
                    } else {
                        document.getElementById('currentProxyKeyGroup').style.display = 'none';
                    }
                    
                    document.getElementById('changeProxyKey').checked = false;
                    document.getElementById('proxyKeyChangeSection').style.display = 'none';
                    document.getElementById('currentProxyKey').value = '';
                    document.getElementById('newProxyKey').value = '';

                    const container = document.getElementById('keyInputsContainer');
                    container.innerHTML = '';
                    if (data.keys && data.keys.length > 0) {
                        data.keys.forEach(function(k) { addKeyInput(k); });
                    } else {
                        addKeyInput();
                    }
                }).catch(err => {});
        }

        document.getElementById('addKeyBtn').addEventListener('click', function() { addKeyInput(); });

        document.getElementById('configForm').addEventListener('submit', function(e) {
            e.preventDefault();
            const formData = new FormData(e.target);
            const keys = formData.getAll('apiKeys[]');
            const changeKey = document.getElementById('changeProxyKey').checked;
            const newKey = document.getElementById('newProxyKey').value;
            const config = {
                targetBase: formData.get('targetBase'),
                genaiBase: formData.get('genaiBase'),
                keys: keys.filter(function(k) { return k.trim() !== ''; }),
                changeProxyKey: changeKey,
                currentProxyKey: document.getElementById('currentProxyKey').value,
                newProxyKey: newKey
            };

            fetchWithAuth('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            }).then(function(res) {
                if(res.ok) {
                    alert('Configuration saved! The server will reload automatically in a few seconds.');
                    if (changeKey) {
                        if (newKey.trim() === '') {
                            localStorage.removeItem('alvus_proxy_key');
                        } else {
                            localStorage.setItem('alvus_proxy_key', newKey);
                        }
                    }
                    setTimeout(loadConfig, 1000);
                }
                else {
                    res.text().then(function(text) {
                        alert('Failed to save configuration: ' + text);
                    });
                }
            }).catch(err => {});
        });

        document.getElementById('clearLogsBtn').addEventListener('click', function() {
            fetchWithAuth('/clear', { method: 'POST' }).then(function() { updateLogs(); }).catch(err => {});
        });

        // Polling
        setInterval(updateStatus, 5000);
        setInterval(updateLogs, 5000);
        updateStatus();
        updateLogs();

    </script>
</body>
</html>
`

func (s *ServerState) clearHandler(w http.ResponseWriter, r *http.Request) {
	if !s.isAuthorized(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	usageMu.Lock()
	usageLogs = []LogEntry{}
	usageMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "cleared"})
}

// ── .env Watcher ──────────────────────────────

func watchEnvFile(state *ServerState, stop <-chan struct{}) {
	var lastMod time.Time
	if info, err := os.Stat(".env"); err == nil {
		lastMod = info.ModTime()
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			info, err := os.Stat(".env")
			if err != nil {
				if os.IsNotExist(err) {
					log.Printf("⚠️ .env file deleted — keeping current config")
				}
				continue
			}
			if !info.ModTime().After(lastMod) {
				continue
			}
			lastMod = info.ModTime()
			time.Sleep(100 * time.Millisecond) // debounce

			log.Printf("🔄 .env changed — reloading...")
			newCfg, newPool, err := reloadConfig()
			if err != nil {
				log.Printf("❌ Reload failed: %v", err)
				continue
			}
			state.mu.Lock()
			state.cfg = newCfg
			state.pool = newPool
			state.mu.Unlock()
			log.Printf("✅ Reloaded — %d keys, target: %s, genai: %s", len(newPool.keys), newCfg.TargetBase, newCfg.GenaiBase)
		}
	}
}

// ── Main ──────────────────────────────────────

func main() {
	isLocal := flag.Bool("local", false, "Bind to 127.0.0.1 (local access only)")
	isNetwork := flag.Bool("network-only", false, "Bind to 0.0.0.0 (accessible via LAN)")
	flag.Parse()

	host := "" // Default (binds to all interfaces)
	if *isLocal {
		host = "127.0.0.1"
	} else if *isNetwork {
		host = "0.0.0.0"
	}

	loadDotEnv(".env")
	cfg, pool := loadConfig()
	state := newServerState(cfg, pool)

	stop := make(chan struct{})
	go watchEnvFile(state, stop)

	// Graceful shutdown on SIGINT/SIGTERM
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	server := &http.Server{Addr: host + ":" + cfg.Port, Handler: state.mux}

	go func() {
		<-sigCh
		log.Printf("🛑 Shutting down gracefully...")
		close(stop)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("❌ Shutdown error: %v", err)
		}
	}()

	displayHost := host
	if displayHost == "" {
		displayHost = "0.0.0.0"
	}
	log.Printf("⚡ Alvus %s:%s → %s | genai → %s (%d keys)", displayHost, cfg.Port, cfg.TargetBase, cfg.GenaiBase, len(pool.keys))
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("❌ Server error: %v", err)
	}
}

// ── .env Loader ───────────────────────────────

func loadDotEnv(filename string) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k, v := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}