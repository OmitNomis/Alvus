package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// grpcStub is a fake Riva TTS function: HTTP/2 over TLS like grpc.nvcf.nvidia.com,
// answering each call with the next scripted grpc-status ("0" = success).
type grpcStub struct {
	*httptest.Server
	mu       sync.Mutex
	statuses []string
	keys     []string
	requests [][]byte // the protobuf message of each call
	headers  []http.Header
}

var stubAudio = []byte{1, 0, 2, 0, 3, 0, 4, 0}

func newGRPCStub(t *testing.T, statuses ...string) *grpcStub {
	t.Helper()
	g := &grpcStub{statuses: statuses}
	g.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		g.mu.Lock()
		status := "0"
		if len(g.statuses) > 0 {
			status, g.statuses = g.statuses[0], g.statuses[1:]
		}
		g.keys = append(g.keys, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
		g.headers = append(g.headers, r.Header.Clone())
		if len(body) >= 5 {
			g.requests = append(g.requests, body[5:])
		}
		g.mu.Unlock()

		w.Header().Set("Content-Type", "application/grpc")
		if status != "0" {
			// Trailers-only response, which is how real gRPC servers fail fast.
			w.Header().Set("Grpc-Status", status)
			w.Header().Set("Grpc-Message", "stub%20failure")
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Trailer", "Grpc-Status")
		w.WriteHeader(http.StatusOK)
		w.Write(grpcFrame(pbBytes(nil, 1, stubAudio)))
		w.Header().Set("Grpc-Status", "0")
	}))
	g.EnableHTTP2 = true
	g.StartTLS()

	old := ttsClient
	ttsClient = &http.Client{Transport: grpcStatusTransport{base: g.Client().Transport}}
	t.Cleanup(func() { ttsClient = old; g.Close() })
	return g
}

func ttsState(g *grpcStub, keys ...string) *ServerState {
	cfg := Config{MaxRetries: 3, CooldownSec: 60, MaxBodyMB: 1, TTSBase: g.URL}
	return &ServerState{cfg: cfg, pool: NewKeyPool(keys, 0), auth: &AdminAuth{}}
}

func callTTS(s *ServerState, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	s.ttsHandler(rec, httptest.NewRequest(http.MethodPost, "/v1/audio/speech", strings.NewReader(body)))
	return rec
}

// pbFieldsOf flattens one level of a protobuf message for assertions.
func pbFieldsOf(t *testing.T, msg []byte) (strs map[int][]byte, nums map[int]uint64) {
	t.Helper()
	strs, nums = map[int][]byte{}, map[int]uint64{}
	if err := pbWalk(msg, func(field, wire int, val []byte, n uint64) {
		if wire == 2 {
			strs[field] = val
		} else {
			nums[field] = n
		}
	}); err != nil {
		t.Fatalf("pbWalk: %v", err)
	}
	return strs, nums
}

func TestTTSReturnsWAVFromGRPC(t *testing.T) {
	g := newGRPCStub(t, "0")
	s := ttsState(g, "key-a")

	rec := callTTS(s, `{"model":"nvidia/magpie-tts-multilingual","input":"おはよう","voice":"Magpie-Multilingual.JA-JP.Isabela","language":"ja-JP"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "audio/wav" {
		t.Errorf("content-type = %q", ct)
	}
	out := rec.Body.Bytes()
	if len(out) != 44+len(stubAudio) || string(out[:4]) != "RIFF" || !bytes.Equal(out[44:], stubAudio) {
		t.Fatalf("not a WAV around the stub audio: % x", out)
	}
	if rate := binary.LittleEndian.Uint32(out[24:]); rate != 22050 {
		t.Errorf("default sample rate = %d", rate)
	}

	h := g.headers[0]
	if h.Get("function-id") != ttsFunctions["magpie-tts-multilingual"] {
		t.Errorf("function-id = %q", h.Get("function-id"))
	}
	if h.Get("Content-Type") != "application/grpc" || g.keys[0] != "key-a" {
		t.Errorf("headers = %v, key %q", h, g.keys[0])
	}
	strs, nums := pbFieldsOf(t, g.requests[0])
	if string(strs[1]) != "おはよう" || string(strs[2]) != "ja-JP" || string(strs[5]) != "Magpie-Multilingual.JA-JP.Isabela" {
		t.Errorf("request fields: text %q, lang %q, voice %q", strs[1], strs[2], strs[5])
	}
	if nums[3] != 1 || nums[4] != 22050 {
		t.Errorf("encoding/sample rate = %v", nums)
	}
	if _, ok := strs[6]; ok {
		t.Error("zero_shot_data sent without an audio prompt")
	}
}

func TestTTSSendsZeroShotPrompt(t *testing.T) {
	g := newGRPCStub(t, "0")
	s := ttsState(g, "key-a")
	clip := []byte("RIFF....fake wav")

	body := `{"model":"magpie-tts-zeroshot","input":"hi","language":"ja-JP","sample_rate":24000,"quality":40,` +
		`"audio_prompt":"` + base64.StdEncoding.EncodeToString(clip) + `","audio_prompt_transcript":"こんにちは"}`
	if rec := callTTS(s, body); rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	strs, nums := pbFieldsOf(t, g.requests[0])
	if nums[4] != 24000 {
		t.Errorf("sample rate = %d", nums[4])
	}
	zs, zn := pbFieldsOf(t, strs[6])
	if !bytes.Equal(zs[1], clip) || string(zs[5]) != "こんにちは" || zn[4] != 40 {
		t.Errorf("zero_shot_data: prompt %q, transcript %q, quality %d", zs[1], zs[5], zn[4])
	}
}

func TestTTSRotatesPastResourceExhausted(t *testing.T) {
	// RESOURCE_EXHAUSTED is gRPC's 429: cool that key down and try the next.
	g := newGRPCStub(t, "8", "0")
	s := ttsState(g, "key-a", "key-b")

	rec := callTTS(s, `{"model":"magpie-tts-multilingual","input":"hi"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if len(g.keys) != 2 || g.keys[0] == g.keys[1] {
		t.Errorf("keys tried = %v, want two different keys", g.keys)
	}
}

func TestTTSRejectedArgumentIsTerminal(t *testing.T) {
	// INVALID_ARGUMENT (e.g. a prompt sent to a model that can't clone) is the
	// caller's mistake: no retry, and the upstream's message comes back.
	g := newGRPCStub(t, "3", "0")
	s := ttsState(g, "key-a", "key-b")

	rec := callTTS(s, `{"model":"chatterbox-multilingual-tts","input":"hi"}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "stub failure") {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	if len(g.keys) != 1 {
		t.Errorf("attempts = %d, want 1", len(g.keys))
	}
}

func TestTTSRejectsBadRequestsLocally(t *testing.T) {
	g := newGRPCStub(t)
	s := ttsState(g, "key-a")
	for _, body := range []string{
		`{"model":"magpie-tts-multilingual","input":"  "}`,
		`{"model":"no-such-model","input":"hi"}`,
		`{"model":"magpie-tts-zeroshot","input":"hi","audio_prompt":"%%%"}`,
		`not json`,
	} {
		if rec := callTTS(s, body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s → %d, want 400", body, rec.Code)
		}
	}
	if len(g.keys) != 0 {
		t.Errorf("bad requests reached the upstream %d times", len(g.keys))
	}
}

func TestGRPCToHTTPMapping(t *testing.T) {
	for code, want := range map[string]int{
		"0": 200, "3": 400, "4": 504, "7": 403, "8": 429, "14": 503, "16": 401, "13": 500,
	} {
		if got := grpcToHTTP(code); got != want {
			t.Errorf("grpc %s → %d, want %d", code, got, want)
		}
	}
}

func TestParseSynthesizeResponseRejectsGarbage(t *testing.T) {
	for _, body := range [][]byte{nil, {0, 0, 0, 0}, {1, 0, 0, 0, 0}, {0, 0, 0, 0, 9, 1}, grpcFrame(pbUint(nil, 2, 5))} {
		if _, err := parseSynthesizeResponse(body); err == nil {
			t.Errorf("accepted % x", body)
		}
	}
}
