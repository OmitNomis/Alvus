package main

// Text-to-speech through NVIDIA's hosted speech models.
//
// NVIDIA serves Magpie TTS and Chatterbox only over gRPC (grpc.nvcf.nvidia.com),
// so the OpenAI-style pass-through can't reach them. /v1/audio/speech takes a
// small JSON body, makes one unary gRPC call and answers with a WAV file.
//
// A unary gRPC call is just an HTTP/2 POST whose body is one length-prefixed
// protobuf message, with the outcome in a grpc-status trailer. The handful of
// protobuf fields involved are encoded by hand, so the zero-dependency,
// pure-stdlib promise holds here too.

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const defaultTTSBase = "https://grpc.nvcf.nvidia.com"

// synthesizePath is the gRPC method Riva-compatible TTS functions expose.
const synthesizePath = "/nvidia.riva.tts.RivaSpeechSynthesis/Synthesize"

// ttsFunctions maps the model names used on build.nvidia.com to their NVCF
// function IDs. A request can also name a function_id directly.
var ttsFunctions = map[string]string{
	"magpie-tts-multilingual":     "877104f7-e885-42b9-8de8-f6e4c6303969",
	"magpie-tts-zeroshot":         "55cf67bf-600f-4b04-8eac-12ed39537a08",
	"chatterbox-multilingual-tts": "ddacc747-1269-4fab-bfd9-8f593dead106",
}

// ttsClient is separate from upstreamClient because gRPC needs HTTP/2, and a
// custom http.Transport only negotiates it when told to. The status wrapper
// lets the shared rotation loop treat gRPC failures like HTTP ones.
var ttsClient = &http.Client{
	Transport: grpcStatusTransport{base: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          16,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: responseHeaderTimeout,
	}},
}

type ttsRequest struct {
	Model          string `json:"model"`
	FunctionID     string `json:"function_id"`
	Input          string `json:"input"`
	Voice          string `json:"voice"`
	Language       string `json:"language"`
	SampleRate     int    `json:"sample_rate"`
	ResponseFormat string `json:"response_format"` // "wav" (default) or "pcm"
	// Zero-shot models clone the voice in a short reference clip: a whole WAV
	// file, base64-encoded. The transcript of that clip is optional but helps.
	AudioPrompt           string `json:"audio_prompt"`
	AudioPromptTranscript string `json:"audio_prompt_transcript"`
	Quality               int    `json:"quality"`
}

func (s *ServerState) ttsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	s.mu.RLock()
	cfg := s.cfg
	pool := s.pool
	s.mu.RUnlock()

	body, err := readLimitedBody(w, r, cfg.maxBodyBytes())
	if err != nil {
		if bodyTooLarge(err) {
			http.Error(w, fmt.Sprintf("alvus: request body over the %d MB limit", cfg.MaxBodyMB), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	var req ttsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "alvus: invalid JSON body", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Input) == "" {
		http.Error(w, "alvus: input is required", http.StatusBadRequest)
		return
	}
	fid := req.FunctionID
	if fid == "" {
		// Accept "nvidia/magpie-tts-zeroshot" as well as the bare name.
		fid = ttsFunctions[req.Model[strings.LastIndex(req.Model, "/")+1:]]
	}
	if fid == "" {
		http.Error(w, "alvus: unknown TTS model "+strconv.Quote(req.Model)+" (known: magpie-tts-multilingual, magpie-tts-zeroshot, chatterbox-multilingual-tts, or pass function_id)", http.StatusBadRequest)
		return
	}
	if req.Language == "" {
		req.Language = "en-US"
	}
	if req.SampleRate <= 0 {
		req.SampleRate = 22050
	}
	var prompt []byte
	if req.AudioPrompt != "" {
		if prompt, err = base64.StdEncoding.DecodeString(req.AudioPrompt); err != nil {
			http.Error(w, "alvus: audio_prompt is not valid base64", http.StatusBadRequest)
			return
		}
	}
	frame := grpcFrame(encodeSynthesizeRequest(req, prompt))
	target := cfg.TTSBase + synthesizePath

	log.Printf("→ TTS %s %q (%d chars, prompt %d bytes)", req.Model, req.Language, len([]rune(req.Input)), len(prompt))

	opts := rotateOpts{PerAttemptTimeout: nonStreamTimeout, Client: ttsClient}
	out, rerr := withKeyRotation(r.Context(), cfg, pool, opts, "[tts] ", func(key string) (*http.Request, error) {
		up, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(frame))
		if err != nil {
			return nil, err
		}
		up.Header.Set("Content-Type", "application/grpc")
		up.Header.Set("TE", "trailers")
		up.Header.Set("function-id", fid)
		up.Header.Set("Authorization", "Bearer "+key)
		return up, nil
	})
	if rerr != nil {
		http.Error(w, "alvus: "+rerr.msg, rerr.status)
		return
	}
	defer out.release()
	defer out.resp.Body.Close()
	resp := out.resp

	if resp.StatusCode != http.StatusOK {
		logUsage(out.key, out.idx, r.Method, target, resp.StatusCode, len(body))
		http.Error(w, "alvus: tts upstream: "+grpcMessage(resp.Header), resp.StatusCode)
		return
	}
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "alvus: tts upstream read failed", http.StatusBadGateway)
		return
	}
	// A failure can also arrive after the headers, in the trailers.
	if code := resp.Trailer.Get("Grpc-Status"); code != "" && code != "0" {
		logUsage(out.key, out.idx, r.Method, target, grpcToHTTP(code), len(body))
		http.Error(w, "alvus: tts upstream: "+grpcMessage(resp.Trailer), grpcToHTTP(code))
		return
	}
	audio, err := parseSynthesizeResponse(payload)
	if err != nil {
		http.Error(w, "alvus: tts upstream sent an unreadable response: "+err.Error(), http.StatusBadGateway)
		return
	}
	logUsage(out.key, out.idx, r.Method, target, http.StatusOK, len(body))
	log.Printf("✅ TTS → %.1fs of audio (key[%d], attempt %d)", float64(len(audio))/2/float64(req.SampleRate), out.idx, out.attempt)

	if req.ResponseFormat == "pcm" {
		w.Header().Set("Content-Type", fmt.Sprintf("audio/L16; rate=%d; channels=1", req.SampleRate))
		w.Write(audio)
		return
	}
	w.Header().Set("Content-Type", "audio/wav")
	w.Write(wavHeader(len(audio), req.SampleRate))
	w.Write(audio)
}

// ── gRPC framing ──────────────────────────────

// grpcFrame wraps one message in gRPC's 5-byte prefix: an uncompressed flag
// and the big-endian message length.
func grpcFrame(msg []byte) []byte {
	out := make([]byte, 5, 5+len(msg))
	binary.BigEndian.PutUint32(out[1:], uint32(len(msg)))
	return append(out, msg...)
}

// grpcStatusTransport rewrites a trailers-only gRPC error — HTTP 200 with a
// non-zero grpc-status in the headers — into the matching HTTP status, so
// withKeyRotation cools down a rate-limited key or quarantines a rejected one
// exactly as it would for a plain HTTP upstream.
type grpcStatusTransport struct{ base http.RoundTripper }

func (t grpcStatusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if code := resp.Header.Get("Grpc-Status"); resp.StatusCode == http.StatusOK && code != "" && code != "0" {
		resp.StatusCode = grpcToHTTP(code)
		resp.Status = fmt.Sprintf("%d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	return resp, nil
}

// grpcToHTTP follows the standard gRPC → HTTP status mapping.
func grpcToHTTP(code string) int {
	switch code {
	case "0":
		return http.StatusOK
	case "3", "9", "11": // INVALID_ARGUMENT, FAILED_PRECONDITION, OUT_OF_RANGE
		return http.StatusBadRequest
	case "4": // DEADLINE_EXCEEDED
		return http.StatusGatewayTimeout
	case "5": // NOT_FOUND
		return http.StatusNotFound
	case "7": // PERMISSION_DENIED
		return http.StatusForbidden
	case "8": // RESOURCE_EXHAUSTED
		return http.StatusTooManyRequests
	case "12": // UNIMPLEMENTED
		return http.StatusNotImplemented
	case "14": // UNAVAILABLE
		return http.StatusServiceUnavailable
	case "16": // UNAUTHENTICATED
		return http.StatusUnauthorized
	}
	return http.StatusInternalServerError
}

// grpcMessage pulls the human-readable error out of a header or trailer set.
func grpcMessage(h http.Header) string {
	msg := h.Get("Grpc-Message")
	if m, err := url.PathUnescape(msg); err == nil {
		msg = m
	}
	if msg == "" {
		msg = "grpc-status " + h.Get("Grpc-Status")
	}
	return msg
}

// ── Protobuf (just the fields TTS needs) ──────

func pbVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func pbBytes(b []byte, field int, v []byte) []byte {
	b = pbVarint(b, uint64(field)<<3|2)
	b = pbVarint(b, uint64(len(v)))
	return append(b, v...)
}

func pbUint(b []byte, field int, v uint64) []byte {
	b = pbVarint(b, uint64(field)<<3)
	return pbVarint(b, v)
}

// encodeSynthesizeRequest builds nvidia.riva.tts.SynthesizeSpeechRequest:
// text=1, language_code=2, encoding=3, sample_rate_hz=4, voice_name=5,
// zero_shot_data=6 {audio_prompt=1, quality=4, transcript=5}.
func encodeSynthesizeRequest(r ttsRequest, prompt []byte) []byte {
	var b []byte
	b = pbBytes(b, 1, []byte(r.Input))
	b = pbBytes(b, 2, []byte(r.Language))
	b = pbUint(b, 3, 1) // LINEAR_PCM
	b = pbUint(b, 4, uint64(r.SampleRate))
	if r.Voice != "" {
		b = pbBytes(b, 5, []byte(r.Voice))
	}
	if len(prompt) > 0 {
		var z []byte
		z = pbBytes(z, 1, prompt)
		if r.Quality > 0 {
			z = pbUint(z, 4, uint64(r.Quality))
		}
		if r.AudioPromptTranscript != "" {
			z = pbBytes(z, 5, []byte(r.AudioPromptTranscript))
		}
		b = pbBytes(b, 6, z)
	}
	return b
}

// pbWalk calls fn for every top-level field of a protobuf message. For
// length-delimited fields val holds the payload; for the rest n holds the value.
func pbWalk(msg []byte, fn func(field, wire int, val []byte, n uint64)) error {
	for len(msg) > 0 {
		tag, k := binary.Uvarint(msg)
		if k <= 0 {
			return errors.New("bad tag")
		}
		msg = msg[k:]
		field, wire := int(tag>>3), int(tag&7)
		switch wire {
		case 0:
			v, k := binary.Uvarint(msg)
			if k <= 0 {
				return errors.New("bad varint")
			}
			fn(field, wire, nil, v)
			msg = msg[k:]
		case 1, 5:
			size := 8
			if wire == 5 {
				size = 4
			}
			if len(msg) < size {
				return errors.New("truncated fixed field")
			}
			fn(field, wire, msg[:size], 0)
			msg = msg[size:]
		case 2:
			l, k := binary.Uvarint(msg)
			if k <= 0 || uint64(len(msg)-k) < l {
				return errors.New("truncated length-delimited field")
			}
			fn(field, wire, msg[k:k+int(l)], 0)
			msg = msg[k+int(l):]
		default:
			return fmt.Errorf("unsupported wire type %d", wire)
		}
	}
	return nil
}

// parseSynthesizeResponse unwraps the gRPC frame and returns the audio field
// (1) of nvidia.riva.tts.SynthesizeSpeechResponse.
func parseSynthesizeResponse(body []byte) ([]byte, error) {
	if len(body) < 5 {
		return nil, errors.New("no gRPC message")
	}
	if body[0] != 0 {
		return nil, errors.New("compressed gRPC message")
	}
	n := binary.BigEndian.Uint32(body[1:5])
	if uint64(len(body)-5) < uint64(n) {
		return nil, errors.New("truncated gRPC message")
	}
	var audio []byte
	err := pbWalk(body[5:5+n], func(field, wire int, val []byte, _ uint64) {
		if field == 1 && wire == 2 {
			audio = val
		}
	})
	if err != nil {
		return nil, err
	}
	if len(audio) == 0 {
		return nil, errors.New("response carried no audio")
	}
	return audio, nil
}

// wavHeader is the 44-byte RIFF header for mono 16-bit PCM.
func wavHeader(dataLen, rate int) []byte {
	h := make([]byte, 44)
	copy(h[0:], "RIFF")
	binary.LittleEndian.PutUint32(h[4:], uint32(36+dataLen))
	copy(h[8:], "WAVEfmt ")
	binary.LittleEndian.PutUint32(h[16:], 16)
	binary.LittleEndian.PutUint16(h[20:], 1) // PCM
	binary.LittleEndian.PutUint16(h[22:], 1) // mono
	binary.LittleEndian.PutUint32(h[24:], uint32(rate))
	binary.LittleEndian.PutUint32(h[28:], uint32(rate*2))
	binary.LittleEndian.PutUint16(h[32:], 2)
	binary.LittleEndian.PutUint16(h[34:], 16)
	copy(h[36:], "data")
	binary.LittleEndian.PutUint32(h[40:], uint32(dataLen))
	return h
}
