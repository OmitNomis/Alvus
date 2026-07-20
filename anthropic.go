package main

// Anthropic Messages API ⇄ OpenAI Chat Completions translation.
//
// This lets Claude Code (which speaks ONLY the Anthropic Messages API) talk to
// any OpenAI-compatible backend through Alvus. Claude Code is pointed here with:
//
//   ANTHROPIC_BASE_URL=http://localhost:3000
//   ANTHROPIC_AUTH_TOKEN=sk-dummy          (ignored — Alvus injects a pooled key)
//   ANTHROPIC_MODEL=<upstream model name>  (or set OVERRIDE_MODEL on Alvus)
//
// The existing OpenAI pass-through (/v1/chat/completions via proxyHandler) is
// untouched; this is a separate /v1/messages handler. Heads-up: Claude Code is
// tuned for Claude models, so non-Claude backends will be rough on tool use.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

var idCounter uint64

func genID(prefix string) string {
	return fmt.Sprintf("%s%d%d", prefix, time.Now().UnixNano(), atomic.AddUint64(&idCounter, 1))
}

// ── Anthropic request shapes (the subset we translate) ──────────────

type antRequest struct {
	Model       string          `json:"model"`
	MaxTokens   int             `json:"max_tokens"`
	System      json.RawMessage `json:"system,omitempty"`
	Messages    []antMessage    `json:"messages"`
	Tools       []antTool       `json:"tools,omitempty"`
	ToolChoice  json.RawMessage `json:"tool_choice,omitempty"`
	Stream      bool            `json:"stream,omitempty"`
	Temperature *float64        `json:"temperature,omitempty"`
	TopP        *float64        `json:"top_p,omitempty"`
}

type antMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

type antTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type antBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   json.RawMessage `json:"content,omitempty"`
	IsError   bool            `json:"is_error,omitempty"`
	Source    *antImageSource `json:"source,omitempty"`
}

type antImageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type"`
	Data      string `json:"data"`
	URL       string `json:"url"`
}

// ── Request: Anthropic → OpenAI ─────────────────────────────────────

func translateAnthropicToOpenAI(raw []byte, overrideModel string) (body []byte, model string, stream bool, err error) {
	var req antRequest
	if err = json.Unmarshal(raw, &req); err != nil {
		return nil, "", false, fmt.Errorf("invalid messages request: %v", err)
	}

	model = req.Model
	if overrideModel != "" {
		model = overrideModel
	}

	oai := map[string]any{"model": model}
	if req.MaxTokens > 0 {
		oai["max_tokens"] = req.MaxTokens
	}
	if req.Stream {
		oai["stream"] = true
		oai["stream_options"] = map[string]any{"include_usage": true}
	}
	if req.Temperature != nil {
		oai["temperature"] = *req.Temperature
	}
	if req.TopP != nil {
		oai["top_p"] = *req.TopP
	}

	var msgs []map[string]any

	// Anthropic's top-level `system` becomes a leading system message.
	if sys := flattenSystem(req.System); sys != "" {
		msgs = append(msgs, map[string]any{"role": "system", "content": sys})
	}

	for _, m := range req.Messages {
		msgs = append(msgs, translateMessage(m)...)
	}
	oai["messages"] = msgs

	if len(req.Tools) > 0 {
		var tools []map[string]any
		for _, t := range req.Tools {
			params := json.RawMessage(t.InputSchema)
			if len(params) == 0 {
				params = json.RawMessage(`{"type":"object","properties":{}}`)
			}
			tools = append(tools, map[string]any{
				"type": "function",
				"function": map[string]any{
					"name":        t.Name,
					"description": t.Description,
					"parameters":  params,
				},
			})
		}
		oai["tools"] = tools
		if tc := translateToolChoice(req.ToolChoice); tc != nil {
			oai["tool_choice"] = tc
		}
	}

	body, err = json.Marshal(oai)
	return body, model, req.Stream, err
}

func flattenSystem(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var blocks []antBlock
	if json.Unmarshal(raw, &blocks) == nil {
		var parts []string
		for _, b := range blocks {
			if b.Type == "text" && b.Text != "" {
				parts = append(parts, b.Text)
			}
		}
		return strings.Join(parts, "\n\n")
	}
	return ""
}

// translateMessage expands one Anthropic message into one or more OpenAI
// messages (tool_result blocks each become a separate role:"tool" message).
func translateMessage(m antMessage) []map[string]any {
	// content may be a plain string …
	var str string
	if json.Unmarshal(m.Content, &str) == nil {
		return []map[string]any{{"role": m.Role, "content": str}}
	}

	// … or an array of content blocks.
	var blocks []antBlock
	if json.Unmarshal(m.Content, &blocks) != nil {
		return []map[string]any{{"role": m.Role, "content": ""}}
	}

	var out []map[string]any

	if m.Role == "assistant" {
		var text strings.Builder
		var toolCalls []map[string]any
		for _, b := range blocks {
			switch b.Type {
			case "text":
				text.WriteString(b.Text)
			case "tool_use":
				args := string(b.Input)
				if args == "" {
					args = "{}"
				}
				toolCalls = append(toolCalls, map[string]any{
					"id":   b.ID,
					"type": "function",
					"function": map[string]any{
						"name":      b.Name,
						"arguments": args,
					},
				})
			}
		}
		msg := map[string]any{"role": "assistant"}
		if text.Len() > 0 {
			msg["content"] = text.String()
		} else {
			msg["content"] = nil
		}
		if len(toolCalls) > 0 {
			msg["tool_calls"] = toolCalls
		}
		return []map[string]any{msg}
	}

	// user / tool role: tool_result blocks split out, the rest forms one message.
	var parts []map[string]any
	var plain strings.Builder
	hasImage := false
	for _, b := range blocks {
		switch b.Type {
		case "tool_result":
			out = append(out, map[string]any{
				"role":         "tool",
				"tool_call_id": b.ToolUseID,
				"content":      flattenToolResult(b.Content),
			})
		case "text":
			plain.WriteString(b.Text)
			parts = append(parts, map[string]any{"type": "text", "text": b.Text})
		case "image":
			if b.Source != nil {
				url := b.Source.URL
				if url == "" && b.Source.Data != "" {
					url = "data:" + b.Source.MediaType + ";base64," + b.Source.Data
				}
				if url != "" {
					hasImage = true
					parts = append(parts, map[string]any{
						"type":      "image_url",
						"image_url": map[string]any{"url": url},
					})
				}
			}
		}
	}

	if len(parts) > 0 {
		content := any(plain.String())
		if hasImage {
			content = parts
		}
		out = append(out, map[string]any{"role": "user", "content": content})
	}
	return out
}

func flattenToolResult(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var blocks []antBlock
	if json.Unmarshal(raw, &blocks) == nil {
		var parts []string
		for _, b := range blocks {
			if b.Text != "" {
				parts = append(parts, b.Text)
			}
		}
		return strings.Join(parts, "\n")
	}
	return string(raw)
}

func translateToolChoice(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var tc struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}
	if json.Unmarshal(raw, &tc) != nil {
		return nil
	}
	switch tc.Type {
	case "auto":
		return "auto"
	case "any":
		return "required"
	case "none":
		return "none"
	case "tool":
		return map[string]any{"type": "function", "function": map[string]any{"name": tc.Name}}
	}
	return nil
}

func mapStopReason(finish string, sawTool bool) string {
	if sawTool {
		return "tool_use"
	}
	switch finish {
	case "stop":
		return "end_turn"
	case "length":
		return "max_tokens"
	case "tool_calls":
		return "tool_use"
	default:
		return "end_turn"
	}
}

// ── OpenAI response shapes ──────────────────────────────────────────

type oaiToolCall struct {
	Index    int    `json:"index"`
	ID       string `json:"id"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type oaiResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content   string        `json:"content"`
			ToolCalls []oaiToolCall `json:"tool_calls"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// ── Response: OpenAI → Anthropic (non-streaming) ────────────────────

func translateOpenAIResponse(raw []byte, model string) []byte {
	var resp oaiResponse
	if err := json.Unmarshal(raw, &resp); err != nil || len(resp.Choices) == 0 {
		return mustJSON(anthErrorBody("api_error", "upstream returned an unparseable response"))
	}
	choice := resp.Choices[0]

	var content []map[string]any
	if choice.Message.Content != "" {
		content = append(content, map[string]any{"type": "text", "text": choice.Message.Content})
	}
	for _, tc := range choice.Message.ToolCalls {
		var input any = map[string]any{}
		if tc.Function.Arguments != "" {
			_ = json.Unmarshal([]byte(tc.Function.Arguments), &input)
		}
		id := tc.ID
		if id == "" {
			id = genID("toolu_")
		}
		content = append(content, map[string]any{
			"type":  "tool_use",
			"id":    id,
			"name":  tc.Function.Name,
			"input": input,
		})
	}
	if content == nil {
		content = []map[string]any{}
	}

	msg := map[string]any{
		"id":            genID("msg_"),
		"type":          "message",
		"role":          "assistant",
		"model":         model,
		"content":       content,
		"stop_reason":   mapStopReason(choice.FinishReason, len(choice.Message.ToolCalls) > 0),
		"stop_sequence": nil,
		"usage": map[string]any{
			"input_tokens":  resp.Usage.PromptTokens,
			"output_tokens": resp.Usage.CompletionTokens,
		},
	}
	return mustJSON(msg)
}

// ── Response: OpenAI SSE → Anthropic SSE (streaming) ────────────────

type oaiStreamChunk struct {
	Choices []struct {
		Delta struct {
			Content   string        `json:"content"`
			ToolCalls []oaiToolCall `json:"tool_calls"`
		} `json:"delta"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

func writeSSE(w io.Writer, f http.Flusher, event string, payload any) {
	b, _ := json.Marshal(payload)
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
	f.Flush()
}

// streamOpenAIToAnthropic consumes the upstream OpenAI SSE stream and re-emits
// it as the Anthropic event sequence Claude Code expects.
func streamOpenAIToAnthropic(w http.ResponseWriter, f http.Flusher, body io.Reader, model string) {
	msgID := genID("msg_")

	writeSSE(w, f, "message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            msgID,
			"type":          "message",
			"role":          "assistant",
			"model":         model,
			"content":       []any{},
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage":         map[string]any{"input_tokens": 0, "output_tokens": 0},
		},
	})

	nextIndex := 0
	textOpen := false
	textIndex := 0
	type toolState struct{ anthIndex int }
	tools := map[int]*toolState{} // OpenAI tool_call index → Anthropic block
	finish := ""
	outputTokens := 0
	sawTool := false

	closeText := func() {
		if textOpen {
			writeSSE(w, f, "content_block_stop", map[string]any{"type": "content_block_stop", "index": textIndex})
			textOpen = false
		}
	}

	reader := bufio.NewReader(body)
	for {
		line, err := reader.ReadString('\n')
		if line != "" {
			line = strings.TrimRight(line, "\r\n")
			if strings.HasPrefix(line, "data:") {
				data := strings.TrimSpace(line[5:])
				if data == "[DONE]" {
					break
				}
				var chunk oaiStreamChunk
				if json.Unmarshal([]byte(data), &chunk) != nil {
					goto next
				}
				if chunk.Usage != nil {
					outputTokens = chunk.Usage.CompletionTokens
				}
				if len(chunk.Choices) == 0 {
					goto next
				}
				ch := chunk.Choices[0]
				if ch.FinishReason != "" {
					finish = ch.FinishReason
				}

				if ch.Delta.Content != "" {
					if !textOpen {
						textIndex = nextIndex
						nextIndex++
						textOpen = true
						writeSSE(w, f, "content_block_start", map[string]any{
							"type":          "content_block_start",
							"index":         textIndex,
							"content_block": map[string]any{"type": "text", "text": ""},
						})
					}
					writeSSE(w, f, "content_block_delta", map[string]any{
						"type":  "content_block_delta",
						"index": textIndex,
						"delta": map[string]any{"type": "text_delta", "text": ch.Delta.Content},
					})
				}

				for _, tc := range ch.Delta.ToolCalls {
					closeText()
					st, ok := tools[tc.Index]
					if !ok {
						sawTool = true
						st = &toolState{anthIndex: nextIndex}
						nextIndex++
						tools[tc.Index] = st
						id := tc.ID
						if id == "" {
							id = genID("toolu_")
						}
						writeSSE(w, f, "content_block_start", map[string]any{
							"type":  "content_block_start",
							"index": st.anthIndex,
							"content_block": map[string]any{
								"type":  "tool_use",
								"id":    id,
								"name":  tc.Function.Name,
								"input": map[string]any{},
							},
						})
					}
					if tc.Function.Arguments != "" {
						writeSSE(w, f, "content_block_delta", map[string]any{
							"type":  "content_block_delta",
							"index": st.anthIndex,
							"delta": map[string]any{"type": "input_json_delta", "partial_json": tc.Function.Arguments},
						})
					}
				}
			}
		}
	next:
		if err != nil {
			break
		}
	}

	// Close whatever block is still open.
	closeText()
	for _, st := range tools {
		writeSSE(w, f, "content_block_stop", map[string]any{"type": "content_block_stop", "index": st.anthIndex})
	}

	writeSSE(w, f, "message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": mapStopReason(finish, sawTool), "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": outputTokens},
	})
	writeSSE(w, f, "message_stop", map[string]any{"type": "message_stop"})
}

// ── HTTP handlers ───────────────────────────────────────────────────

func anthErrorBody(typ, msg string) map[string]any {
	return map[string]any{"type": "error", "error": map[string]any{"type": typ, "message": msg}}
}

func anthErr(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(anthErrorBody(typ, msg))
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func (s *ServerState) anthropicHandler(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	cfg := s.cfg
	pool := s.pool
	s.mu.RUnlock()

	if r.Method != http.MethodPost {
		anthErr(w, http.StatusMethodNotAllowed, "invalid_request_error", "method not allowed")
		return
	}

	raw, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		anthErr(w, http.StatusBadRequest, "invalid_request_error", "failed to read request body")
		return
	}

	oaiBody, model, stream, err := translateAnthropicToOpenAI(raw, cfg.OverrideModel)
	if err != nil {
		anthErr(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}

	target := cfg.TargetBase + "/chat/completions"

	log.Printf("→ [anthropic] %s model=%s stream=%v (%d→%d bytes)", target, model, stream, len(raw), len(oaiBody))

	// Zero deadline for streams — never cut a live one.
	opts := rotateOpts{PerAttemptTimeout: nonStreamTimeout}
	if stream {
		opts.PerAttemptTimeout = 0
	}

	out, rerr := withKeyRotation(r.Context(), cfg, pool, opts, "[anthropic] ", func(key string) (*http.Request, error) {
		req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(oaiBody))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+key)
		if stream {
			req.Header.Set("Accept", "text/event-stream")
		}
		return req, nil
	})
	if rerr != nil {
		anthErr(w, rerr.status, "api_error", rerr.msg)
		return
	}
	// LIFO: body closes first, then the attempt context is released.
	defer out.release()
	defer out.resp.Body.Close()
	resp := out.resp

	// Terminal 4xx: surface upstream's complaint in Anthropic's error shape.
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("⚠️ [anthropic] Upstream %d (terminal): %s", resp.StatusCode, body)
		anthErr(w, resp.StatusCode, "invalid_request_error", strings.TrimSpace(string(body)))
		return
	}

	logUsage(out.key, out.idx, http.MethodPost, target, resp.StatusCode, len(raw))

	if stream {
		f, ok := w.(http.Flusher)
		if !ok {
			anthErr(w, http.StatusInternalServerError, "api_error", "streaming unsupported by server")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		streamOpenAIToAnthropic(w, f, resp.Body, model)
		log.Printf("✅ [anthropic] streamed (key[%d], attempt %d)", out.idx, out.attempt)
		return
	}

	body, _ := io.ReadAll(resp.Body)
	translated := translateOpenAIResponse(body, model)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(translated)
	log.Printf("✅ [anthropic] %d (key[%d], attempt %d)", resp.StatusCode, out.idx, out.attempt)
}

// anthropicCountTokensHandler returns a rough estimate so Claude Code's
// pre-flight token check doesn't error. We don't proxy this upstream because
// OpenAI-compatible backends have no equivalent endpoint.
func (s *ServerState) anthropicCountTokensHandler(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	r.Body.Close()
	// ~4 chars per token is the usual ballpark.
	est := len(raw) / 4
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"input_tokens": est})
}
