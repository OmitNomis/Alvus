package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// translate runs an Anthropic request body through the translator and hands
// back the decoded OpenAI payload.
func translate(t *testing.T, body string, override string) map[string]any {
	t.Helper()
	out, _, _, err := translateAnthropicToOpenAI([]byte(body), override)
	if err != nil {
		t.Fatalf("translateAnthropicToOpenAI: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatalf("translated body is not valid JSON: %v\n%s", err, out)
	}
	return got
}

// messages pulls the translated message list out as a slice of maps.
func messages(t *testing.T, payload map[string]any) []map[string]any {
	t.Helper()
	raw, ok := payload["messages"].([]any)
	if !ok {
		t.Fatalf("no messages in payload: %v", payload)
	}
	out := make([]map[string]any, len(raw))
	for i, m := range raw {
		out[i], ok = m.(map[string]any)
		if !ok {
			t.Fatalf("message %d is not an object: %v", i, m)
		}
	}
	return out
}

// ── Request translation ─────────────────────────────────────────────

func TestTranslateBasicRequest(t *testing.T) {
	got := translate(t, `{
		"model": "claude-x",
		"max_tokens": 512,
		"temperature": 0.7,
		"messages": [{"role": "user", "content": "hello"}]
	}`, "")

	if got["model"] != "claude-x" {
		t.Errorf("model = %v", got["model"])
	}
	if got["max_tokens"] != float64(512) {
		t.Errorf("max_tokens = %v", got["max_tokens"])
	}
	if got["temperature"] != 0.7 {
		t.Errorf("temperature = %v", got["temperature"])
	}
	if _, ok := got["stream"]; ok {
		t.Error("stream should be absent when not requested")
	}

	msgs := messages(t, got)
	if len(msgs) != 1 || msgs[0]["role"] != "user" || msgs[0]["content"] != "hello" {
		t.Errorf("messages = %v", msgs)
	}
}

func TestOverrideModelWins(t *testing.T) {
	got := translate(t, `{"model":"claude-x","messages":[]}`, "forced/model")
	if got["model"] != "forced/model" {
		t.Errorf("model = %v, want the override", got["model"])
	}
}

func TestStreamRequestsUsage(t *testing.T) {
	got := translate(t, `{"model":"m","stream":true,"messages":[]}`, "")

	if got["stream"] != true {
		t.Errorf("stream = %v", got["stream"])
	}
	// Without include_usage the final chunk carries no token counts, and the
	// translated message_delta would report zero.
	opts, ok := got["stream_options"].(map[string]any)
	if !ok || opts["include_usage"] != true {
		t.Errorf("stream_options = %v, want include_usage:true", got["stream_options"])
	}
}

func TestSystemPromptBecomesLeadingMessage(t *testing.T) {
	t.Run("string form", func(t *testing.T) {
		got := translate(t, `{
			"model":"m",
			"system":"be terse",
			"messages":[{"role":"user","content":"hi"}]
		}`, "")
		msgs := messages(t, got)
		if len(msgs) != 2 {
			t.Fatalf("got %d messages, want 2", len(msgs))
		}
		if msgs[0]["role"] != "system" || msgs[0]["content"] != "be terse" {
			t.Errorf("leading message = %v", msgs[0])
		}
	})

	t.Run("block form is joined", func(t *testing.T) {
		got := translate(t, `{
			"model":"m",
			"system":[{"type":"text","text":"first"},{"type":"text","text":"second"}],
			"messages":[]
		}`, "")
		msgs := messages(t, got)
		if len(msgs) != 1 || msgs[0]["content"] != "first\n\nsecond" {
			t.Errorf("system = %v", msgs[0])
		}
	})

	t.Run("absent system adds no message", func(t *testing.T) {
		got := translate(t, `{"model":"m","messages":[{"role":"user","content":"hi"}]}`, "")
		if msgs := messages(t, got); len(msgs) != 1 {
			t.Errorf("got %d messages, want 1", len(msgs))
		}
	})
}

func TestToolDefinitionsTranslate(t *testing.T) {
	got := translate(t, `{
		"model":"m",
		"messages":[],
		"tools":[{
			"name":"read_file",
			"description":"Read a file",
			"input_schema":{"type":"object","properties":{"path":{"type":"string"}}}
		}]
	}`, "")

	tools, ok := got["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %v", got["tools"])
	}
	tool := tools[0].(map[string]any)
	if tool["type"] != "function" {
		t.Errorf("type = %v", tool["type"])
	}
	fn := tool["function"].(map[string]any)
	if fn["name"] != "read_file" || fn["description"] != "Read a file" {
		t.Errorf("function = %v", fn)
	}
	params, ok := fn["parameters"].(map[string]any)
	if !ok || params["type"] != "object" {
		t.Errorf("parameters = %v", fn["parameters"])
	}
}

func TestToolWithoutSchemaGetsAnEmptyObject(t *testing.T) {
	// An omitted input_schema must still produce a valid JSON Schema, or
	// strict backends reject the whole request.
	got := translate(t, `{"model":"m","messages":[],"tools":[{"name":"ping"}]}`, "")

	tools := got["tools"].([]any)
	fn := tools[0].(map[string]any)["function"].(map[string]any)
	params, ok := fn["parameters"].(map[string]any)
	if !ok || params["type"] != "object" {
		t.Errorf("parameters = %v, want an empty object schema", fn["parameters"])
	}
}

func TestToolChoiceMapping(t *testing.T) {
	tests := []struct {
		in   string
		want any
	}{
		{`{"type":"auto"}`, "auto"},
		{`{"type":"any"}`, "required"},
		{`{"type":"none"}`, "none"},
	}
	for _, tc := range tests {
		got := translate(t, `{
			"model":"m","messages":[],
			"tools":[{"name":"ping","input_schema":{"type":"object"}}],
			"tool_choice":`+tc.in+`
		}`, "")
		if got["tool_choice"] != tc.want {
			t.Errorf("tool_choice %s → %v, want %v", tc.in, got["tool_choice"], tc.want)
		}
	}

	t.Run("named tool", func(t *testing.T) {
		got := translate(t, `{
			"model":"m","messages":[],
			"tools":[{"name":"ping","input_schema":{"type":"object"}}],
			"tool_choice":{"type":"tool","name":"ping"}
		}`, "")
		tc, ok := got["tool_choice"].(map[string]any)
		if !ok {
			t.Fatalf("tool_choice = %v", got["tool_choice"])
		}
		fn := tc["function"].(map[string]any)
		if tc["type"] != "function" || fn["name"] != "ping" {
			t.Errorf("tool_choice = %v", tc)
		}
	})

	t.Run("dropped without tools", func(t *testing.T) {
		got := translate(t, `{"model":"m","messages":[],"tool_choice":{"type":"auto"}}`, "")
		if _, ok := got["tool_choice"]; ok {
			t.Error("tool_choice should not be sent when there are no tools")
		}
	})
}

func TestAssistantToolUseBecomesToolCalls(t *testing.T) {
	got := translate(t, `{
		"model":"m",
		"messages":[{
			"role":"assistant",
			"content":[
				{"type":"text","text":"Let me look."},
				{"type":"tool_use","id":"toolu_1","name":"read_file","input":{"path":"a.go"}}
			]
		}]
	}`, "")

	msgs := messages(t, got)
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if msgs[0]["content"] != "Let me look." {
		t.Errorf("content = %v", msgs[0]["content"])
	}

	calls, ok := msgs[0]["tool_calls"].([]any)
	if !ok || len(calls) != 1 {
		t.Fatalf("tool_calls = %v", msgs[0]["tool_calls"])
	}
	call := calls[0].(map[string]any)
	if call["id"] != "toolu_1" || call["type"] != "function" {
		t.Errorf("call = %v", call)
	}
	fn := call["function"].(map[string]any)
	if fn["name"] != "read_file" {
		t.Errorf("name = %v", fn["name"])
	}
	// Arguments must be a JSON *string*, not an object.
	args, ok := fn["arguments"].(string)
	if !ok {
		t.Fatalf("arguments = %T, want a string", fn["arguments"])
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(args), &decoded); err != nil {
		t.Fatalf("arguments is not valid JSON: %v (%q)", err, args)
	}
	if decoded["path"] != "a.go" {
		t.Errorf("arguments = %q", args)
	}
}

func TestAssistantToolUseWithoutTextSendsNullContent(t *testing.T) {
	got := translate(t, `{
		"model":"m",
		"messages":[{"role":"assistant","content":[
			{"type":"tool_use","id":"t1","name":"ping","input":{}}
		]}]
	}`, "")

	msgs := messages(t, got)
	if content, ok := msgs[0]["content"]; !ok || content != nil {
		t.Errorf("content = %v, want an explicit null", content)
	}
}

func TestToolResultBecomesItsOwnMessage(t *testing.T) {
	got := translate(t, `{
		"model":"m",
		"messages":[{
			"role":"user",
			"content":[
				{"type":"tool_result","tool_use_id":"toolu_1","content":"file contents"},
				{"type":"text","text":"what does it do?"}
			]
		}]
	}`, "")

	msgs := messages(t, got)
	if len(msgs) != 2 {
		t.Fatalf("got %d messages, want 2 (tool result + user text)", len(msgs))
	}
	if msgs[0]["role"] != "tool" || msgs[0]["tool_call_id"] != "toolu_1" {
		t.Errorf("tool message = %v", msgs[0])
	}
	if msgs[0]["content"] != "file contents" {
		t.Errorf("tool content = %v", msgs[0]["content"])
	}
	if msgs[1]["role"] != "user" || msgs[1]["content"] != "what does it do?" {
		t.Errorf("user message = %v", msgs[1])
	}
}

func TestToolResultBlockContentIsFlattened(t *testing.T) {
	got := translate(t, `{
		"model":"m",
		"messages":[{"role":"user","content":[
			{"type":"tool_result","tool_use_id":"t1","content":[
				{"type":"text","text":"line one"},
				{"type":"text","text":"line two"}
			]}
		]}]
	}`, "")

	msgs := messages(t, got)
	if msgs[0]["content"] != "line one\nline two" {
		t.Errorf("content = %v", msgs[0]["content"])
	}
}

func TestImageBlocksBecomeImageURLParts(t *testing.T) {
	got := translate(t, `{
		"model":"m",
		"messages":[{"role":"user","content":[
			{"type":"text","text":"what is this?"},
			{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}
		]}]
	}`, "")

	msgs := messages(t, got)
	parts, ok := msgs[0]["content"].([]any)
	if !ok {
		t.Fatalf("content = %T, want an array of parts once an image is present", msgs[0]["content"])
	}
	if len(parts) != 2 {
		t.Fatalf("got %d parts, want 2", len(parts))
	}
	img := parts[1].(map[string]any)
	if img["type"] != "image_url" {
		t.Fatalf("part = %v", img)
	}
	url := img["image_url"].(map[string]any)["url"].(string)
	if url != "data:image/png;base64,AAAA" {
		t.Errorf("url = %q", url)
	}
}

func TestTextOnlyContentStaysAString(t *testing.T) {
	// Only promote to the parts array when there is actually an image —
	// some backends handle plain strings better.
	got := translate(t, `{
		"model":"m",
		"messages":[{"role":"user","content":[{"type":"text","text":"plain"}]}]
	}`, "")

	msgs := messages(t, got)
	if _, ok := msgs[0]["content"].(string); !ok {
		t.Errorf("content = %T, want a string", msgs[0]["content"])
	}
}

func TestInvalidRequestIsRejected(t *testing.T) {
	if _, _, _, err := translateAnthropicToOpenAI([]byte(`not json`), ""); err == nil {
		t.Error("want an error for a malformed body")
	}
}

// ── Response translation ────────────────────────────────────────────

func TestTranslateResponseText(t *testing.T) {
	got := translateOpenAIResponse([]byte(`{
		"choices":[{"message":{"content":"hello there"},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":10,"completion_tokens":3}
	}`), "some/model")

	var msg map[string]any
	if err := json.Unmarshal(got, &msg); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}

	if msg["type"] != "message" || msg["role"] != "assistant" {
		t.Errorf("envelope = %v", msg)
	}
	if msg["model"] != "some/model" {
		t.Errorf("model = %v", msg["model"])
	}
	if msg["stop_reason"] != "end_turn" {
		t.Errorf("stop_reason = %v", msg["stop_reason"])
	}
	if !strings.HasPrefix(msg["id"].(string), "msg_") {
		t.Errorf("id = %v", msg["id"])
	}

	content := msg["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("content = %v", content)
	}
	block := content[0].(map[string]any)
	if block["type"] != "text" || block["text"] != "hello there" {
		t.Errorf("block = %v", block)
	}

	usage := msg["usage"].(map[string]any)
	if usage["input_tokens"] != float64(10) || usage["output_tokens"] != float64(3) {
		t.Errorf("usage = %v", usage)
	}
}

func TestTranslateResponseToolCall(t *testing.T) {
	got := translateOpenAIResponse([]byte(`{
		"choices":[{
			"message":{"content":"","tool_calls":[{
				"id":"call_1",
				"function":{"name":"read_file","arguments":"{\"path\":\"a.go\"}"}
			}]},
			"finish_reason":"tool_calls"
		}]
	}`), "m")

	var msg map[string]any
	json.Unmarshal(got, &msg)

	if msg["stop_reason"] != "tool_use" {
		t.Errorf("stop_reason = %v", msg["stop_reason"])
	}
	content := msg["content"].([]any)
	if len(content) != 1 {
		t.Fatalf("content = %v", content)
	}
	block := content[0].(map[string]any)
	if block["type"] != "tool_use" || block["id"] != "call_1" || block["name"] != "read_file" {
		t.Errorf("block = %v", block)
	}
	// input must be a decoded object, not the raw JSON string.
	input, ok := block["input"].(map[string]any)
	if !ok {
		t.Fatalf("input = %T, want an object", block["input"])
	}
	if input["path"] != "a.go" {
		t.Errorf("input = %v", input)
	}
}

func TestTranslateResponseEmptyContentIsAnArray(t *testing.T) {
	// null content breaks clients that iterate the block list.
	got := translateOpenAIResponse([]byte(`{"choices":[{"message":{"content":""}}]}`), "m")

	var msg map[string]any
	json.Unmarshal(got, &msg)
	if content, ok := msg["content"].([]any); !ok || len(content) != 0 {
		t.Errorf("content = %v, want []", msg["content"])
	}
}

func TestTranslateResponseGarbage(t *testing.T) {
	for _, body := range []string{`not json`, `{}`, `{"choices":[]}`} {
		got := translateOpenAIResponse([]byte(body), "m")
		var msg map[string]any
		if err := json.Unmarshal(got, &msg); err != nil {
			t.Fatalf("%q produced invalid JSON: %v", body, err)
		}
		if msg["type"] != "error" {
			t.Errorf("%q → %v, want an error envelope", body, msg)
		}
	}
}

func TestTranslateResponseReasoningBecomesThinking(t *testing.T) {
	got := translateOpenAIResponse([]byte(`{
		"choices":[{"message":{
			"reasoning_content":"let me think about this",
			"content":"the answer is 4"
		},"finish_reason":"stop"}]
	}`), "m")

	var msg map[string]any
	json.Unmarshal(got, &msg)

	content := msg["content"].([]any)
	if len(content) != 2 {
		t.Fatalf("content = %v, want a thinking block then a text block", content)
	}
	// Reasoning leads, mirroring how the model produced it.
	think := content[0].(map[string]any)
	if think["type"] != "thinking" || think["thinking"] != "let me think about this" {
		t.Errorf("thinking block = %v", think)
	}
	text := content[1].(map[string]any)
	if text["type"] != "text" || text["text"] != "the answer is 4" {
		t.Errorf("text block = %v", text)
	}
}

func TestMapStopReason(t *testing.T) {
	tests := []struct {
		finish  string
		sawTool bool
		want    string
	}{
		{"stop", false, "end_turn"},
		{"length", false, "max_tokens"},
		{"tool_calls", false, "tool_use"},
		{"", false, "end_turn"},
		{"something_new", false, "end_turn"},
		// A tool call always wins, whatever the backend called the finish.
		{"stop", true, "tool_use"},
		{"length", true, "tool_use"},
	}
	for _, tc := range tests {
		if got := mapStopReason(tc.finish, tc.sawTool); got != tc.want {
			t.Errorf("mapStopReason(%q, %v) = %q, want %q", tc.finish, tc.sawTool, got, tc.want)
		}
	}
}

// ── Streaming translation ───────────────────────────────────────────

type sseEvent struct {
	event string
	data  map[string]any
}

// runStream feeds canned upstream SSE through the translator and decodes what
// came out the other side.
func runStream(t *testing.T, upstream string) []sseEvent {
	t.Helper()
	rec := httptest.NewRecorder()
	streamOpenAIToAnthropic(rec, rec, strings.NewReader(upstream), "some/model")

	var out []sseEvent
	for _, chunk := range strings.Split(rec.Body.String(), "\n\n") {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}
		var ev sseEvent
		for _, line := range strings.Split(chunk, "\n") {
			switch {
			case strings.HasPrefix(line, "event: "):
				ev.event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				raw := strings.TrimPrefix(line, "data: ")
				if err := json.Unmarshal([]byte(raw), &ev.data); err != nil {
					t.Fatalf("event %q has undecodable data %q: %v", ev.event, raw, err)
				}
			}
		}
		out = append(out, ev)
	}
	return out
}

func eventNames(events []sseEvent) []string {
	names := make([]string, len(events))
	for i, e := range events {
		names[i] = e.event
	}
	return names
}

func TestStreamTextSequence(t *testing.T) {
	events := runStream(t, strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"Hel"}}]}`,
		`data: {"choices":[{"delta":{"content":"lo"}}]}`,
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
		`data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2}}`,
		`data: [DONE]`,
	}, "\n\n")+"\n\n")

	want := []string{
		"message_start",
		"content_block_start",
		"content_block_delta",
		"content_block_delta",
		"content_block_stop",
		"message_delta",
		"message_stop",
	}
	if got := eventNames(events); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("event sequence:\n got %v\nwant %v", got, want)
	}

	// The two deltas carry the text, in order.
	var text strings.Builder
	for _, e := range events {
		if e.event == "content_block_delta" {
			delta := e.data["delta"].(map[string]any)
			if delta["type"] != "text_delta" {
				t.Errorf("delta type = %v", delta["type"])
			}
			text.WriteString(delta["text"].(string))
		}
	}
	if text.String() != "Hello" {
		t.Errorf("reassembled text = %q, want \"Hello\"", text.String())
	}

	last := events[len(events)-2]
	if last.event != "message_delta" {
		t.Fatalf("expected message_delta, got %s", last.event)
	}
	if reason := last.data["delta"].(map[string]any)["stop_reason"]; reason != "end_turn" {
		t.Errorf("stop_reason = %v", reason)
	}
	if usage := last.data["usage"].(map[string]any); usage["output_tokens"] != float64(2) {
		t.Errorf("output_tokens = %v, want 2", usage["output_tokens"])
	}
}

func TestStreamToolCallSequence(t *testing.T) {
	events := runStream(t, strings.Join([]string{
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"read_file","arguments":"{\"pa"}}]}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"th\":\"a.go\"}"}}]}}]}`,
		`data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
		`data: [DONE]`,
	}, "\n\n")+"\n\n")

	var start *sseEvent
	var partial strings.Builder
	for i, e := range events {
		switch e.event {
		case "content_block_start":
			start = &events[i]
		case "content_block_delta":
			delta := e.data["delta"].(map[string]any)
			if delta["type"] != "input_json_delta" {
				t.Errorf("delta type = %v, want input_json_delta", delta["type"])
			}
			partial.WriteString(delta["partial_json"].(string))
		}
	}

	if start == nil {
		t.Fatal("no content_block_start for the tool call")
	}
	block := start.data["content_block"].(map[string]any)
	if block["type"] != "tool_use" || block["id"] != "call_1" || block["name"] != "read_file" {
		t.Errorf("content_block = %v", block)
	}

	// The argument fragments must reassemble into the exact JSON the model sent.
	var args map[string]any
	if err := json.Unmarshal([]byte(partial.String()), &args); err != nil {
		t.Fatalf("reassembled arguments are not valid JSON: %v (%q)", err, partial.String())
	}
	if args["path"] != "a.go" {
		t.Errorf("arguments = %q", partial.String())
	}

	last := events[len(events)-2]
	if reason := last.data["delta"].(map[string]any)["stop_reason"]; reason != "tool_use" {
		t.Errorf("stop_reason = %v, want tool_use", reason)
	}
}

func TestStreamTextThenToolClosesTextBlockFirst(t *testing.T) {
	events := runStream(t, strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"thinking"}}]}`,
		`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c1","function":{"name":"ping","arguments":"{}"}}]}}]}`,
		`data: [DONE]`,
	}, "\n\n")+"\n\n")

	// Anthropic requires a block to be closed before the next one opens.
	var open []string
	depth := 0
	for _, e := range events {
		switch e.event {
		case "content_block_start":
			depth++
			if depth > 1 {
				t.Fatalf("a second block opened while one was still open: %v", eventNames(events))
			}
			open = append(open, "start")
		case "content_block_stop":
			depth--
			open = append(open, "stop")
		}
	}
	if depth != 0 {
		t.Errorf("stream ended with %d block(s) still open", depth)
	}
	if len(open) != 4 {
		t.Errorf("block events = %v, want start/stop for both text and tool", open)
	}
}

func TestStreamReasoningBecomesThinkingBlock(t *testing.T) {
	events := runStream(t, strings.Join([]string{
		`data: {"choices":[{"delta":{"reasoning_content":"hmm"}}]}`,
		`data: {"choices":[{"delta":{"reasoning_content":", let me see"}}]}`,
		`data: {"choices":[{"delta":{"content":"done"}}]}`,
		`data: {"choices":[{"delta":{},"finish_reason":"stop"}]}`,
		`data: [DONE]`,
	}, "\n\n")+"\n\n")

	// The thinking block must open, fill, and close before the text block opens
	// — Anthropic never allows two open blocks at once.
	depth := 0
	var thinking, text strings.Builder
	firstStartType := ""
	for _, e := range events {
		switch e.event {
		case "content_block_start":
			depth++
			if depth > 1 {
				t.Fatalf("a block opened while one was still open: %v", eventNames(events))
			}
			if firstStartType == "" {
				firstStartType = e.data["content_block"].(map[string]any)["type"].(string)
			}
		case "content_block_stop":
			depth--
		case "content_block_delta":
			delta := e.data["delta"].(map[string]any)
			switch delta["type"] {
			case "thinking_delta":
				thinking.WriteString(delta["thinking"].(string))
			case "text_delta":
				text.WriteString(delta["text"].(string))
			}
		}
	}

	if depth != 0 {
		t.Errorf("stream ended with %d block(s) open", depth)
	}
	if firstStartType != "thinking" {
		t.Errorf("first block = %q, want thinking to lead", firstStartType)
	}
	if thinking.String() != "hmm, let me see" {
		t.Errorf("reassembled thinking = %q", thinking.String())
	}
	if text.String() != "done" {
		t.Errorf("reassembled text = %q", text.String())
	}
}

func TestStreamIgnoresJunkLines(t *testing.T) {
	events := runStream(t, strings.Join([]string{
		`: this is an SSE comment`,
		`data: {not json}`,
		``,
		`data: {"choices":[{"delta":{"content":"ok"}}]}`,
		`data: [DONE]`,
	}, "\n\n")+"\n\n")

	var text string
	for _, e := range events {
		if e.event == "content_block_delta" {
			text += e.data["delta"].(map[string]any)["text"].(string)
		}
	}
	if text != "ok" {
		t.Errorf("text = %q, want \"ok\" — junk should be skipped, not fatal", text)
	}
	if names := eventNames(events); names[len(names)-1] != "message_stop" {
		t.Errorf("stream did not terminate cleanly: %v", names)
	}
}

func TestStreamWithNoContentStillTerminates(t *testing.T) {
	// An upstream that immediately closes must still yield a well-formed
	// Anthropic turn, or the client hangs waiting for message_stop.
	events := runStream(t, "")

	want := []string{"message_start", "message_delta", "message_stop"}
	if got := eventNames(events); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("got %v, want %v", got, want)
	}
}

// brokenReader yields some bytes and then fails, standing in for an upstream
// connection that drops mid-turn.
type brokenReader struct {
	data []byte
	err  error
}

func (b *brokenReader) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, b.err
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, nil
}

func TestStreamReportsATruncatedTurnAsAnError(t *testing.T) {
	rec := httptest.NewRecorder()
	src := &brokenReader{
		data: []byte("data: {\"choices\":[{\"delta\":{\"content\":\"half an ans\"}}]}\n\n"),
		err:  io.ErrUnexpectedEOF,
	}
	streamOpenAIToAnthropic(rec, rec, src, "some/model")

	body := rec.Body.String()

	// The client must not be told the turn finished normally.
	if strings.Contains(body, "message_stop") {
		t.Error("a broken stream was finished off with message_stop")
	}
	if strings.Contains(body, "end_turn") {
		t.Error("a broken stream reported stop_reason end_turn")
	}
	if !strings.Contains(body, "event: error") {
		t.Fatalf("no error event emitted:\n%s", body)
	}

	// Blocks opened before the break still have to be closed.
	if got := strings.Count(body, "event: content_block_start"); got != 1 {
		t.Errorf("content_block_start count = %d, want 1", got)
	}
	if got := strings.Count(body, "event: content_block_stop"); got != 1 {
		t.Errorf("content_block_stop count = %d, want 1 — the open block was left dangling", got)
	}
}

func TestStreamCleanEndIsNotAnError(t *testing.T) {
	// A plain EOF with no [DONE] is how several backends close. That is a
	// normal end of turn, not a failure.
	rec := httptest.NewRecorder()
	src := &brokenReader{
		data: []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n"),
		err:  io.EOF,
	}
	streamOpenAIToAnthropic(rec, rec, src, "some/model")

	body := rec.Body.String()
	if strings.Contains(body, "event: error") {
		t.Errorf("a clean EOF was reported as an error:\n%s", body)
	}
	if !strings.Contains(body, "message_stop") {
		t.Errorf("no message_stop on a clean end:\n%s", body)
	}
}

func TestStreamReportsInputTokens(t *testing.T) {
	events := runStream(t, strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"hi"}}]}`,
		`data: {"choices":[],"usage":{"prompt_tokens":42,"completion_tokens":7}}`,
		`data: [DONE]`,
	}, "\n\n")+"\n\n")

	last := events[len(events)-2]
	if last.event != "message_delta" {
		t.Fatalf("expected message_delta, got %s", last.event)
	}
	usage := last.data["usage"].(map[string]any)
	if usage["input_tokens"] != float64(42) {
		t.Errorf("input_tokens = %v, want 42", usage["input_tokens"])
	}
	if usage["output_tokens"] != float64(7) {
		t.Errorf("output_tokens = %v, want 7", usage["output_tokens"])
	}
}

func TestStreamMessageStartShape(t *testing.T) {
	events := runStream(t, "data: [DONE]\n\n")

	msg, ok := events[0].data["message"].(map[string]any)
	if !ok {
		t.Fatalf("message_start = %v", events[0].data)
	}
	if msg["type"] != "message" || msg["role"] != "assistant" || msg["model"] != "some/model" {
		t.Errorf("message = %v", msg)
	}
	if !strings.HasPrefix(msg["id"].(string), "msg_") {
		t.Errorf("id = %v", msg["id"])
	}
	if content, ok := msg["content"].([]any); !ok || len(content) != 0 {
		t.Errorf("content = %v, want []", msg["content"])
	}
}

// ── Misc ────────────────────────────────────────────────────────────

func TestWantsStream(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		accept string
		want   bool
	}{
		{"stream flag", `{"stream":true}`, "", true},
		{"no flag", `{"stream":false}`, "", false},
		{"absent flag", `{"model":"m"}`, "", false},
		{"accept header", `{}`, "text/event-stream", true},
		{"accept header with params", `{}`, "text/event-stream; charset=utf-8", true},
		{"empty body", ``, "", false},
		{"unparseable body", `not json`, "", false},
		{"header wins over body", `{"stream":false}`, "text/event-stream", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			if tc.accept != "" {
				h.Set("Accept", tc.accept)
			}
			if got := wantsStream([]byte(tc.body), h); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGenIDIsUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := genID("msg_")
		if seen[id] {
			t.Fatalf("duplicate id %q at iteration %d", id, i)
		}
		seen[id] = true
	}
}
