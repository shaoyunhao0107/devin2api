// 本文件是三协议原生工具调用的端到端测试：HTTP 请求 → 真实 app 路由 →
// 三个 surface 的 DecodeRequest → 真实 Devin adapter（buildRequest/
// responseDecoder）→ 进程内 Connect 上游桩 → 客户端 JSON/SSE。断言同时落在
// 桩收到的 wire 字段（工具声明/tool_choice/调用-结果配对）与客户端拿到的
// 协议字段（tool_use/tool_calls/function_call、stop_reason/finish_reason）上，
// 不以 HTTP 200 为唯一依据；不依赖真实上游额度。
package app

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	devinproto "local/devinproto"
	"local/devinproto/devinprotoconnect"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"

	"github.com/WncFht/devin2api/internal/adapter/devin"
	"github.com/WncFht/devin2api/internal/config"
)

// toolUpstream 是进程内 Devin 上游桩：GetChatMessage 记录 wire 请求后按
// 脚本回帧，GetCliModelConfigs 回单模型目录。
type toolUpstream struct {
	devinprotoconnect.UnimplementedApiServerServiceHandler
	frames   []*devinproto.GetChatMessageResponse
	mu       sync.Mutex
	requests []*devinproto.GetChatMessageRequest
}

// GetChatMessage 记录请求并依次写出脚本帧。
func (stub *toolUpstream) GetChatMessage(_ context.Context, req *connect.Request[devinproto.GetChatMessageRequest], stream *connect.ServerStream[devinproto.GetChatMessageResponse]) error {
	stub.mu.Lock()
	stub.requests = append(stub.requests, req.Msg)
	stub.mu.Unlock()
	for _, frame := range stub.frames {
		if err := stream.Send(frame); err != nil {
			return err
		}
	}
	return nil
}

// GetCliModelConfigs 回固定目录。
func (stub *toolUpstream) GetCliModelConfigs(context.Context, *connect.Request[devinproto.GetCliModelConfigsRequest]) (*connect.Response[devinproto.GetCliModelConfigsResponse], error) {
	return connect.NewResponse(&devinproto.GetCliModelConfigsResponse{ClientModelConfigs: []*devinproto.ExaCodeiumCommonPb_ClientModelConfig{
		{ModelUid: proto.String("stub-model")},
	}}), nil
}

// lastRequest 返回最近一次 wire 请求。
func (stub *toolUpstream) lastRequest(t *testing.T) *devinproto.GetChatMessageRequest {
	t.Helper()
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.requests) == 0 {
		t.Fatal("upstream stub received no GetChatMessage")
	}
	return stub.requests[len(stub.requests)-1]
}

// newToolApp 起桩 + 真实 adapter（零值 ToolProjection = native）+ app 路由。
func newToolApp(t *testing.T, frames ...*devinproto.GetChatMessageResponse) (*App, *toolUpstream) {
	t.Helper()
	stub := &toolUpstream{frames: frames}
	mux := http.NewServeMux()
	path, handler := devinprotoconnect.NewApiServerServiceHandler(stub)
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	providerAdapter, err := devin.New(devin.Config{
		Model:    "stub-model",
		Identity: devin.LaneIdentity{Token: "stub-token"},
		Endpoint: devin.Endpoint{BaseURL: server.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(providerAdapter.Close)
	return New(providerAdapter, config.ServerConfig{Listen: ":0"}, nil), stub
}

// 上游帧构造器：元数据帧、工具调用帧、文本帧、思考+签名帧、终止帧。
func frameMeta() *devinproto.GetChatMessageResponse {
	return &devinproto.GetChatMessageResponse{
		MessageId: proto.String("bot-tools"),
		RequestId: proto.String("req-tools"),
		Usage:     &devinproto.ExaCodeiumCommonPb_ModelUsageStats{ModelUid: proto.String("stub-model"), InputTokens: proto.Uint64(42), OutputTokens: proto.Uint64(7)},
	}
}

func frameToolCall(id, name, arguments string) *devinproto.GetChatMessageResponse {
	return &devinproto.GetChatMessageResponse{DeltaToolCalls: []*devinproto.ExaCodeiumCommonPb_ChatToolCall{
		{Id: proto.String(id), Name: proto.String(name), ArgumentsJson: proto.String(arguments)},
	}}
}

func frameText(text string) *devinproto.GetChatMessageResponse {
	return &devinproto.GetChatMessageResponse{DeltaText: proto.String(text)}
}

func frameThinking(text, signature string) []*devinproto.GetChatMessageResponse {
	return []*devinproto.GetChatMessageResponse{
		{DeltaThinking: proto.String(text)},
		{DeltaSignature: proto.String(signature), DeltaSignatureType: proto.String("anthropic")},
	}
}

func frameStop(reason devinproto.ExaCodeiumCommonPb_StopReason) *devinproto.GetChatMessageResponse {
	return &devinproto.GetChatMessageResponse{StopReason: reason.Enum()}
}

var (
	stopFunctionCall = devinproto.ExaCodeiumCommonPb_StopReason_ExaCodeiumCommonPb_StopReason_STOP_REASON_FUNCTION_CALL
	stopPattern      = devinproto.ExaCodeiumCommonPb_StopReason_ExaCodeiumCommonPb_StopReason_STOP_REASON_STOP_PATTERN
)

// parallelReadFrames 是上游同轮并行两个 read_file 调用的回帧。
func parallelReadFrames() []*devinproto.GetChatMessageResponse {
	return []*devinproto.GetChatMessageResponse{
		frameMeta(),
		frameToolCall("read_file_0", "read_file", `{"path":"a.go"}`),
		frameToolCall("read_file_1", "read_file", `{"path":"b.go"}`),
		frameStop(stopFunctionCall),
	}
}

// answerFrames 是工具结果回传后的纯文本收尾回帧。
func answerFrames() []*devinproto.GetChatMessageResponse {
	return []*devinproto.GetChatMessageResponse{frameMeta(), frameText("both files are fine"), frameStop(stopPattern)}
}

// readFileSchema 带 enum/anyOf/required/字段说明：native 投影必须原样带上 wire。
const readFileSchema = `{"type":"object","properties":{"path":{"type":"string","description":"Workspace path"},"encoding":{"type":"string","enum":["utf8","base64"]},"limit":{"anyOf":[{"type":"integer"},{"type":"null"}]}},"required":["path"]}`

// assertNativeToolWire 校验 native 投影的 wire 工具语义：system 原样（不追加
// 说明段）、read_file 说明进 tools[].description、schema 关键字不剥。
func assertNativeToolWire(t *testing.T, wire *devinproto.GetChatMessageRequest, system string) {
	t.Helper()
	if wire.GetPrompt() != system {
		t.Fatalf("wire prompt = %q, want untouched system %q", wire.GetPrompt(), system)
	}
	var readFile *devinproto.ExaChatPb_ChatToolDefinition
	for _, tool := range wire.GetTools() {
		if tool.GetName() == "read_file" {
			readFile = tool
		}
	}
	if readFile == nil {
		t.Fatalf("read_file missing from wire tools: %v", wire.GetTools())
	}
	if readFile.GetDescription() != "Read a file from the workspace" {
		t.Fatalf("wire description = %q", readFile.GetDescription())
	}
	var schema map[string]any
	if err := json.Unmarshal([]byte(readFile.GetJsonSchemaString()), &schema); err != nil {
		t.Fatal(err)
	}
	properties := schema["properties"].(map[string]any)
	if properties["path"].(map[string]any)["description"] != "Workspace path" {
		t.Fatalf("field description stripped: %s", readFile.GetJsonSchemaString())
	}
	if len(properties["encoding"].(map[string]any)["enum"].([]any)) != 2 {
		t.Fatalf("enum lost: %s", readFile.GetJsonSchemaString())
	}
	if _, ok := properties["limit"].(map[string]any)["anyOf"]; !ok {
		t.Fatalf("anyOf lost: %s", readFile.GetJsonSchemaString())
	}
	if required := schema["required"].([]any); len(required) != 1 || required[0] != "path" {
		t.Fatalf("required lost: %s", readFile.GetJsonSchemaString())
	}
}

// assertPairedHistory 校验 follow-up 轮的 wire 历史：一条 assistant 携带
// 两个调用（id 原样回放），其后紧跟按调用序配对的两条 TOOL 结果。
func assertPairedHistory(t *testing.T, wire *devinproto.GetChatMessageRequest) {
	t.Helper()
	prompts := wire.GetChatMessagePrompts()
	callIndex := -1
	for index, prompt := range prompts {
		if len(prompt.GetToolCalls()) > 0 {
			callIndex = index
		}
	}
	if callIndex < 0 || callIndex+2 >= len(prompts) {
		t.Fatalf("wire history has no call prompt followed by two results: %v", prompts)
	}
	calls := prompts[callIndex].GetToolCalls()
	if len(calls) != 2 || calls[0].GetId() != "call_a" || calls[1].GetId() != "call_b" {
		t.Fatalf("wire calls = %v", calls)
	}
	if calls[0].GetName() != "read_file" || calls[0].GetArgumentsJson() != `{"path":"a.go"}` {
		t.Fatalf("wire call a = %v", calls[0])
	}
	for offset, wantID := range []string{"call_a", "call_b"} {
		result := prompts[callIndex+1+offset]
		if result.GetSource() != devinproto.ExaCodeiumCommonPb_ChatMessageSource_ExaCodeiumCommonPb_ChatMessageSource_CHAT_MESSAGE_SOURCE_TOOL || result.GetToolCallId() != wantID {
			t.Fatalf("wire result %d = %v, want TOOL for %s", offset, result, wantID)
		}
	}
	if !strings.HasPrefix(prompts[callIndex+1].GetPrompt(), "package a") {
		t.Fatalf("result a text = %q", prompts[callIndex+1].GetPrompt())
	}
}

// postJSON 发一个 /v1 请求并要求 200。
func postJSON(t *testing.T, application *App, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	response := httptest.NewRecorder()
	application.Router().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("%s status = %d: %s", path, response.Code, response.Body.String())
	}
	return response
}

// sseFrame 是解析后的一帧 SSE：event 名（data-only 流为空）与 JSON data。
type sseFrame struct {
	name string
	data map[string]any
}

// parseSSE 把响应体切成帧；非 JSON data（[DONE]）跳过。
func parseSSE(t *testing.T, body string) []sseFrame {
	t.Helper()
	var frames []sseFrame
	var name string
	scanner := bufio.NewScanner(strings.NewReader(body))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			var data map[string]any
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &data) == nil {
				frames = append(frames, sseFrame{name: name, data: data})
			}
			name = ""
		}
	}
	return frames
}

// field 按路径取嵌套值（map 键或数组下标串）。
func field(value any, path ...any) any {
	for _, step := range path {
		switch key := step.(type) {
		case string:
			object, ok := value.(map[string]any)
			if !ok {
				return nil
			}
			value = object[key]
		case int:
			list, ok := value.([]any)
			if !ok || key >= len(list) {
				return nil
			}
			value = list[key]
		}
	}
	return value
}

// --- Anthropic Messages ---

const anthropicTools = `"tools":[{"name":"read_file","description":"Read a file from the workspace","input_schema":` + readFileSchema + `},{"name":"bash","description":"Run a shell command","input_schema":{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}}]`

func TestToolsE2EAnthropicToolUse(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			frames := append([]*devinproto.GetChatMessageResponse{frameMeta()}, frameThinking("plan: read both", "sig-anthropic")...)
			frames = append(frames, parallelReadFrames()[1:]...)
			application, stub := newToolApp(t, frames...)
			body := `{"model":"stub-model","max_tokens":1024,"stream":` + map[bool]string{false: "false", true: "true"}[stream] + `,"system":"You are terse.",` + anthropicTools + `,"tool_choice":{"type":"any"},"messages":[{"role":"user","content":"read a.go and b.go"}]}`
			response := postJSON(t, application, "/v1/messages", body)
			wire := stub.lastRequest(t)
			assertNativeToolWire(t, wire, "You are terse.")
			if wire.GetToolChoice().GetOptionName() != "required" {
				t.Fatalf("anthropic any must map to wire required, got %v", wire.GetToolChoice())
			}
			if !stream {
				var parsed map[string]any
				if err := json.Unmarshal(response.Body.Bytes(), &parsed); err != nil {
					t.Fatal(err)
				}
				if parsed["stop_reason"] != "tool_use" {
					t.Fatalf("stop_reason = %v", parsed["stop_reason"])
				}
				content := parsed["content"].([]any)
				if field(content, 0, "type") != "thinking" || field(content, 0, "signature") != "sig-anthropic" {
					t.Fatalf("thinking block must precede tool_use with its signature: %v", content)
				}
				for index, want := range []string{"a.go", "b.go"} {
					block := content[index+1]
					if field(block, "type") != "tool_use" || field(block, "name") != "read_file" || field(block, "input", "path") != want {
						t.Fatalf("tool_use %d = %v", index, block)
					}
				}
				if field(content, 1, "id") != "read_file_0" || field(content, 2, "id") != "read_file_1" {
					t.Fatalf("tool_use ids must echo upstream ids: %v", content)
				}
				return
			}
			sse := parseSSE(t, response.Body.String())
			var order []string
			partial := map[float64]string{}
			var stopReason any
			for _, frame := range sse {
				switch frame.name {
				case "content_block_start":
					order = append(order, field(frame.data, "content_block", "type").(string))
					if field(frame.data, "content_block", "type") == "tool_use" && field(frame.data, "content_block", "name") != "read_file" {
						t.Fatalf("tool_use start = %v", frame.data)
					}
				case "content_block_delta":
					if field(frame.data, "delta", "type") == "input_json_delta" {
						partial[field(frame.data, "index").(float64)] += field(frame.data, "delta", "partial_json").(string)
					}
				case "message_delta":
					stopReason = field(frame.data, "delta", "stop_reason")
				}
			}
			if strings.Join(order, ",") != "thinking,tool_use,tool_use" {
				t.Fatalf("block order = %v", order)
			}
			if partial[1] != `{"path":"a.go"}` || partial[2] != `{"path":"b.go"}` {
				t.Fatalf("input_json_delta accumulation = %v", partial)
			}
			if stopReason != "tool_use" {
				t.Fatalf("message_delta stop_reason = %v", stopReason)
			}
			if !strings.Contains(response.Body.String(), `"signature":"sig-anthropic"`) {
				t.Fatalf("signature_delta missing: %s", response.Body.String())
			}
		})
	}
}

func TestToolsE2EAnthropicToolResultFollowUp(t *testing.T) {
	for _, stream := range []string{"false", "true"} {
		t.Run("stream="+stream, func(t *testing.T) {
			application, stub := newToolApp(t, answerFrames()...)
			body := `{"model":"stub-model","max_tokens":1024,"stream":` + stream + `,"system":"You are terse.",` + anthropicTools + `,"messages":[` +
				`{"role":"user","content":"read a.go and b.go"},` +
				`{"role":"assistant","content":[{"type":"tool_use","id":"call_a","name":"read_file","input":{"path":"a.go"}},{"type":"tool_use","id":"call_b","name":"read_file","input":{"path":"b.go"}}]},` +
				`{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_b","content":"package b"},{"type":"tool_result","tool_use_id":"call_a","content":[{"type":"text","text":"package a"}]}]}]}`
			response := postJSON(t, application, "/v1/messages", body)
			assertPairedHistory(t, stub.lastRequest(t))
			if !strings.Contains(response.Body.String(), "both files are fine") || !strings.Contains(response.Body.String(), `"end_turn"`) {
				t.Fatalf("follow-up answer = %s", response.Body.String())
			}
		})
	}
}

func TestToolsE2EAnthropicToolChoiceNoneAndNamed(t *testing.T) {
	application, stub := newToolApp(t, answerFrames()...)
	postJSON(t, application, "/v1/messages", `{"model":"stub-model","max_tokens":64,`+anthropicTools+`,"tool_choice":{"type":"none"},"messages":[{"role":"user","content":"hi"}]}`)
	wire := stub.lastRequest(t)
	if len(wire.GetTools()) != 0 || wire.GetToolChoice().GetOptionName() != "none" {
		t.Fatalf("tool_choice none: tools=%d choice=%v", len(wire.GetTools()), wire.GetToolChoice())
	}
	postJSON(t, application, "/v1/messages", `{"model":"stub-model","max_tokens":64,`+anthropicTools+`,"tool_choice":{"type":"tool","name":"bash","disable_parallel_tool_use":true},"messages":[{"role":"user","content":"hi"}]}`)
	wire = stub.lastRequest(t)
	if wire.GetToolChoice().GetToolName() != "bash" || !wire.GetDisableParallelToolCalls() || len(wire.GetTools()) != 2 {
		t.Fatalf("named tool_choice: choice=%v disable_parallel=%v tools=%d", wire.GetToolChoice(), wire.GetDisableParallelToolCalls(), len(wire.GetTools()))
	}
}

// --- OpenAI Chat Completions ---

const chatTools = `"tools":[{"type":"function","function":{"name":"read_file","description":"Read a file from the workspace","parameters":` + readFileSchema + `,"strict":true}},{"type":"custom","custom":{"name":"apply_patch","description":"Apply a patch","format":{"type":"grammar","grammar":{"syntax":"lark","definition":"start: patch"}}}}]`

func TestToolsE2EChatToolCalls(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			application, stub := newToolApp(t, parallelReadFrames()...)
			body := `{"model":"stub-model","stream":` + map[bool]string{false: "false", true: "true"}[stream] + `,"messages":[{"role":"system","content":"You are terse."},{"role":"user","content":"read a.go and b.go"}],` + chatTools + `,"tool_choice":"required"}`
			response := postJSON(t, application, "/v1/chat/completions", body)
			wire := stub.lastRequest(t)
			assertNativeToolWire(t, wire, "You are terse.")
			if wire.GetToolChoice().GetOptionName() != "required" {
				t.Fatalf("wire tool_choice = %v", wire.GetToolChoice())
			}
			for _, tool := range wire.GetTools() {
				switch tool.GetName() {
				case "read_file":
					if !tool.GetStrict() {
						t.Fatal("chat function.strict must reach the wire")
					}
				case "apply_patch":
					if !strings.Contains(tool.GetDescription(), "start: patch") || !strings.Contains(tool.GetJsonSchemaString(), `"input"`) {
						t.Fatalf("custom tool wire = %v", tool)
					}
				}
			}
			if !stream {
				var parsed map[string]any
				if err := json.Unmarshal(response.Body.Bytes(), &parsed); err != nil {
					t.Fatal(err)
				}
				if field(parsed, "choices", 0, "finish_reason") != "tool_calls" {
					t.Fatalf("finish_reason = %v", field(parsed, "choices", 0, "finish_reason"))
				}
				calls := field(parsed, "choices", 0, "message", "tool_calls").([]any)
				if len(calls) != 2 || field(calls, 1, "id") != "read_file_1" || field(calls, 1, "function", "arguments") != `{"path":"b.go"}` {
					t.Fatalf("tool_calls = %v", calls)
				}
				return
			}
			arguments := map[float64]string{}
			ids := map[float64]string{}
			var finish any
			for _, frame := range parseSSE(t, response.Body.String()) {
				for _, raw := range asList(field(frame.data, "choices", 0, "delta", "tool_calls")) {
					index := field(raw, "index").(float64)
					if id, ok := field(raw, "id").(string); ok {
						ids[index] = id
					}
					if piece, ok := field(raw, "function", "arguments").(string); ok {
						arguments[index] += piece
					}
				}
				if reason := field(frame.data, "choices", 0, "finish_reason"); reason != nil {
					finish = reason
				}
			}
			if ids[0] != "read_file_0" || ids[1] != "read_file_1" || arguments[0] != `{"path":"a.go"}` || arguments[1] != `{"path":"b.go"}` {
				t.Fatalf("stream tool_calls ids=%v args=%v", ids, arguments)
			}
			if finish != "tool_calls" {
				t.Fatalf("stream finish_reason = %v", finish)
			}
		})
	}
}

func TestToolsE2EChatCustomToolCall(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			application, _ := newToolApp(t, frameMeta(), frameToolCall("apply_patch_0", "apply_patch", `{"input":"*** Begin Patch\n*** End Patch"}`), frameStop(stopFunctionCall))
			body := `{"model":"stub-model","stream":` + map[bool]string{false: "false", true: "true"}[stream] + `,"messages":[{"role":"user","content":"patch it"}],` + chatTools + `,"tool_choice":{"type":"custom","custom":{"name":"apply_patch"}}}`
			response := postJSON(t, application, "/v1/chat/completions", body)
			if !stream {
				var parsed map[string]any
				if err := json.Unmarshal(response.Body.Bytes(), &parsed); err != nil {
					t.Fatal(err)
				}
				call := field(parsed, "choices", 0, "message", "tool_calls", 0)
				if field(call, "type") != "custom" || field(call, "custom", "name") != "apply_patch" || field(call, "custom", "input") != "*** Begin Patch\n*** End Patch" {
					t.Fatalf("custom tool call = %v", call)
				}
				return
			}
			var input, kind string
			for _, frame := range parseSSE(t, response.Body.String()) {
				for _, raw := range asList(field(frame.data, "choices", 0, "delta", "tool_calls")) {
					if value, ok := field(raw, "type").(string); ok {
						kind = value
					}
					if piece, ok := field(raw, "custom", "input").(string); ok {
						input += piece
					}
				}
			}
			if kind != "custom" || input != "*** Begin Patch\n*** End Patch" {
				t.Fatalf("stream custom call type=%q input=%q", kind, input)
			}
		})
	}
}

func TestToolsE2EChatToolMessagesFollowUp(t *testing.T) {
	for _, stream := range []string{"false", "true"} {
		t.Run("stream="+stream, func(t *testing.T) {
			application, stub := newToolApp(t, answerFrames()...)
			body := `{"model":"stub-model","stream":` + stream + `,` + chatTools + `,"messages":[` +
				`{"role":"user","content":"read a.go and b.go"},` +
				`{"role":"assistant","content":null,"tool_calls":[{"id":"call_a","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"a.go\"}"}},{"id":"call_b","type":"function","function":{"name":"read_file","arguments":"{\"path\":\"b.go\"}"}}]},` +
				`{"role":"tool","tool_call_id":"call_a","content":"package a"},` +
				`{"role":"tool","tool_call_id":"call_b","content":"package b"}]}`
			response := postJSON(t, application, "/v1/chat/completions", body)
			assertPairedHistory(t, stub.lastRequest(t))
			if !strings.Contains(response.Body.String(), "both files are fine") || !strings.Contains(response.Body.String(), `"finish_reason":"stop"`) {
				t.Fatalf("follow-up answer = %s", response.Body.String())
			}
		})
	}
}

func TestToolsE2EChatCustomHistoryReplay(t *testing.T) {
	application, stub := newToolApp(t, answerFrames()...)
	postJSON(t, application, "/v1/chat/completions", `{"model":"stub-model",`+chatTools+`,"messages":[`+
		`{"role":"user","content":"patch"},`+
		`{"role":"assistant","content":null,"tool_calls":[{"id":"call_p","type":"custom","custom":{"name":"apply_patch","input":"*** Begin Patch"}}]},`+
		`{"role":"tool","tool_call_id":"call_p","content":"applied"}]}`)
	prompts := stub.lastRequest(t).GetChatMessagePrompts()
	call := prompts[1].GetToolCalls()[0]
	if !call.GetIsCustomToolCall() || call.GetInvalidJsonStr() != "*** Begin Patch" || call.GetName() != "apply_patch" {
		t.Fatalf("custom history call = %v", call)
	}
	if prompts[2].GetToolCallId() != "call_p" {
		t.Fatalf("custom result pairing = %v", prompts[2])
	}
}

func TestToolsE2EChatToolChoiceNoneAndNamed(t *testing.T) {
	application, stub := newToolApp(t, answerFrames()...)
	postJSON(t, application, "/v1/chat/completions", `{"model":"stub-model",`+chatTools+`,"tool_choice":"none","messages":[{"role":"user","content":"hi"}]}`)
	if wire := stub.lastRequest(t); len(wire.GetTools()) != 0 || wire.GetToolChoice().GetOptionName() != "none" {
		t.Fatalf("tool_choice none: %v", wire.GetToolChoice())
	}
	postJSON(t, application, "/v1/chat/completions", `{"model":"stub-model",`+chatTools+`,"tool_choice":{"type":"function","function":{"name":"read_file"}},"parallel_tool_calls":false,"messages":[{"role":"user","content":"hi"}]}`)
	if wire := stub.lastRequest(t); wire.GetToolChoice().GetToolName() != "read_file" || !wire.GetDisableParallelToolCalls() {
		t.Fatalf("named tool_choice: %v disable_parallel=%v", wire.GetToolChoice(), wire.GetDisableParallelToolCalls())
	}
}

// --- OpenAI Responses ---

const responsesTools = `"tools":[{"type":"function","name":"read_file","description":"Read a file from the workspace","parameters":` + readFileSchema + `,"strict":false},{"type":"custom","name":"apply_patch","description":"Apply a patch","format":{"type":"grammar","syntax":"lark","definition":"start: patch"}}]`

func TestToolsE2EResponsesFunctionCall(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			application, stub := newToolApp(t, parallelReadFrames()...)
			body := `{"model":"stub-model","stream":` + map[bool]string{false: "false", true: "true"}[stream] + `,"instructions":"You are terse.",` + responsesTools + `,"tool_choice":{"type":"function","name":"read_file"},"input":[{"type":"message","role":"user","content":"read a.go and b.go"}]}`
			response := postJSON(t, application, "/v1/responses", body)
			wire := stub.lastRequest(t)
			assertNativeToolWire(t, wire, "You are terse.")
			if wire.GetToolChoice().GetToolName() != "read_file" {
				t.Fatalf("wire tool_choice = %v", wire.GetToolChoice())
			}
			var output []any
			if !stream {
				var parsed map[string]any
				if err := json.Unmarshal(response.Body.Bytes(), &parsed); err != nil {
					t.Fatal(err)
				}
				output = parsed["output"].([]any)
			} else {
				arguments := map[string]string{}
				for _, frame := range parseSSE(t, response.Body.String()) {
					switch field(frame.data, "type") {
					case "response.function_call_arguments.delta":
						arguments[field(frame.data, "item_id").(string)] += field(frame.data, "delta").(string)
					case "response.completed":
						output = field(frame.data, "response", "output").([]any)
					}
				}
				if len(arguments) != 2 {
					t.Fatalf("argument deltas per item = %v", arguments)
				}
			}
			var calls []any
			for _, item := range output {
				if field(item, "type") == "function_call" {
					calls = append(calls, item)
				}
			}
			if len(calls) != 2 || field(calls, 0, "call_id") != "read_file_0" || field(calls, 1, "arguments") != `{"path":"b.go"}` || field(calls, 0, "name") != "read_file" {
				t.Fatalf("function_call items = %v", calls)
			}
		})
	}
}

func TestToolsE2EResponsesCustomToolCall(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			application, stub := newToolApp(t, frameMeta(), frameToolCall("apply_patch_0", "apply_patch", `{"input":"*** Begin Patch\n*** End Patch"}`), frameStop(stopFunctionCall))
			body := `{"model":"stub-model","stream":` + map[bool]string{false: "false", true: "true"}[stream] + `,` + responsesTools + `,"tool_choice":"required","input":"patch it"}`
			response := postJSON(t, application, "/v1/responses", body)
			if wire := stub.lastRequest(t); wire.GetToolChoice().GetOptionName() != "required" {
				t.Fatalf("wire tool_choice = %v", wire.GetToolChoice())
			}
			text := response.Body.String()
			if !strings.Contains(text, `"type":"custom_tool_call"`) || !strings.Contains(text, `"input":"*** Begin Patch\n*** End Patch"`) {
				t.Fatalf("custom_tool_call output = %s", text)
			}
			if stream && !strings.Contains(text, "response.custom_tool_call_input.done") {
				t.Fatalf("custom input done event missing: %s", text)
			}
		})
	}
}

func TestToolsE2EResponsesFunctionCallOutputFollowUp(t *testing.T) {
	for _, stream := range []string{"false", "true"} {
		t.Run("stream="+stream, func(t *testing.T) {
			application, stub := newToolApp(t, answerFrames()...)
			body := `{"model":"stub-model","stream":` + stream + `,` + responsesTools + `,"input":[` +
				`{"type":"message","role":"user","content":"read a.go and b.go"},` +
				`{"type":"function_call","call_id":"call_a","name":"read_file","arguments":"{\"path\":\"a.go\"}"},` +
				`{"type":"function_call","call_id":"call_b","name":"read_file","arguments":"{\"path\":\"b.go\"}"},` +
				`{"type":"function_call_output","call_id":"call_b","output":"package b"},` +
				`{"type":"function_call_output","call_id":"call_a","output":"package a"}]}`
			response := postJSON(t, application, "/v1/responses", body)
			assertPairedHistory(t, stub.lastRequest(t))
			if !strings.Contains(response.Body.String(), "both files are fine") {
				t.Fatalf("follow-up answer = %s", response.Body.String())
			}
		})
	}
}

func TestToolsE2EResponsesCustomOutputFollowUp(t *testing.T) {
	application, stub := newToolApp(t, answerFrames()...)
	postJSON(t, application, "/v1/responses", `{"model":"stub-model",`+responsesTools+`,"input":[`+
		`{"type":"message","role":"user","content":"patch"},`+
		`{"type":"custom_tool_call","call_id":"call_p","name":"apply_patch","input":"*** Begin Patch"},`+
		`{"type":"custom_tool_call_output","call_id":"call_p","output":"applied"}]}`)
	prompts := stub.lastRequest(t).GetChatMessagePrompts()
	call := prompts[1].GetToolCalls()[0]
	if !call.GetIsCustomToolCall() || call.GetInvalidJsonStr() != "*** Begin Patch" || prompts[2].GetToolCallId() != "call_p" || prompts[2].GetPrompt() != "applied" {
		t.Fatalf("custom replay = %v / %v", call, prompts[2])
	}
}

// --- 无工具普通请求不被破坏 ---

func TestToolsE2EPlainTextUnaffected(t *testing.T) {
	cases := map[string]string{
		"/v1/messages":         `{"model":"stub-model","max_tokens":64,"system":"S","messages":[{"role":"user","content":"hi"}]}`,
		"/v1/chat/completions": `{"model":"stub-model","messages":[{"role":"system","content":"S"},{"role":"user","content":"hi"}]}`,
		"/v1/responses":        `{"model":"stub-model","instructions":"S","input":"hi"}`,
	}
	for path, body := range cases {
		t.Run(path, func(t *testing.T) {
			application, stub := newToolApp(t, answerFrames()...)
			response := postJSON(t, application, path, body)
			wire := stub.lastRequest(t)
			if wire.GetPrompt() != "S" || len(wire.GetTools()) != 0 || wire.GetToolChoice() != nil {
				t.Fatalf("plain wire: prompt=%q tools=%d choice=%v", wire.GetPrompt(), len(wire.GetTools()), wire.GetToolChoice())
			}
			if !strings.Contains(response.Body.String(), "both files are fine") {
				t.Fatalf("plain answer = %s", response.Body.String())
			}
		})
	}
}

// asList 把可能缺席的 JSON 数组值转成切片。
func asList(value any) []any {
	list, _ := value.([]any)
	return list
}
