# 原生工具调用：三协议链路、投影模式与验证

本文说明 `/v1/messages`、`/v1/chat/completions`、`/v1/responses` 三个入口的工具调用如何经同一套中间层投影到 Devin `GetChatMessage`，`devin.tool_projection` 两种形态的差别，以及不依赖上游额度的本地验证方式。上游字段实测结论见 `upstream-protocol.md`「工具调用契约」与 `ChatToolDefinition` 字段表。

## 链路总览

三个入口只在两端各有一层编解码，中间的投影、配对、响应事件全部共用：

```text
客户端 JSON/SSE
  ├─ /v1/messages          internal/api/anthropic/messages  DecodeRequest
  ├─ /v1/chat/completions  internal/api/openai/chat         DecodeRequest
  └─ /v1/responses         internal/api/openai/responses    DecodeRequest
        │  （tool_choice 解析 common/toolchoice.go；custom 声明 common/customtool.go）
        ▼
llm.RequestMessages（internal/llm/request.go）
  Tools []ToolDefinition{Name, Description, InputSchema, Strict, ReadOnlyHint, ServerName, AttributionFieldNames, Custom, Server}
  ToolChoice{auto|none|required|named}，DisableParallelToolCalls
  Messages: AssistantMessage{Thinking/Text/ToolCall{ID,Name,Arguments,Custom}}，ToolResultMessage{ToolCallID,IsError}
        │  MergeAdjacentAssistantTurns / DemoteOrphanToolResults / Validate（三入口同调）
        ▼
Devin wire（internal/adapter/devin/request_encoder.go buildRequestSeeded）
  tools[] ← convertToolDefinition（tool_definition.go，按 devin.tool_projection）
  tool_choice.option_name / tool_name，disable_parallel_tool_calls
  chat_message_prompts[]：助手回合单条 prompt 携带 thinking+signature+toolCalls；
  TOOL 结果经 pairToolCallsWithResults 重排到各自 call 之后
        ▼
上游帧 → responseDecoder（response_decoder.go）→ llm.ResponseEvent
  start / thinking_* / signature / text_* / toolcall_start|delta|end / done(StopReason) / error
        ▼
各入口 StreamEncoder / EncodeFinal → 客户端 JSON/SSE
```

## 字段映射

| 语义                | Anthropic Messages                                     | Chat Completions                                                        | Responses                                                                   | 中间层 / wire                                                       |
| ------------------- | ------------------------------------------------------ | ----------------------------------------------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| 工具声明            | `tools[].{name,description,input_schema}`              | `tools[].function.{name,description,parameters}`                        | `tools[].{name,description,parameters}`                                     | `ToolDefinition` → `tools[].{name,description,json_schema_string}`  |
| strict              | `tools[].strict`                                       | `tools[].function.strict`                                               | `tools[].strict`                                                            | `Strict` → `tools[].strict`                                         |
| 只读提示            | `tools[].annotations.readOnlyHint`                     | —                                                                       | —                                                                           | `ReadOnlyHint` → `tools[].read_only_hint`                           |
| freeform 工具       | —                                                      | `{"type":"custom","custom":{name,description,format}}`                  | `{"type":"custom",name,format}`                                             | 包装成单 `input` 字符串参数 function，grammar 追加进说明            |
| namespace 工具      | —                                                      | —                                                                       | `{"type":"namespace"}` 子工具                                               | 展平为 `{ns}__{sub}`，响应侧还原                                    |
| tool_choice auto    | `{"type":"auto"}`                                      | `"auto"`                                                                | `"auto"`                                                                    | 不发（上游缺省）                                                    |
| tool_choice 必调    | `{"type":"any"}`                                       | `"required"`                                                            | `"required"`                                                                | `option_name=required`（上游不收 `any`）                            |
| tool_choice 禁用    | `{"type":"none"}`                                      | `"none"`                                                                | `"none"`                                                                    | `option_name=none`，且 tools 不上 wire                              |
| tool_choice 指名    | `{"type":"tool","name":X}`                             | `{"type":"function","function":{"name":X}}` / `{"type":"custom",...}`   | `{"type":"function","name":X}` / custom / namespace                         | `tool_name=X`（不在 tools 表本地 400）                              |
| 禁并行              | `tool_choice.disable_parallel_tool_use`                | `parallel_tool_calls:false`                                             | `parallel_tool_calls:false`                                                 | `disable_parallel_tool_calls`（上游接受但不执行）                   |
| 历史调用            | `tool_use{id,name,input}`                              | `tool_calls[{id,type:function,function}]` / `type:custom`               | `function_call{call_id}` / `custom_tool_call{input}`                        | `ToolCall` → `toolCalls[{id,name,argumentsJson}]` / `invalidJsonStr` |
| 历史结果            | `tool_result{tool_use_id,content,is_error}`            | `role:tool{tool_call_id}`                                               | `function_call_output` / `custom_tool_call_output`                          | `ToolResultMessage` → TOOL prompt `{toolCallId,toolResultIsError}`  |
| 响应调用            | `tool_use` 块 + `input_json_delta`                     | `tool_calls` / `delta.tool_calls[].function.arguments`；custom 为 `custom.input` | `function_call` / `custom_tool_call` item + `*_arguments.delta` / `custom_tool_call_input.delta` | `toolcall_start/delta/end` 事件                                     |
| 收尾                | `stop_reason:"tool_use"`                               | `finish_reason:"tool_calls"`                                            | `status:"completed"`                                                        | 上游 `FUNCTION_CALL` → `StopReasonToolUse`                          |

调用 id 一律原样回放：响应侧下发的就是上游 id（形态按模型分，如 `read_file_0` / `chatcmpl-tool-<hex>`），客户端下一轮带回后作为 wire `toolCalls[].id` 与 TOOL 结果的 `toolCallId` 上行。thinking 与签名在同一助手回合里排在调用之前（wire 单条 prompt 同体携带），签名按 provider 体制配对回放（见 `upstream-protocol.md`「签名体制」）。

## `devin.tool_projection`

| 值                 | tools[].description           | json_schema_string                                                                     | system prompt                                         |
| ------------------ | ----------------------------- | -------------------------------------------------------------------------------------- | ----------------------------------------------------- |
| `native`（默认）   | 客户端原始说明（空则用工具名） | 原始 schema；只展开本地 `$ref`（剥 `$defs`/`definitions`/`$schema`）与包裹裸属性 map    | 原样，不追加工具段                                    |
| `compat`           | 工具名                        | 剥 `description`/`title`/`$comment`/`x-*` 注解后同样归一；条件必填 prose 合成 `anyOf`   | 尾部追加 `# tools descriptions` 段（说明 + 字段摘要） |

两种形态共有的归一化都有上游实测依据（`upstream-protocol.md`）并计入 `repairs`：本地 `$ref` 解不开或循环时剥键计 `schema_ref_dropped`；同名工具声明去重保首个计 `dropped_duplicate_tools`；工具名字符集 `[A-Za-z0-9_-]` 在入口校验，不合法直接 400 而不是静默改名。

`native` 与真实 Devin CLI 抓包形态一致（CLI 在 `tools[].description` 发完整说明），也是唯一不把工具定义重复写进 system 的形态。`compat` 是本项目早期为避开上游指纹分类面做的形态，保留为回退：若上游对带原生说明/注解的请求出现确定性拒绝（`invalid_argument`/`unknown`，或 `There is an issue with this request` 一类模板文案），在 `config.yaml` 设：

```yaml
devin:
  tool_projection: compat
```

然后 `POST /admin/config/reload`（或重启）。该键热应用，`applied` 名单里会出现 `devin.tool_projection`；拼写错误在加载期 422，不会静默落回默认。没有做按错误自动切换：同一请求换形态重发会再吃一次上游额度，且拒绝文案已 mask 化（见 `upstream-protocol.md` 2026-09-24 复测），自动判别会把真实的历史形状错误误判成投影问题。排障时对比同一请求在两种形态下的 `03-devin-request.json` 即可定位。

## Devin 上游限制（与投影无关，两种形态都成立）

- `is_custom_tool` + grammar 声明通道确定性 `unknown`：custom 工具只能走包装 function，响应侧按声明名解包。
- `tool_choice` 不收 `any`；swe-1-7（非 lightning）拒绝 `required` 与指名。
- `disable_parallel_tool_calls` 不生效，模型照样同轮多调用。
- 调用与结果必须紧邻配对；无 call 的孤儿结果 `invalid_argument`（入口统一降级为 user 文本）。
- 历史图片只能挂当前轮；工具结果为空时补 `[tool result]` 占位。

## 本地验证

不依赖上游额度的验证都在仓库测试里：

```bash
# 三协议 × 流式/非流式端到端：真实路由 + 真实 adapter + 进程内 Connect 上游桩，
# 断言 wire 工具字段与客户端协议字段
go test ./internal/app -run TestToolsE2E -v

# wire 形态钉版：native_* 是默认投影，其余用例钉 compat
go test ./internal/adapter/devin -run TestWireGolden
UPDATE_GOLDEN=1 go test ./internal/adapter/devin -run TestWireGolden   # 有意改形态后重生成
```

对运行中的实例（`debug.enabled: true`）用 curl 打一次带工具的请求，再读该请求的 `03-devin-request.json` 看 `tools[]` 与 `prompt`。端口与令牌按本机配置替换：

```bash
curl -sS localhost:<port>/v1/chat/completions -H 'Authorization: Bearer <token>' -H 'Content-Type: application/json' -d '{
  "model": "<model>",
  "messages": [{"role": "user", "content": "read main.go"}],
  "tools": [{"type": "function", "function": {"name": "read_file", "description": "Read a file",
    "parameters": {"type": "object", "properties": {"path": {"type": "string", "description": "path"}}, "required": ["path"]}}}],
  "tool_choice": "required"
}' -D - | grep -i x-request-id
# 用 X-Request-Id 查 logs 主键后读 wire：
curl -sS localhost:<port>/admin/debug-logs/<id>/file/03-devin-request.json?raw=1 -H 'Authorization: Bearer <dashboard.password>'
```

上游额度耗尽（weekly quota exhausted）时真实请求拿不到 200，不能用它判断工具投影是否正确；以上桩测试与 `03-devin-request.json` 的 wire 内容才是投影层的依据。
