// 本文件是 OpenAI 两个入口（Responses / Chat Completions）共用的 custom
// （freeform）工具声明投影。
package common

import (
	"encoding/json"

	"github.com/WncFht/devin2api/internal/llm"
)

// customToolInputSchema 把 freeform 工具包装成上游接受的 function 形态：
// 上游 is_custom_tool 声明通道实测确定性 unknown，改为声明单字符串参数的
// function，模型将原文填入 input（实测 apply_patch 补丁按此下发）；响应
// 侧 devin 解码器按声明名集合解包回原文并置 ToolCall.Custom。
var customToolInputSchema = json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"],"additionalProperties":false}`)

// CustomToolDefinition 把一条 custom 工具声明投影成中间工具定义：输入语法
// （lark/regex grammar）是模型能看到的唯一格式规范，追加进说明随
// tools[].description 上行；syntax/definition 为空（format.type=text）时
// 说明原样。
func CustomToolDefinition(name, description, syntax, definition string) llm.ToolDefinition {
	if definition != "" {
		description += "\n\nInput grammar (" + syntax + "):\n" + definition
	}
	return llm.ToolDefinition{
		Name:        name,
		Description: description,
		InputSchema: customToolInputSchema,
		Custom:      true,
	}
}
