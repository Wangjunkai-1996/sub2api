package service

import (
	"bytes"
	"errors"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/tidwall/gjson"
)

var ErrOpenAIRecoveryStructuredOutput = errors.New("OpenAI recovery structured output failed validation")

func OpenAIRecoveryStructuredFormat(request []byte, mode OpenAIRecoveryWireFormat) string {
	path := "text.format.type"
	if mode == OpenAIRecoveryChatSSE || mode == OpenAIRecoveryChatJSON {
		path = "response_format.type"
	}
	format := gjson.GetBytes(request, path).String()
	if format == "" && (mode == OpenAIRecoveryResponsesSSE || mode == OpenAIRecoveryResponsesJSON) {
		format = gjson.GetBytes(request, "response_format.type").String()
	}
	if format == "json_object" || format == "json_schema" {
		return format
	}
	return ""
}

// ValidateOpenAIRecoveryStructuredOutput applies only to an entire structured
// response unit, never ordinary streamed text. It does not load schemas from
// the network or filesystem and does not include output text in its errors.
func ValidateOpenAIRecoveryStructuredOutput(request []byte, mode OpenAIRecoveryWireFormat, text string) error {
	format := OpenAIRecoveryStructuredFormat(request, mode)
	if format == "" {
		return nil
	}
	if len(text) > openAIRecoveryMaxPendingBytes {
		return ErrOpenAIRecoveryStructuredOutput
	}
	instance, err := jsonschema.UnmarshalJSON(strings.NewReader(text))
	if err != nil {
		return ErrOpenAIRecoveryStructuredOutput
	}
	if format == "json_object" {
		if _, ok := instance.(map[string]any); !ok {
			return ErrOpenAIRecoveryStructuredOutput
		}
		return nil
	}
	path := "text.format.schema"
	if mode == OpenAIRecoveryChatSSE || mode == OpenAIRecoveryChatJSON || gjson.GetBytes(request, "text.format.type").String() == "" {
		path = "response_format.json_schema.schema"
	}
	schemaRaw := gjson.GetBytes(request, path).Raw
	if schemaRaw == "" && path == "text.format.schema" {
		schemaRaw = gjson.GetBytes(request, "text.format.json_schema.schema").Raw
	}
	return validateOpenAIRecoverySchema(schemaRaw, instance)
}

// ValidateOpenAIRecoveryToolArguments validates executable function calls
// against the exact declaration supplied by this request. Custom/freeform
// tools have a different completion contract and must not call this function.
func ValidateOpenAIRecoveryToolArguments(request []byte, name, arguments string) error {
	if name == "" || len(arguments) > openAIRecoveryMaxPendingBytes {
		return ErrOpenAIRecoveryStructuredOutput
	}
	instance, err := jsonschema.UnmarshalJSON(strings.NewReader(arguments))
	if err != nil {
		return ErrOpenAIRecoveryStructuredOutput
	}
	if _, object := instance.(map[string]any); !object {
		return ErrOpenAIRecoveryStructuredOutput
	}
	schemaRaw := ""
	matched := 0
	for _, tool := range gjson.GetBytes(request, "tools").Array() {
		if tool.Get("type").String() != "function" {
			continue
		}
		declaration := tool
		if tool.Get("function").Exists() {
			declaration = tool.Get("function")
		}
		if declaration.Get("name").String() == name {
			matched++
			schemaRaw = declaration.Get("parameters").Raw
		}
	}
	for _, declaration := range gjson.GetBytes(request, "functions").Array() {
		if declaration.Get("name").String() == name {
			matched++
			schemaRaw = declaration.Get("parameters").Raw
		}
	}
	if matched != 1 {
		return ErrOpenAIRecoveryStructuredOutput
	}
	if schemaRaw == "" {
		schemaRaw = `{"type":"object"}`
	}
	return validateOpenAIRecoverySchema(schemaRaw, instance)
}

func validateOpenAIRecoverySchema(schemaRaw string, instance any) error {
	if len(schemaRaw) == 0 || len(schemaRaw) > 256<<10 {
		return ErrOpenAIRecoveryStructuredOutput
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewBufferString(schemaRaw))
	if err != nil {
		return ErrOpenAIRecoveryStructuredOutput
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(jsonschema.SchemeURLLoader{})
	compiler.AssertFormat()
	const location = "https://recovery.invalid/output.schema.json"
	if compiler.AddResource(location, document) != nil {
		return ErrOpenAIRecoveryStructuredOutput
	}
	schema, err := compiler.Compile(location)
	if err != nil || schema.Validate(instance) != nil {
		return ErrOpenAIRecoveryStructuredOutput
	}
	return nil
}
