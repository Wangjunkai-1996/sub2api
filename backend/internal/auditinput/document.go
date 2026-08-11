package auditinput

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const (
	ParserVersion = "auditinput/v2"

	MaxTextRunes  = 64 * 1024 * 1024
	MaxImages     = 4
	MaxImageBytes = 8 * 1024 * 1024

	ProtocolAnthropicMessages = "anthropic_messages"
	ProtocolOpenAIResponses   = "openai_responses"
	ProtocolOpenAIChat        = "openai_chat_completions"
	ProtocolGemini            = "gemini"
	ProtocolOpenAIImages      = "openai_images"

	IssueInvalidJSON         = "invalid_json"
	IssueDuplicateField      = "duplicate_json_field"
	IssueInvalidRoot         = "invalid_root"
	IssueUnsupportedProtocol = "unsupported_protocol"
	IssueEmptyContent        = "empty_content"
	IssueUnknownType         = "unknown_item_type"
	IssueUnknownField        = "unknown_field"
	IssueUnknownRole         = "unknown_role"
	IssueInvalidShape        = "invalid_shape"
	IssueRemoteFile          = "remote_file_uninspectable"
	IssueEncryptedContent    = "encrypted_content_uninspectable"
	IssueUnsupportedMedia    = "unsupported_media"
	IssueInvalidMedia        = "invalid_media"
	IssueTextLimit           = "text_limit_exceeded"
	IssueImageLimit          = "image_limit_exceeded"
	IssueImageSize           = "image_size_exceeded"
)

type Segment struct {
	Text       string `json:"-"`
	Normalized string `json:"normalized"`
	Folded     string `json:"folded"`
	Role       string `json:"role,omitempty"`
	Kind       string `json:"kind,omitempty"`
	Path       string `json:"path"`
}

type Media struct {
	Kind      string `json:"kind"`
	MIMEType  string `json:"mime_type"`
	Source    string `json:"source"`
	Path      string `json:"path"`
	Digest    string `json:"digest"`
	SizeBytes int    `json:"size_bytes"`
	Value     string `json:"-"`
}

// OpaqueState records only the digest of provider-generated assistant state.
// The ciphertext itself must never enter normalized audit text or diagnostics.
type OpaqueState struct {
	Kind   string `json:"kind"`
	Path   string `json:"path"`
	Digest string `json:"digest"`
}

type ControlItem struct {
	Kind string `json:"kind"`
	Path string `json:"path"`
}

type Issue struct {
	Code string `json:"code"`
	Path string `json:"path,omitempty"`
}

type Document struct {
	ParserVersion      string        `json:"parser_version"`
	Protocol           string        `json:"protocol"`
	Segments           []Segment     `json:"segments"`
	Media              []Media       `json:"media"`
	OpaqueStates       []OpaqueState `json:"opaque_states,omitempty"`
	ControlItems       []ControlItem `json:"control_items,omitempty"`
	Store              *bool         `json:"store,omitempty"`
	PreviousResponseID string        `json:"previous_response_id,omitempty"`
	NormalizedText     string        `json:"normalized_text"`
	FoldedText         string        `json:"folded_text"`
	Hash               string        `json:"hash"`
	Complete           bool          `json:"complete"`
	Truncated          bool          `json:"truncated"`
	Issues             []Issue       `json:"issues,omitempty"`
}

func (d *Document) Clone() *Document {
	if d == nil {
		return nil
	}
	clone := *d
	clone.Segments = append([]Segment(nil), d.Segments...)
	clone.Media = append([]Media(nil), d.Media...)
	clone.OpaqueStates = append([]OpaqueState(nil), d.OpaqueStates...)
	clone.ControlItems = append([]ControlItem(nil), d.ControlItems...)
	if d.Store != nil {
		store := *d.Store
		clone.Store = &store
	}
	clone.Issues = append([]Issue(nil), d.Issues...)
	return &clone
}

func (d *Document) HasIssue(code string) bool {
	if d == nil {
		return false
	}
	for _, issue := range d.Issues {
		if issue.Code == code {
			return true
		}
	}
	return false
}

func Parse(protocol string, body []byte) *Document {
	protocol = canonicalProtocol(protocol)
	b := &builder{doc: Document{ParserVersion: ParserVersion, Protocol: protocol}}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		b.issue(IssueEmptyContent, "$")
		return b.finish()
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		b.issue(IssueInvalidJSON, "$")
		return b.finish()
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		b.issue(IssueInvalidJSON, "$")
		return b.finish()
	}
	if duplicatePath, duplicate := duplicateJSONFieldPath(trimmed); duplicate {
		b.issue(IssueDuplicateField, duplicatePath)
		return b.finish()
	}
	root, ok := value.(map[string]any)
	if !ok {
		b.issue(IssueInvalidRoot, "$")
		return b.finish()
	}

	switch protocol {
	case ProtocolOpenAIResponses:
		b.parseResponses(root, "$")
	case ProtocolOpenAIChat:
		b.parseChat(root, "$")
	case ProtocolAnthropicMessages:
		b.parseAnthropic(root, "$")
	case ProtocolGemini:
		b.parseGemini(root, "$")
	case ProtocolOpenAIImages, "grok_media", "media", "images":
		b.parseMediaRequest(root, "$")
	case "openai_embeddings":
		b.parseEmbedding(root, "$")
	case "openai_alpha_search":
		b.parseSearch(root, "$")
	default:
		b.issue(IssueUnsupportedProtocol, "$")
	}
	return b.finish()
}

// ParseResponsesOutput parses the complete model-visible output array from a
// successful Responses terminal event. It intentionally reuses the request
// item parser so continuation lineage and direct request auditing share the
// same type, role, media, normalization, and size rules.
func ParseResponsesOutput(raw []byte) *Document {
	b := &builder{doc: Document{ParserVersion: ParserVersion, Protocol: ProtocolOpenAIResponses}}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		b.issue(IssueEmptyContent, "$.output")
		return b.finish()
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		b.issue(IssueInvalidJSON, "$.output")
		return b.finish()
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		b.issue(IssueInvalidJSON, "$.output")
		return b.finish()
	}
	if duplicatePath, duplicate := duplicateJSONFieldPath(trimmed); duplicate {
		b.issue(IssueDuplicateField, "$.output"+strings.TrimPrefix(duplicatePath, "$"))
		return b.finish()
	}
	items, ok := value.([]any)
	if !ok {
		b.issue(IssueInvalidShape, "$.output")
		return b.finish()
	}
	b.parseResponsesInput(items, "$.output")
	return b.finish()
}

func duplicateJSONFieldPath(raw []byte) (string, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	path, duplicate, err := scanJSONValueForDuplicateFields(decoder, "$")
	return path, duplicate && err == nil
}

// HasDuplicateJSONFields reports whether a syntactically valid JSON value
// contains the same object member name more than once at any depth. Callers
// that use a last-key-wins parser must reject such payloads before making a
// security decision because an upstream parser may select a different value.
func HasDuplicateJSONFields(raw []byte) bool {
	_, duplicate := duplicateJSONFieldPath(raw)
	return duplicate
}

func scanJSONValueForDuplicateFields(decoder *json.Decoder, path string) (string, bool, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", false, err
	}
	delim, structured := token.(json.Delim)
	if !structured {
		return "", false, nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return "", false, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return "", false, errors.New("json object key is not a string")
			}
			fieldPath := childPath(path, key)
			if _, exists := seen[key]; exists {
				return fieldPath, true, nil
			}
			seen[key] = struct{}{}
			if duplicatePath, duplicate, err := scanJSONValueForDuplicateFields(decoder, fieldPath); err != nil || duplicate {
				return duplicatePath, duplicate, err
			}
		}
		_, err = decoder.Token()
		return "", false, err
	case '[':
		index := 0
		for decoder.More() {
			itemPath := indexPath(path, index)
			if duplicatePath, duplicate, err := scanJSONValueForDuplicateFields(decoder, itemPath); err != nil || duplicate {
				return duplicatePath, duplicate, err
			}
			index++
		}
		_, err = decoder.Token()
		return "", false, err
	default:
		return "", false, errors.New("unexpected json delimiter")
	}
}

type builder struct {
	doc       Document
	textRunes int
}

func (b *builder) finish() *Document {
	if len(b.doc.Segments) == 0 && len(b.doc.Media) == 0 && len(b.doc.OpaqueStates) == 0 && len(b.doc.ControlItems) == 0 && len(b.doc.Issues) == 0 {
		b.issue(IssueEmptyContent, "$")
	}
	texts := make([]string, 0, len(b.doc.Segments))
	for _, segment := range b.doc.Segments {
		texts = append(texts, segment.Normalized)
	}
	b.doc.NormalizedText = strings.Join(texts, "\n")
	b.doc.FoldedText = foldNormalizedForMatching(b.doc.NormalizedText)
	h := sha256.New()
	_, _ = h.Write([]byte(ParserVersion))
	_, _ = h.Write([]byte("\nprotocol:" + b.doc.Protocol + "\ntext:" + b.doc.NormalizedText))
	for _, media := range b.doc.Media {
		_, _ = h.Write([]byte("\nmedia:" + media.Digest))
	}
	for _, state := range b.doc.OpaqueStates {
		_, _ = h.Write([]byte("\nopaque:" + state.Kind + ":" + state.Path + ":" + state.Digest))
	}
	for _, control := range b.doc.ControlItems {
		_, _ = h.Write([]byte("\ncontrol:" + control.Kind + ":" + control.Path))
	}
	b.doc.Hash = hex.EncodeToString(h.Sum(nil))
	b.doc.Complete = len(b.doc.Issues) == 0 && !b.doc.Truncated
	return &b.doc
}

func (b *builder) issue(code, path string) {
	for _, issue := range b.doc.Issues {
		if issue.Code == code && issue.Path == path {
			return
		}
	}
	b.doc.Issues = append(b.doc.Issues, Issue{Code: code, Path: path})
}

func (b *builder) addText(value, role, kind, path string) {
	normalized := NormalizeText(value)
	if normalized == "" {
		return
	}
	separator := 0
	if len(b.doc.Segments) > 0 {
		separator = 1
	}
	remaining := MaxTextRunes - b.textRunes - separator
	if remaining <= 0 {
		b.doc.Truncated = true
		b.issue(IssueTextLimit, path)
		return
	}
	runeCount := utf8.RuneCountInString(normalized)
	if runeCount > remaining {
		runes := []rune(normalized)
		runes = runes[:remaining]
		normalized = string(runes)
		runeCount = remaining
		b.doc.Truncated = true
		b.issue(IssueTextLimit, path)
	}
	b.doc.Segments = append(b.doc.Segments, Segment{
		Normalized: normalized,
		Role:       strings.ToLower(strings.TrimSpace(role)), Kind: kind, Path: path,
	})
	b.textRunes += separator + runeCount
}

func (b *builder) addJSON(value any, role, kind, path string) {
	if value == nil {
		return
	}
	raw, err := json.Marshal(value)
	if err != nil {
		b.issue(IssueInvalidShape, path)
		return
	}
	if string(raw) == "null" || string(raw) == "{}" || string(raw) == "[]" || string(raw) == `""` {
		return
	}
	b.addText(string(raw), role, kind, path)
}

func (b *builder) addOpaqueState(kind, value, path string) {
	digest := sha256.Sum256([]byte(value))
	b.doc.OpaqueStates = append(b.doc.OpaqueStates, OpaqueState{
		Kind: strings.ToLower(strings.TrimSpace(kind)), Path: path, Digest: hex.EncodeToString(digest[:]),
	})
}

func (b *builder) addControlItem(kind, path string) {
	b.doc.ControlItems = append(b.doc.ControlItems, ControlItem{
		Kind: strings.ToLower(strings.TrimSpace(kind)), Path: path,
	})
}

func (b *builder) addImage(value, mimeType, path string) {
	if len(b.doc.Media) >= MaxImages {
		b.issue(IssueImageLimit, path)
		return
	}
	value = strings.TrimSpace(value)
	mimeType = strings.ToLower(strings.TrimSpace(mimeType))
	if value == "" {
		b.issue(IssueInvalidMedia, path)
		return
	}
	media := Media{Kind: "image", MIMEType: mimeType, Path: path, SizeBytes: -1, Value: value}
	var digest [sha256.Size]byte
	if strings.HasPrefix(strings.ToLower(value), "data:") {
		parsedMIME, decoded, ok := decodeDataURL(value)
		if !ok || !strings.HasPrefix(parsedMIME, "image/") {
			b.issue(IssueInvalidMedia, path)
			return
		}
		if len(decoded) > MaxImageBytes {
			b.issue(IssueImageSize, path)
			return
		}
		media.MIMEType, media.Source, media.SizeBytes = parsedMIME, "data_url", len(decoded)
		digest = sha256.Sum256(decoded)
	} else if parsed, err := url.Parse(value); err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" {
		// A remote object cannot be size-verified before account selection. Strict
		// admission therefore treats it like any other opaque remote file.
		b.issue(IssueRemoteFile, path)
		return
	} else {
		decoded, err := base64.StdEncoding.DecodeString(value)
		if err != nil || !strings.HasPrefix(mimeType, "image/") {
			b.issue(IssueInvalidMedia, path)
			return
		}
		if len(decoded) > MaxImageBytes {
			b.issue(IssueImageSize, path)
			return
		}
		media.Source, media.SizeBytes = "base64", len(decoded)
		media.Value = fmt.Sprintf("data:%s;base64,%s", mimeType, value)
		digest = sha256.Sum256(decoded)
	}
	media.Digest = hex.EncodeToString(digest[:])
	b.doc.Media = append(b.doc.Media, media)
}

func (b *builder) addTextFile(value, mimeType, path string) {
	value = strings.TrimSpace(value)
	if value == "" {
		b.issue(IssueInvalidMedia, path)
		return
	}
	var decoded []byte
	if strings.HasPrefix(strings.ToLower(value), "data:") {
		parsedMIME, data, ok := decodeDataURL(value)
		if !ok {
			b.issue(IssueInvalidMedia, path)
			return
		}
		mimeType, decoded = parsedMIME, data
	} else {
		var err error
		decoded, err = base64.StdEncoding.DecodeString(value)
		if err != nil {
			b.issue(IssueInvalidMedia, path)
			return
		}
	}
	if len(decoded) > MaxImageBytes {
		b.issue(IssueImageSize, path)
		return
	}
	if !isTextMIME(mimeType) || !utf8.Valid(decoded) {
		b.issue(IssueUnsupportedMedia, path)
		return
	}
	b.addText(string(decoded), "user", "text_file", path)
}

func NormalizeText(value string) string {
	value = norm.NFKC.String(value)
	var out strings.Builder
	out.Grow(len(value))
	spacePending := false
	for _, r := range value {
		if unicode.Is(unicode.Cf, r) || unicode.Is(unicode.Cc, r) {
			if unicode.IsSpace(r) {
				spacePending = out.Len() > 0
			}
			continue
		}
		if unicode.IsSpace(r) {
			spacePending = out.Len() > 0
			continue
		}
		if spacePending {
			out.WriteByte(' ')
			spacePending = false
		}
		out.WriteRune(r)
	}
	return strings.TrimSpace(out.String())
}

func FoldForMatching(value string) string {
	return foldNormalizedForMatching(NormalizeText(value))
}

func foldNormalizedForMatching(value string) string {
	var out strings.Builder
	out.Grow(len(value))
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			out.WriteRune(r)
		}
	}
	return out.String()
}

func canonicalProtocol(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "openai_responses", "responses", "responses_websocket":
		return ProtocolOpenAIResponses
	case "openai_chat_completions", "openai_chat", "chat_completions":
		return ProtocolOpenAIChat
	case "anthropic_messages", "claude_messages", "messages":
		return ProtocolAnthropicMessages
	case "gemini", "gemini_generate_content":
		return ProtocolGemini
	case "openai_images":
		return ProtocolOpenAIImages
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func decodeDataURL(value string) (string, []byte, bool) {
	comma := strings.IndexByte(value, ',')
	if comma <= len("data:") {
		return "", nil, false
	}
	header, payload := value[len("data:"):comma], value[comma+1:]
	parts := strings.Split(header, ";")
	mimeType := strings.ToLower(strings.TrimSpace(parts[0]))
	base64Encoded := false
	for _, part := range parts[1:] {
		if strings.EqualFold(strings.TrimSpace(part), "base64") {
			base64Encoded = true
		}
	}
	if !base64Encoded {
		decoded, err := url.PathUnescape(payload)
		return mimeType, []byte(decoded), err == nil
	}
	decoded, err := base64.StdEncoding.DecodeString(payload)
	return mimeType, decoded, err == nil
}

func isTextMIME(value string) bool {
	value = strings.ToLower(strings.TrimSpace(strings.Split(value, ";")[0]))
	return strings.HasPrefix(value, "text/") || value == "application/json" || value == "application/xml" ||
		value == "application/yaml" || value == "application/x-yaml" || value == "application/javascript"
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func childPath(parent, key string) string {
	if parent == "$" {
		return "$." + key
	}
	return parent + "." + key
}

func indexPath(parent string, index int) string {
	return fmt.Sprintf("%s[%d]", parent, index)
}
