package service

import (
	"crypto/rand"
	"math/big"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/tidwall/gjson"
)

func ExtractContentModerationText(protocol string, body []byte) string {
	return ExtractContentModerationInput(protocol, body).Text
}

func ExtractContentModerationDocument(protocol string, body []byte) *auditinput.Document {
	return auditinput.ParseForTextAudit(protocol, body)
}

func ExtractContentModerationInput(protocol string, body []byte) ContentModerationInput {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return ContentModerationInput{}
	}
	var parts []string
	var images []string
	switch protocol {
	case ContentModerationProtocolAnthropicMessages:
		collectLastAnthropicUserMessage(gjson.GetBytes(body, "messages"), &parts, &images)
	case ContentModerationProtocolOpenAIChat:
		collectLastRoleMessage(gjson.GetBytes(body, "messages"), "user", &parts, &images)
	case ContentModerationProtocolOpenAIResponses:
		collectLastResponsesInput(gjson.GetBytes(body, "input"), &parts, &images)
	case ContentModerationProtocolGemini:
		collectLastGeminiContent(gjson.GetBytes(body, "contents"), &parts, &images)
	case ContentModerationProtocolOpenAIImages:
		addModerationText(&parts, gjson.GetBytes(body, "prompt").String())
		collectContentValue(gjson.GetBytes(body, "images"), &parts, &images)
	default:
		collectLastResponsesInput(gjson.GetBytes(body, "input"), &parts, &images)
		collectLastRoleMessage(gjson.GetBytes(body, "messages"), "user", &parts, &images)
		collectLastGeminiContent(gjson.GetBytes(body, "contents"), &parts, &images)
	}
	out := ContentModerationInput{
		Text:   normalizeContentModerationText(strings.Join(parts, "\n")),
		Images: normalizeModerationImages(images),
	}
	out.Normalize()
	return out
}

// ExtractStrictCurrentUserText returns only the latest user-authored text in
// the current OpenAI request. It deliberately never reads or retains image
// payload values; image presence and structural completeness are handled by
// auditinput.ParseForTextAudit before strict admission reaches this helper.
func ExtractStrictCurrentUserText(protocol string, body []byte) string {
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return ""
	}
	var text string
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case ContentModerationProtocolOpenAIChat:
		text = strictLastChatUserText(gjson.GetBytes(body, "messages"))
	case ContentModerationProtocolOpenAIResponses:
		text = strictLastResponsesUserText(gjson.GetBytes(body, "input"))
	default:
		return ""
	}
	return trimRunes(normalizeContentModerationText(text), maxStrictModerationTextRunes)
}

func strictLastChatUserText(messages gjson.Result) string {
	if !messages.IsArray() {
		return ""
	}
	items := messages.Array()
	for index := len(items) - 1; index >= 0; index-- {
		message := items[index]
		if strings.EqualFold(strings.TrimSpace(message.Get("role").String()), "user") {
			return strings.Join(strictTextContent(message.Get("content")), "\n")
		}
	}
	return ""
}

func strictLastResponsesUserText(input gjson.Result) string {
	switch {
	case !input.Exists():
		return ""
	case input.Type == gjson.String:
		return input.String()
	case input.IsObject():
		text, _ := strictResponsesUserItem(input)
		return text
	case !input.IsArray():
		return ""
	}

	items := input.Array()
	implicitParts := make([]string, 0, 2)
	foundImplicit := false
	for index := len(items) - 1; index >= 0; index-- {
		text, kind := strictResponsesUserItem(items[index])
		switch kind {
		case strictResponsesUserExplicit:
			if foundImplicit {
				return strictJoinReversedText(implicitParts)
			}
			return text
		case strictResponsesUserImplicit:
			foundImplicit = true
			if text != "" {
				implicitParts = append(implicitParts, text)
			}
		default:
			if foundImplicit {
				return strictJoinReversedText(implicitParts)
			}
		}
	}
	return strictJoinReversedText(implicitParts)
}

func strictJoinReversedText(parts []string) string {
	for left, right := 0, len(parts)-1; left < right; left, right = left+1, right-1 {
		parts[left], parts[right] = parts[right], parts[left]
	}
	return strings.Join(parts, "\n")
}

type strictResponsesUserKind int

const (
	strictResponsesUserNone strictResponsesUserKind = iota
	strictResponsesUserExplicit
	strictResponsesUserImplicit
)

func strictResponsesUserItem(item gjson.Result) (string, strictResponsesUserKind) {
	if item.Type == gjson.String {
		return item.String(), strictResponsesUserImplicit
	}
	if !item.IsObject() {
		return "", strictResponsesUserNone
	}
	role := strings.ToLower(strings.TrimSpace(item.Get("role").String()))
	typeName := strings.ToLower(strings.TrimSpace(item.Get("type").String()))
	if role != "" && role != "user" {
		return "", strictResponsesUserNone
	}
	if role == "user" {
		return strictResponsesItemText(item, typeName), strictResponsesUserExplicit
	}
	switch typeName {
	case "input_text", "text":
		return strings.Join(strictTextContent(item), "\n"), strictResponsesUserImplicit
	case "input_image", "image_url", "image":
		return "", strictResponsesUserImplicit
	default:
		return "", strictResponsesUserNone
	}
}

func strictResponsesItemText(item gjson.Result, typeName string) string {
	switch typeName {
	case "input_text", "text":
		return strings.Join(strictTextContent(item), "\n")
	case "message", "":
		// The parser accepts exactly one of content, text, or refusal for a
		// message item. Mirror that contract so an earlier image cannot make an
		// accepted top-level text payload look like an image-only request.
		for _, field := range []string{"content", "text", "refusal"} {
			value := item.Get(field)
			if value.Exists() {
				return strings.Join(strictTextContent(value), "\n")
			}
		}
		return ""
	default:
		return ""
	}
}

func strictTextContent(value gjson.Result) []string {
	var parts []string
	strictCollectTextContent(value, &parts)
	return parts
}

func strictCollectTextContent(value gjson.Result, parts *[]string) {
	switch {
	case !value.Exists():
		return
	case value.Type == gjson.String:
		addModerationText(parts, value.String())
	case value.IsArray():
		value.ForEach(func(_, item gjson.Result) bool {
			strictCollectTextContent(item, parts)
			return true
		})
	case value.IsObject():
		typeName := strings.ToLower(strings.TrimSpace(value.Get("type").String()))
		switch typeName {
		case "text", "input_text", "output_text", "refusal", "summary_text", "reasoning_text":
			addModerationText(parts, value.Get("text").String())
		case "":
			if value.Get("text").Exists() {
				addModerationText(parts, value.Get("text").String())
			} else {
				strictCollectTextContent(value.Get("content"), parts)
			}
		}
	}
}

func contentModerationInputFromDocument(document *auditinput.Document) ContentModerationInput {
	if document == nil {
		return ContentModerationInput{}
	}
	images := make([]string, 0, len(document.Media))
	for _, media := range document.Media {
		if media.Kind == "image" && strings.TrimSpace(media.Value) != "" {
			images = append(images, media.Value)
		}
	}
	return ContentModerationInput{
		Text:   document.NormalizedText,
		Images: normalizeModerationImages(images),
	}
}

func normalizeModerationImages(images []string) []string {
	out := make([]string, 0, len(images))
	seen := make(map[string]struct{}, len(images))
	for _, image := range images {
		image = strings.TrimSpace(image)
		if image == "" {
			continue
		}
		if _, ok := seen[image]; ok {
			continue
		}
		seen[image] = struct{}{}
		out = append(out, image)
	}
	return out
}

func limitContentModerationImages(images []string) []string {
	if len(images) <= maxContentModerationInputImages {
		return images
	}
	idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(images))))
	if err != nil {
		return images[:maxContentModerationInputImages]
	}
	return []string{images[int(idx.Int64())]}
}

func normalizeContentModerationText(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}
