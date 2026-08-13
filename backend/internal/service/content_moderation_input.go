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
	document := auditinput.ParseForTextAudit(protocol, body)
	if document == nil || !document.Complete {
		return ""
	}
	return normalizeContentModerationText(document.NormalizedText)
}

func strictLastChatUserText(messages gjson.Result) string {
	if !messages.IsArray() {
		return ""
	}
	items := messages.Array()
	if len(items) == 0 {
		return ""
	}
	message := items[len(items)-1]
	if !message.IsObject() || !strings.EqualFold(strings.TrimSpace(message.Get("role").String()), "user") {
		return ""
	}
	return strings.Join(strictTextContent(message.Get("content")), "\n")
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
	lastBusinessIndex := -1
	for index := len(items) - 1; index >= 0; index-- {
		if strictResponsesTransparentControl(items[index]) {
			continue
		}
		lastBusinessIndex = index
		break
	}
	if lastBusinessIndex < 0 {
		return ""
	}

	text, kind := strictResponsesUserItem(items[lastBusinessIndex])
	switch kind {
	case strictResponsesUserExplicit:
		return text
	case strictResponsesUserImplicit:
		implicitParts := make([]string, 0, 2)
		for index := lastBusinessIndex; index >= 0; index-- {
			if strictResponsesTransparentControl(items[index]) {
				continue
			}
			itemText, itemKind := strictResponsesUserItem(items[index])
			if itemKind != strictResponsesUserImplicit {
				break
			}
			if itemText != "" {
				implicitParts = append(implicitParts, itemText)
			}
		}
		return strictJoinReversedText(implicitParts)
	default:
		return ""
	}
}

func strictResponsesTransparentControl(item gjson.Result) bool {
	if strictResponsesForwardSanitizerDropsInputItem(item) {
		return true
	}
	if !item.IsObject() || !strings.EqualFold(strings.TrimSpace(item.Get("type").String()), "compaction_trigger") {
		return false
	}
	role := item.Get("role")
	roleExists := role.Exists() || role.Raw != ""
	return !roleExists || (role.Type == gjson.String && strings.TrimSpace(role.String()) == "")
}

func strictResponsesForwardSanitizerDropsInputItem(item gjson.Result) bool {
	if !item.IsObject() {
		return false
	}
	if strictResponsesForwardSanitizerDropsImagePart(item) {
		return true
	}
	content := item.Get("content")
	if !content.IsArray() {
		return false
	}
	dropped, remaining := false, 0
	content.ForEach(func(_, part gjson.Result) bool {
		if strictResponsesForwardSanitizerDropsImagePart(part) {
			dropped = true
		} else {
			remaining++
		}
		return true
	})
	return dropped && remaining == 0
}

func strictResponsesForwardSanitizerDropsImagePart(part gjson.Result) bool {
	if !part.IsObject() {
		return false
	}
	typeName := part.Get("type")
	imageURL := part.Get("image_url")
	return typeName.Type == gjson.String && imageURL.Type == gjson.String &&
		strings.TrimSpace(typeName.String()) == "input_image" && auditinput.IsEmptyBase64DataURI(imageURL.String())
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
	roleResult := item.Get("role")
	if (roleResult.Exists() || roleResult.Raw != "") && roleResult.Type != gjson.String {
		return "", strictResponsesUserNone
	}
	role := strings.ToLower(strings.TrimSpace(roleResult.String()))
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
		addStrictModerationText(parts, value.String())
	case value.IsArray():
		value.ForEach(func(_, item gjson.Result) bool {
			strictCollectTextContent(item, parts)
			return true
		})
	case value.IsObject():
		typeName := strings.ToLower(strings.TrimSpace(value.Get("type").String()))
		switch typeName {
		case "text", "input_text", "output_text", "refusal", "summary_text", "reasoning_text":
			addStrictModerationText(parts, value.Get("text").String())
		case "":
			if value.Get("text").Exists() {
				addStrictModerationText(parts, value.Get("text").String())
			} else {
				strictCollectTextContent(value.Get("content"), parts)
			}
		}
	}
}

func addStrictModerationText(parts *[]string, text string) {
	text = strings.TrimSpace(text)
	if text != "" {
		*parts = append(*parts, text)
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
