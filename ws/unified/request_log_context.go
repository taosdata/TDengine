package unified

import (
	"encoding/binary"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/taosdata/driver-go/v3/ws/client"
	"github.com/taosdata/driver-go/v3/ws/unified/proto"
)

const (
	redactedLogValue     = "***"
	maxSanitizedLogLen   = 2048
	maxTextPreviewLogLen = 512
)

var sensitiveTextValuePattern = regexp.MustCompile(`(?i)\b(password|passwd|pass|token|access_token|refresh_token|bearer_token|bearertoken|authorization|secret|totp|totpcode|otp)\b(\s*[:=]\s*)(\"[^\"]*\"|'[^']*'|` + "`[^`]*`" + `|[^,\s;]+)`)

func buildRequestTimeoutMessage(scope string, action string, reqID uint64, args []byte) string {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		scope = "request"
	}
	return fmt.Sprintf("%s message timeout %s", scope, buildTextRequestSummary(action, reqID, args))
}

func buildTextRequestSummary(action string, reqID uint64, args []byte) string {
	action = strings.TrimSpace(action)
	if action == "" {
		action = "unknown"
	}
	return fmt.Sprintf("action=%s req_id=%d args=%s", action, reqID, sanitizeJSONArgsForLog(args))
}

func buildBinaryQueryRequestSummary(reqID uint64, sql string) string {
	preview := truncateForLog(sanitizeFreeTextForLog(sql), maxTextPreviewLogLen)
	return fmt.Sprintf("binary_action=query req_id=%d sql_len=%d sql=%q", reqID, len(sql), preview)
}

func buildStmtBinaryRequestSummary(reqID uint64, payload []byte) string {
	if len(payload) < 26 {
		return fmt.Sprintf("binary_action=unknown req_id=%d payload_len=%d", reqID, len(payload))
	}
	stmtOrResultID := binary.LittleEndian.Uint64(payload[8:16])
	actionCode := binary.LittleEndian.Uint64(payload[16:24])
	version := binary.LittleEndian.Uint16(payload[24:26])
	switch actionCode {
	case proto.Stmt2BindMessage:
		colIndex := uint32(0)
		if len(payload) >= 30 {
			colIndex = binary.LittleEndian.Uint32(payload[26:30])
		}
		colIndexText := strconv.FormatUint(uint64(colIndex), 10)
		if colIndex == math.MaxUint32 {
			colIndexText = "all"
		}
		return fmt.Sprintf("binary_action=stmt2_bind req_id=%d stmt_id=%d version=%d col_index=%s payload_len=%d", reqID, stmtOrResultID, version, colIndexText, len(payload))
	default:
		return fmt.Sprintf("binary_action=%d req_id=%d id=%d version=%d payload_len=%d", actionCode, reqID, stmtOrResultID, version, len(payload))
	}
}

func buildFetchRawBlockRequestSummary(reqID uint64, resultID uint64) string {
	return fmt.Sprintf("binary_action=fetch_raw_block req_id=%d result_id=%d", reqID, resultID)
}

func sanitizeJSONArgsForLog(args []byte) string {
	if len(args) == 0 {
		return "{}"
	}
	var decoded interface{}
	if err := client.JsonI.Unmarshal(args, &decoded); err != nil {
		return fmt.Sprintf("<invalid_json len=%d>", len(args))
	}
	redactValueForLog(decoded)
	sanitized, err := client.JsonI.Marshal(decoded)
	if err != nil {
		return fmt.Sprintf("<redact_failed len=%d>", len(args))
	}
	if len(sanitized) > maxSanitizedLogLen {
		return string(sanitized[:maxSanitizedLogLen]) + "...(truncated)"
	}
	return string(sanitized)
}

func sanitizeFreeTextForLog(text string) string {
	if strings.TrimSpace(text) == "" {
		return text
	}
	return sensitiveTextValuePattern.ReplaceAllString(text, `$1$2`+redactedLogValue)
}

func truncateForLog(text string, maxLen int) string {
	if maxLen <= 0 || len(text) <= maxLen {
		return text
	}
	return text[:maxLen] + "...(truncated)"
}

func redactValueForLog(value interface{}) {
	switch v := value.(type) {
	case map[string]interface{}:
		for key, item := range v {
			if isSensitiveLogKey(key) {
				v[key] = redactLogValue(item)
				continue
			}
			if text, ok := item.(string); ok {
				if sanitized, ok := redactURLStringForLog(text); ok {
					v[key] = sanitized
					continue
				}
				v[key] = sanitizeFreeTextForLog(text)
				continue
			}
			redactValueForLog(item)
		}
	case []interface{}:
		for i := 0; i < len(v); i++ {
			if text, ok := v[i].(string); ok {
				if sanitized, ok := redactURLStringForLog(text); ok {
					v[i] = sanitized
					continue
				}
				v[i] = sanitizeFreeTextForLog(text)
				continue
			}
			redactValueForLog(v[i])
		}
	}
}

func redactLogValue(value interface{}) interface{} {
	if value == nil {
		return nil
	}
	text, ok := value.(string)
	if !ok {
		return redactedLogValue
	}
	if text == "" {
		return ""
	}
	if sanitized, ok := redactURLStringForLog(text); ok {
		return sanitized
	}
	return redactedLogValue
}

func redactURLStringForLog(raw string) (string, bool) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", false
	}
	changed := false
	if u.User != nil {
		username := u.User.Username()
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(username, redactedLogValue)
			changed = true
		}
	}
	query := u.Query()
	for key, values := range query {
		if !isSensitiveLogKey(key) {
			continue
		}
		for i := 0; i < len(values); i++ {
			values[i] = redactedLogValue
		}
		query[key] = values
		changed = true
	}
	if changed {
		u.RawQuery = query.Encode()
	}
	return u.String(), true
}

func isSensitiveLogKey(key string) bool {
	normalized := strings.ToLower(strings.TrimSpace(key))
	switch normalized {
	case "password", "passwd", "pass", "token", "access_token", "refresh_token",
		"bearer_token", "bearertoken", "authorization", "td.connect.pass", "td.connect.token",
		"totp", "totpcode", "otp", "otpcode":
		return true
	}
	containsOTPToken := containsDelimitedLogToken(normalized, "otp")
	return strings.Contains(normalized, "password") ||
		strings.Contains(normalized, "passwd") ||
		strings.Contains(normalized, "token") ||
		strings.Contains(normalized, "authorization") ||
		strings.Contains(normalized, "secret") ||
		containsOTPToken
}

func containsDelimitedLogToken(key string, token string) bool {
	if key == "" || token == "" {
		return false
	}
	start := -1
	for i := 0; i < len(key); i++ {
		if isLogTokenChar(key[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 && key[start:i] == token {
			return true
		}
		start = -1
	}
	return start >= 0 && key[start:] == token
}

func isLogTokenChar(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9')
}
