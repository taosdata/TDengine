package unified

import (
	"github.com/gorilla/websocket"
	tLog "github.com/taosdata/driver-go/v3/log"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func packetTypeName(messageType int) string {
	switch messageType {
	case websocket.TextMessage:
		return "text"
	case websocket.BinaryMessage:
		return "binary"
	default:
		return "unknown"
	}
}

func packetContentForLog(messageType int, payload []byte) string {
	maxBytes := tLog.GetMaxPacketLogBytes()
	if messageType == websocket.TextMessage {
		sanitized := sanitizeTextPacketForLog(payload)
		return tLog.TruncateText(sanitized, maxBytes)
	}
	return tLog.HexPreview(payload, maxBytes)
}

func sanitizeTextPacketForLog(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	var decoded interface{}
	if err := client.JsonI.Unmarshal(payload, &decoded); err == nil {
		redactValueForLog(decoded)
		if sanitized, marshalErr := client.JsonI.Marshal(decoded); marshalErr == nil {
			return string(sanitized)
		}
	}
	return sanitizeFreeTextForLog(string(payload))
}

func sanitizeEndpointForLog(raw string) string {
	if sanitized, ok := redactURLStringForLog(raw); ok {
		return sanitized
	}
	return sanitizeFreeTextForLog(raw)
}

func shouldLogPacketInfo() bool {
	return tLog.IsPacketLoggingEnabled() && tLog.IsInfoEnabled()
}

func shouldLogPacketWarn() bool {
	return tLog.IsPacketLoggingEnabled() && isLogLevelEnabled(tLog.LogLevelWarn)
}

func isLogLevelEnabled(level tLog.LogLevel) bool {
	return int32(level) >= int32(tLog.GetLevel())
}
