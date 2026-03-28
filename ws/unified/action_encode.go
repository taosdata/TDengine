package unified

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/taosdata/driver-go/v3/ws/client"
)

var (
	wsActionPrefix = []byte(`{"action":`)
	wsActionArgs   = []byte(`,"args":`)
	// actionJSONCache stores marshaled JSON string tokens for action names.
	// Values are immutable byte slices and safe for concurrent reads.
	actionJSONCache sync.Map // map[string][]byte
)

func getMarshaledAction(action string) ([]byte, error) {
	if cached, ok := actionJSONCache.Load(action); ok {
		return cached.([]byte), nil
	}
	encoded, err := client.JsonI.Marshal(action)
	if err != nil {
		return nil, err
	}
	// Store an immutable copy to prevent accidental external mutation.
	encoded = append([]byte(nil), encoded...)
	if existing, loaded := actionJSONCache.LoadOrStore(action, encoded); loaded {
		return existing.([]byte), nil
	}
	return encoded, nil
}

// encodeWSActionToBuffer writes {"action":<json-string>,"args":<raw-json>} to buf.
// appendNewline keeps wire compatibility with json.Encoder.Encode call sites.
func encodeWSActionToBuffer(buf *bytes.Buffer, action string, args json.RawMessage, appendNewline bool) error {
	actionJSON, err := getMarshaledAction(action)
	if err != nil {
		return err
	}

	buf.Reset()
	buf.Grow(len(wsActionPrefix) + len(actionJSON) + len(wsActionArgs) + len(args) + 4 + 2) // 4 = "null", 2 = "}\n"
	_, _ = buf.Write(wsActionPrefix)
	_, _ = buf.Write(actionJSON)
	_, _ = buf.Write(wsActionArgs)
	if args == nil {
		buf.WriteString("null")
	} else {
		_, _ = buf.Write(args)
	}
	buf.WriteByte('}')
	if appendNewline {
		buf.WriteByte('\n')
	}
	return nil
}
