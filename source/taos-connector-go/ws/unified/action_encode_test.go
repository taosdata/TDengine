package unified

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/taosdata/driver-go/v3/ws/client"
)

func encodeWSActionLegacy(action string, args json.RawMessage, appendNewline bool) ([]byte, error) {
	wsAction := &client.WSAction{
		Action: action,
		Args:   args,
	}
	if appendNewline {
		var buf bytes.Buffer
		err := client.JsonI.NewEncoder(&buf).Encode(wsAction)
		if err != nil {
			return nil, err
		}
		return append([]byte(nil), buf.Bytes()...), nil
	}
	payload, err := client.JsonI.Marshal(wsAction)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

// TestEncodeWSActionToBuffer_EquivalentWithLegacy verifies the expected behavior for this scenario.
func TestEncodeWSActionToBuffer_EquivalentWithLegacy(t *testing.T) {
	validObjectArgs, err := client.JsonI.Marshal(map[string]interface{}{
		"req_id":  uint64(12345),
		"db":      "demo",
		"special": "a\"b\\c",
	})
	require.NoError(t, err)

	cases := []struct {
		name          string
		action        string
		args          json.RawMessage
		appendNewline bool
	}{
		{
			name:          "object_args_with_newline",
			action:        "conn",
			args:          validObjectArgs,
			appendNewline: true,
		},
		{
			name:          "object_args_without_newline",
			action:        "insert",
			args:          validObjectArgs,
			appendNewline: false,
		},
		{
			name:          "array_args_with_newline",
			action:        "custom_action",
			args:          json.RawMessage(`[1,2,{"x":"y"}]`),
			appendNewline: true,
		},
		{
			name:          "nil_args_with_newline",
			action:        "conn",
			args:          nil,
			appendNewline: true,
		},
		{
			name:          "escaped_action_without_newline",
			action:        "a\"b\\c",
			args:          json.RawMessage(`{"k":"v"}`),
			appendNewline: false,
		},
	}

	for i := 0; i < len(cases); i++ {
		tc := cases[i]
		t.Run(tc.name, func(t *testing.T) {
			want, err := encodeWSActionLegacy(tc.action, tc.args, tc.appendNewline)
			require.NoError(t, err)

			var buf bytes.Buffer
			err = encodeWSActionToBuffer(&buf, tc.action, tc.args, tc.appendNewline)
			require.NoError(t, err)

			require.Equal(t, want, buf.Bytes())
		})
	}
}
