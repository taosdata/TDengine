package inputjson

import (
	"bytes"
	"encoding/json"
)

func Unmarshal(data []byte, v interface{}) error {
	reader := bytes.NewReader(data)
	// json.Decoder can not use a pool because when Decode return error,
	// the Decoder is in an invalid state and can not be reused.
	dec := json.NewDecoder(reader)
	// UseNumber causes the Decoder to unmarshal a number into an interface{} as a
	// json.Number instead of as a float64.
	dec.UseNumber()
	return dec.Decode(v)
}
