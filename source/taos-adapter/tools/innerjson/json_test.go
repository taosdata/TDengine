package innerjson

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	A int    `json:"a"`
	B string `json:"b"`
	C bool   `json:"c"`
}

type TestWrongStruct struct {
	C chan struct{} `json:"c"`
}

func TestMarshal(t *testing.T) {
	var s = "ts >= 123"
	x, _ := json.Marshal(s)
	fmt.Println(string(x))
	bs, err := Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, `"ts >= 123"`, string(bs))
	var ts = TestStruct{
		A: 1,
		B: "test",
		C: true,
	}
	bs, err = Marshal(ts)
	if err != nil {
		t.Fatal(err)
	}
	expected := `{"a":1,"b":"test","c":true}`
	assert.Equal(t, expected, string(bs))

	var tws = TestWrongStruct{
		C: make(chan struct{}),
	}
	_, err = Marshal(tws)
	assert.Error(t, err)
	bs, err = Marshal(ts)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, expected, string(bs))
}
