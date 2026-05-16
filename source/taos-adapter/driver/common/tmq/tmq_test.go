package tmq

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

const createJson = `{
    "type": "create",
    "tableName": "t1",
    "tableType": "super", 
    "columns": [ 
        {
            "name": "c1", 
            "type": 0, 
            "length": 0 
        },
        {
            "name": "c2",
            "type": 8,
            "length": 8
        }
    ],
    "tags": [
        {
            "name": "t1",
            "type": 0,
            "length": 0
        },
        {
            "name": "t2",
            "type": 8,
            "length": 8
        }
    ]
}`
const dropJson = `{
  "type":"drop",            
  "tableName":"t1",         
  "tableType":"super",      
  "tableNameList":["t1", "t2"]
}`

// @author: xftan
// @date: 2023/10/13 11:19
// @description: test json
func TestCreateJson(t *testing.T) {
	var obj Meta
	err := json.Unmarshal([]byte(createJson), &obj)
	if err != nil {
		t.Log(err)
		return
	}
	t.Log(obj)
}

// @author: xftan
// @date: 2023/10/13 11:19
// @description: test drop json
func TestDropJson(t *testing.T) {
	var obj Meta
	err := json.Unmarshal([]byte(dropJson), &obj)
	if err != nil {
		t.Log(err)
		return
	}
	t.Log(obj)
}

func TestOffset_String(t *testing.T) {
	tests := []struct {
		name string
		o    Offset
		want string
	}{
		{
			name: "Valid Offset",
			o:    100,
			want: "100",
		},
		{
			name: "Invalid Offset",
			o:    OffsetInvalid,
			want: "unset",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.o.String(); got != tt.want {
				t.Errorf("Offset.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOffset_Valid(t *testing.T) {
	tests := []struct {
		name string
		o    Offset
		want bool
	}{
		{
			name: "Valid Offset",
			o:    100,
			want: true,
		},
		{
			name: "Invalid Offset",
			o:    OffsetInvalid,
			want: true,
		},
		{
			name: "Negative Offset",
			o:    -100,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.o.Valid(); got != tt.want {
				t.Errorf("Offset.Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTopicPartition_String(t *testing.T) {
	tests := []struct {
		name string
		tp   TopicPartition
		want string
	}{
		{
			name: "With Error",
			tp: TopicPartition{
				Topic:     stringPtr("test-topic"),
				Partition: 0,
				Offset:    100,
				Error:     errors.New("error message"),
			},
			want: "test-topic[0]@100(error message)",
		},
		{
			name: "Without Error",
			tp: TopicPartition{
				Topic:     stringPtr("test-topic"),
				Partition: 0,
				Offset:    100,
			},
			want: "test-topic[0]@100",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.tp.String(); got != tt.want {
				t.Errorf("TopicPartition.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAssignment_MarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		a    Assignment
		want string
	}{
		{
			name: "Marshal Assignment",
			a: Assignment{
				VGroupID: 1,
				Offset:   100,
				Begin:    50,
				End:      150,
			},
			want: `{"vgroup_id":1,"offset":100,"begin":50,"end":150}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.a)
			if err != nil {
				t.Errorf("MarshalJSON error: %v", err)
				return
			}
			if !reflect.DeepEqual(string(got), tt.want) {
				t.Errorf("MarshalJSON = %v, want %v", string(got), tt.want)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}
