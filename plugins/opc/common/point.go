package common

import (
	"collector/types"
	"time"
)

type NodeValue struct {
	Identifier string
	Name       string
	Timestamp  time.Time
	StartTime  time.Time
	FinishTime time.Time
	Value      interface{}
	ValueType  types.ValueType
	Status     int64
}

type Point struct {
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
}
