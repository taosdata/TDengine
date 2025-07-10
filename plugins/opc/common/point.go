package common

import (
	"collector/types"
	"sync"
	"time"
)

type NodeValue struct {
	IDStr      string
	Name       string
	Timestamp  time.Time
	StartTime  time.Time
	FinishTime time.Time
	Value      interface{}
	ValueType  types.ValueType
	Status     int64
}

func (nv *NodeValue) Copy() *NodeValue {
	newNV := GetNodeValue()
	newNV.IDStr = nv.IDStr
	newNV.Name = nv.Name
	newNV.Timestamp = nv.Timestamp
	newNV.StartTime = nv.StartTime
	newNV.FinishTime = nv.FinishTime
	newNV.Value = nv.Value
	newNV.ValueType = nv.ValueType
	newNV.Status = nv.Status
	return newNV
}

var NodeValuePool = sync.Pool{
	New: func() interface{} {
		return &NodeValue{}
	},
}

func PutNodeValue(nv *NodeValue) {
	NodeValuePool.Put(nv)
}

func GetNodeValue() *NodeValue {
	return NodeValuePool.Get().(*NodeValue)
}

type Point struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description"`
}
