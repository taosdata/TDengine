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
	ID          string `json:"id"`
	IsStatic    bool   `json:"is_static"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	DisplayName string `json:"display_name,omitempty"`
	NodeType    string `json:"node_type,omitempty"`
	ParentID    string `json:"-"`
	Path        string `json:"path,omitempty"`
	// IsProperty 表示该 Variable 是父 Variable 的 Property 元数据（如 EURange、EngineeringUnits）。
	// 为 true 时不应建独立子表，而应合并到父 Variable 子表的 Tag。
	// 仅对 NodeClass=Variable 节点有意义；Object / 顶层 Variable 一律为 false。
	IsProperty bool `json:"is_property,omitempty"`
	// Properties 存储父 Variable 收集到的、归属自身的 Property 名→值（已序列化为字符串；
	// 复杂类型用 JSON 字符串）。仅对动态 Variable（IsProperty=false）有意义；
	// Property 节点本身的 Properties 永远为 nil（其值已塞进父）。
	Properties map[string]string `json:"properties,omitempty"`
}
