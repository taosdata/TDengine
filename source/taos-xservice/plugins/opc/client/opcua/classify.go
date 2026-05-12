package opcua

import (
	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

// ClassifyResult 表示对一个 NodeClass=Variable 节点的分类结果。
//
// 分类只适用于 Variable 节点；Object / 非 Variable 节点由调用方在 BFS 主循环
// 自行处理（Object 走 opc_object 拓扑通道，非 Variable 直接跳过）。
type ClassifyResult int

const (
	// ClassifyDynamicVariable 表示动态 Variable，独立建子表写入时序数据。
	ClassifyDynamicVariable ClassifyResult = iota
	// ClassifyProperty 表示父 Variable 的 Property 元数据，应合并为父 Variable
	// 子表的 Tag，不订阅、不建子表。
	ClassifyProperty
	// ClassifySkip 是防御性返回值，目前未使用；保留以便后续扩展。
	ClassifySkip
)

// String 提供 ClassifyResult 的可读名，方便日志与单测断言。
func (r ClassifyResult) String() string {
	switch r {
	case ClassifyDynamicVariable:
		return "DynamicVariable"
	case ClassifyProperty:
		return "Property"
	case ClassifySkip:
		return "Skip"
	}
	return "Unknown"
}

// itemTypeIDs 是 OPC UA 标准命名空间下、应被识别为"动态 Variable"的 TypeDefinition 集合。
//
// 这些类型都派生自 BaseDataVariableType，语义上代表过程值（process value），
// 应建独立子表订阅。
//
// 参考 OPC UA Part 8（Data Access）第 5 章。
var itemTypeIDs = map[uint32]struct{}{
	id.BaseDataVariableType:        {},
	id.DataItemType:                {},
	id.AnalogItemType:              {},
	id.AnalogUnitRangeType:         {},
	id.AnalogUnitType:              {},
	id.DiscreteItemType:            {},
	id.TwoStateDiscreteType:        {},
	id.MultiStateDiscreteType:      {},
	id.MultiStateValueDiscreteType: {},
	id.ArrayItemType:               {},
	id.YArrayItemType:              {},
	id.XYArrayItemType:             {},
	id.ImageItemType:               {},
	id.CubeItemType:                {},
	id.NDimensionArrayItemType:     {},
}

// numericIDInNS0 提取命名空间 0（OPC UA 标准）下的数值 NodeID。
//
// gopcua 对数值型 NodeID 有三种紧凑编码：TwoByte（i<=255）、FourByte（ns<=255 && i<=65535）、
// Numeric（任意）。对应 OPC UA 二进制协议的 NodeIdEncoding 0/1/2，三者语义等价、
// IntID() 都返回 ID。早期实现只接受 Numeric，会把 i=46/i=68 这类标准 ID
// （server 通常用 TwoByte 编码）当成"非数值 ID"拒绝，导致所有 Property/ItemType
// 判定失败、全部走 fallback 规则。此函数统一三种编码。
func numericIDInNS0(n *ua.NodeID) (uint32, bool) {
	if n == nil {
		return 0, false
	}
	if n.Namespace() != 0 {
		return 0, false
	}
	switch n.Type() {
	case ua.NodeIDTypeTwoByte, ua.NodeIDTypeFourByte, ua.NodeIDTypeNumeric:
		return n.IntID(), true
	}
	return 0, false
}

// isItemTypeDefinition 判断 typeDef 是否属于"动态 Variable"白名单。
//
// 仅对命名空间 0（OPC UA 标准）下的数值 ID 做匹配；自定义命名空间下的
// TypeDefinition 一律不命中（由兜底规则处理）。
func isItemTypeDefinition(typeDef *ua.NodeID) bool {
	intID, ok := numericIDInNS0(typeDef)
	if !ok {
		return false
	}
	_, hit := itemTypeIDs[intID]
	return hit
}

// isPropertyType 判断 typeDef 是否为 OPC UA 标准的 PropertyType（i=68）。
func isPropertyType(typeDef *ua.NodeID) bool {
	intID, ok := numericIDInNS0(typeDef)
	return ok && intID == id.PropertyType
}

// isHasPropertyRef 判断 refType 是否为 OPC UA 标准的 HasProperty Reference（i=46）。
func isHasPropertyRef(refType *ua.NodeID) bool {
	intID, ok := numericIDInNS0(refType)
	return ok && intID == id.HasProperty
}

// Classify 对 NodeClass=Variable 节点应用 4 级分类规则，返回分类结果与命中规则的简短描述。
//
// 规则优先级（高 → 低）：
//
//  1. 父==Variable && refType==HasProperty(i=46) → Property
//  2. typeDef==PropertyType(i=68)               → Property
//     typeDef ∈ ItemType 系列                  → DynamicVariable
//  3. 兜底                                     → DynamicVariable（调用方应记录 WARN）
//
// 第二个返回值是命中规则的简短代号（如 "rule2-HasProperty"），用于日志与单测断言。
//
// 参数：
//   - parentNodeClass: 父节点的 NodeClass（不是当前节点）
//   - refType:         父→当前节点的 Reference 类型 NodeID（如 HasProperty / HasComponent）
//   - typeDef:         当前节点的 HasTypeDefinition NodeID（如 PropertyType / AnalogItemType）
//
// 调用方必须先确保当前节点是 NodeClass=Variable。
func Classify(parentNodeClass ua.NodeClass, refType *ua.NodeID, typeDef *ua.NodeID) (ClassifyResult, string) {
	// 规则 2：父 Variable + HasProperty Reference
	if parentNodeClass == ua.NodeClassVariable && isHasPropertyRef(refType) {
		return ClassifyProperty, "rule2-HasProperty"
	}
	// 规则 3a：TypeDefinition == PropertyType(i=68)
	if isPropertyType(typeDef) {
		return ClassifyProperty, "rule3-PropertyType"
	}
	// 规则 3b：TypeDefinition ∈ ItemType 系列
	if isItemTypeDefinition(typeDef) {
		return ClassifyDynamicVariable, "rule3-ItemType"
	}
	// 规则 4：兜底
	return ClassifyDynamicVariable, "rule4-fallback"
}
