package opcua

import (
	"testing"

	"github.com/gopcua/opcua/id"
	"github.com/gopcua/opcua/ua"
)

func numericNS0(intID uint32) *ua.NodeID {
	return ua.NewNumericNodeID(0, intID)
}

func numericNS(ns uint16, intID uint32) *ua.NodeID {
	return ua.NewNumericNodeID(ns, intID)
}

func stringNS(ns uint16, s string) *ua.NodeID {
	return ua.NewStringNodeID(ns, s)
}

func TestClassify_Rule2_HasPropertyUnderVariable(t *testing.T) {
	// 父==Variable + Reference==HasProperty(i=46) → Property
	got, hit := Classify(
		ua.NodeClassVariable,
		numericNS0(id.HasProperty),
		nil,
	)
	if got != ClassifyProperty {
		t.Fatalf("want Property, got %s", got)
	}
	if hit != "rule2-HasProperty" {
		t.Fatalf("want rule2-HasProperty, got %s", hit)
	}
}

func TestClassify_Rule2_HasPropertyUnderObject_NotProperty(t *testing.T) {
	// 父==Object + Reference==HasProperty → 规则 2 不命中（限定父==Variable）
	// fallthrough 到规则 3 / 4：typeDef=nil → 兜底 DynamicVariable
	got, hit := Classify(
		ua.NodeClassObject,
		numericNS0(id.HasProperty),
		nil,
	)
	if got != ClassifyDynamicVariable {
		t.Fatalf("want DynamicVariable, got %s", got)
	}
	if hit != "rule4-fallback" {
		t.Fatalf("want rule4-fallback, got %s", hit)
	}
}

func TestClassify_Rule3a_PropertyType(t *testing.T) {
	// TypeDefinition==PropertyType(i=68) → Property（不论父类型/Reference）
	got, hit := Classify(
		ua.NodeClassObject,
		numericNS0(id.HasComponent),
		numericNS0(id.PropertyType),
	)
	if got != ClassifyProperty {
		t.Fatalf("want Property, got %s", got)
	}
	if hit != "rule3-PropertyType" {
		t.Fatalf("want rule3-PropertyType, got %s", hit)
	}
}

func TestClassify_Rule3b_AnalogItemType(t *testing.T) {
	// TypeDefinition==AnalogItemType(i=2368) → DynamicVariable
	got, hit := Classify(
		ua.NodeClassObject,
		numericNS0(id.HasComponent),
		numericNS0(id.AnalogItemType),
	)
	if got != ClassifyDynamicVariable {
		t.Fatalf("want DynamicVariable, got %s", got)
	}
	if hit != "rule3-ItemType" {
		t.Fatalf("want rule3-ItemType, got %s", hit)
	}
}

func TestClassify_Rule3b_BaseDataVariableType(t *testing.T) {
	got, hit := Classify(
		ua.NodeClassObject,
		numericNS0(id.HasComponent),
		numericNS0(id.BaseDataVariableType),
	)
	if got != ClassifyDynamicVariable || hit != "rule3-ItemType" {
		t.Fatalf("want DynamicVariable/rule3-ItemType, got %s/%s", got, hit)
	}
}

func TestClassify_Rule3b_DiscreteAndArrayItemTypes(t *testing.T) {
	cases := []uint32{
		id.DataItemType,
		id.DiscreteItemType,
		id.TwoStateDiscreteType,
		id.MultiStateDiscreteType,
		id.MultiStateValueDiscreteType,
		id.ArrayItemType,
		id.YArrayItemType,
		id.XYArrayItemType,
		id.ImageItemType,
		id.CubeItemType,
		id.NDimensionArrayItemType,
		id.AnalogUnitType,
		id.AnalogUnitRangeType,
	}
	for _, tid := range cases {
		got, hit := Classify(
			ua.NodeClassObject,
			numericNS0(id.HasComponent),
			numericNS0(tid),
		)
		if got != ClassifyDynamicVariable || hit != "rule3-ItemType" {
			t.Fatalf("typeDef=i=%d: want DynamicVariable/rule3-ItemType, got %s/%s", tid, got, hit)
		}
	}
}

func TestClassify_Rule4_Fallback(t *testing.T) {
	// 没有任何规则命中 → 兜底 DynamicVariable
	got, hit := Classify(
		ua.NodeClassObject,
		numericNS0(id.HasComponent),
		nil,
	)
	if got != ClassifyDynamicVariable {
		t.Fatalf("want DynamicVariable, got %s", got)
	}
	if hit != "rule4-fallback" {
		t.Fatalf("want rule4-fallback, got %s", hit)
	}
}

func TestClassify_Rule4_NonStandardTypeDefinition(t *testing.T) {
	// 自定义命名空间下的 TypeDefinition 一律不命中规则 3 → 走兜底
	got, hit := Classify(
		ua.NodeClassVariable,
		numericNS0(id.HasComponent),
		numericNS(2, 9999),
	)
	if got != ClassifyDynamicVariable {
		t.Fatalf("want DynamicVariable, got %s", got)
	}
	if hit != "rule4-fallback" {
		t.Fatalf("want rule4-fallback, got %s", hit)
	}
}

func TestClassify_Rule2_PrecedesRule3b(t *testing.T) {
	// 规则 2（HasProperty + 父 Variable）应优先于规则 3b（即使 typeDef 在白名单）
	// 真实场景：不规范服务器把 EURange 的 TypeDef 错填成 BaseDataVariableType，
	// 但 Reference 仍是 HasProperty —— 应判为 Property
	got, hit := Classify(
		ua.NodeClassVariable,
		numericNS0(id.HasProperty),
		numericNS0(id.BaseDataVariableType),
	)
	if got != ClassifyProperty {
		t.Fatalf("want Property, got %s", got)
	}
	if hit != "rule2-HasProperty" {
		t.Fatalf("want rule2-HasProperty, got %s", hit)
	}
}

func TestClassify_Rule3a_PrecedesRule3b(t *testing.T) {
	// PropertyType 优先于 ItemType 系列（理论上不会同时命中，但代码上 rule3a 先判）
	got, hit := Classify(
		ua.NodeClassObject,
		numericNS0(id.HasComponent),
		numericNS0(id.PropertyType),
	)
	if got != ClassifyProperty || hit != "rule3-PropertyType" {
		t.Fatalf("want Property/rule3-PropertyType, got %s/%s", got, hit)
	}
}

func TestClassify_StringRefTypeIgnored(t *testing.T) {
	// 自定义命名空间或非数值的 ReferenceTypeID 一律不命中规则 2
	got, hit := Classify(
		ua.NodeClassVariable,
		stringNS(2, "MyCustomRef"),
		nil,
	)
	if got != ClassifyDynamicVariable || hit != "rule4-fallback" {
		t.Fatalf("want DynamicVariable/rule4-fallback, got %s/%s", got, hit)
	}
}

func TestClassify_NilInputs_Fallback(t *testing.T) {
	// 全 nil（refType + typeDef） + 父 NodeClass=Unspecified → 兜底
	got, hit := Classify(ua.NodeClassUnspecified, nil, nil)
	if got != ClassifyDynamicVariable || hit != "rule4-fallback" {
		t.Fatalf("want DynamicVariable/rule4-fallback, got %s/%s", got, hit)
	}
}

// TestClassify_Rule2_HasProperty_TwoByteEncoding 回归 bug：实际 OPC server 用 TwoByte
// 编码下发 i=46（HasProperty），早期 helper 严格要求 NodeIDTypeNumeric 把它误判为非数值，
// 导致 EURange/EngineeringUnits 等 Property 全部走 fallback 当作动态 Variable。
func TestClassify_Rule2_HasProperty_TwoByteEncoding(t *testing.T) {
	got, hit := Classify(
		ua.NodeClassVariable,
		ua.NewTwoByteNodeID(uint8(id.HasProperty)),
		ua.NewTwoByteNodeID(uint8(id.PropertyType)),
	)
	if got != ClassifyProperty {
		t.Fatalf("want Property, got %s/%s", got, hit)
	}
	if hit != "rule2-HasProperty" {
		t.Fatalf("want rule2-HasProperty, got %s", hit)
	}
}

// TestClassify_Rule3a_PropertyType_TwoByteEncoding 同上，覆盖 Rule 3a。
func TestClassify_Rule3a_PropertyType_TwoByteEncoding(t *testing.T) {
	got, hit := Classify(
		ua.NodeClassObject, // 父非 Variable，规则 2 不命中
		ua.NewTwoByteNodeID(uint8(id.HasComponent)),
		ua.NewTwoByteNodeID(uint8(id.PropertyType)),
	)
	if got != ClassifyProperty || hit != "rule3-PropertyType" {
		t.Fatalf("want Property/rule3-PropertyType, got %s/%s", got, hit)
	}
}

// TestClassify_Rule3b_ItemType_FourByteEncoding 覆盖 FourByte 编码下 ItemType 判定。
func TestClassify_Rule3b_ItemType_FourByteEncoding(t *testing.T) {
	got, hit := Classify(
		ua.NodeClassObject,
		ua.NewFourByteNodeID(0, uint16(id.HasComponent)),
		ua.NewFourByteNodeID(0, uint16(id.AnalogItemType)),
	)
	if got != ClassifyDynamicVariable || hit != "rule3-ItemType" {
		t.Fatalf("want DynamicVariable/rule3-ItemType, got %s/%s", got, hit)
	}
}

func TestClassifyResult_String(t *testing.T) {
	cases := map[ClassifyResult]string{
		ClassifyDynamicVariable: "DynamicVariable",
		ClassifyProperty:        "Property",
		ClassifySkip:            "Skip",
		ClassifyResult(99):      "Unknown",
	}
	for r, want := range cases {
		if got := r.String(); got != want {
			t.Fatalf("%d: want %q, got %q", r, want, got)
		}
	}
}
