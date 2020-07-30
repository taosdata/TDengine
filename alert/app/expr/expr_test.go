package expr

import "testing"

func TestIntArithmetic(t *testing.T) {
	cases := []struct {
		expr     string
		expected int64
	}{
		{"+10", 10},
		{"-10", -10},
		{"3 + 4 + 5 + 6 * 7 + 8", 62},
		{"3 + 4 + (5 + 6) * 7 + 8", 92},
		{"3 + 4 + (5 + 6) * 7 / 11 + 8", 22},
		{"3 + 4 + -5 * 6 / 7 % 8", 3},
		{"10 - 5", 5},
	}

	for _, c := range cases {
		expr, e := Compile(c.expr)
		if e != nil {
			t.Errorf("failed to compile expression '%s': %s", c.expr, e.Error())
		}
		if res := expr.Eval(nil); res.(int64) != c.expected {
			t.Errorf("result for expression '%s' is %v, but expected is %v", c.expr, res, c.expected)
		}
	}
}

func TestFloatArithmetic(t *testing.T) {
	cases := []struct {
		expr     string
		expected float64
	}{
		{"+10.5", 10.5},
		{"-10.5", -10.5},
		{"3.1 + 4.2 + 5 + 6 * 7 + 8", 62.3},
		{"3.1 + 4.2 + (5 + 6) * 7 + 8.3", 92.6},
		{"3.1 + 4.2 + (5.1 + 5.9) * 7 / 11 + 8", 22.3},
		{"3.3 + 4.2 - 4.0 * 7.5 / 3", -2.5},
		{"3.3 + 4.2 - 4 * 7.0 / 2", -6.5},
		{"3.5/2.0", 1.75},
		{"3.5/2", 1.75},
		{"7 / 3.5", 2},
		{"3.5 % 2.0", 1.5},
		{"3.5 % 2", 1.5},
		{"7 % 2.5", 2},
		{"7.3 - 2", 5.3},
		{"7 - 2.3", 4.7},
		{"1 + 1.5", 2.5},
	}

	for _, c := range cases {
		expr, e := Compile(c.expr)
		if e != nil {
			t.Errorf("failed to compile expression '%s': %s", c.expr, e.Error())
		}
		if res := expr.Eval(nil); res.(float64) != c.expected {
			t.Errorf("result for expression '%s' is %v, but expected is %v", c.expr, res, c.expected)
		}
	}
}

func TestVariable(t *testing.T) {
	variables := map[string]interface{}{
		"a": int64(6),
		"b": int64(7),
	}
	env := func(key string) interface{} {
		return variables[key]
	}

	cases := []struct {
		expr     string
		expected int64
	}{
		{"3 + 4 + (+5) + a * b + 8", 62},
		{"3 + 4 + (5 + a) * b + 8", 92},
		{"3 + 4 + (5 + a) * b / 11 + 8", 22},
	}

	for _, c := range cases {
		expr, e := Compile(c.expr)
		if e != nil {
			t.Errorf("failed to compile expression '%s': %s", c.expr, e.Error())
		}
		if res := expr.Eval(env); res.(int64) != c.expected {
			t.Errorf("result for expression '%s' is %v, but expected is %v", c.expr, res, c.expected)
		}
	}
}

func TestFunction(t *testing.T) {
	variables := map[string]interface{}{
		"a": int64(6),
		"b": 7.0,
	}

	env := func(key string) interface{} {
		return variables[key]
	}

	cases := []struct {
		expr     string
		expected float64
	}{
		{"sum(3, 4,  5, a * b, 8)", 62},
		{"sum(3, 4, (5 + a) * b, 8)", 92},
		{"sum(3, 4, (5 + a) * b / 11, 8)", 22},
	}

	for _, c := range cases {
		expr, e := Compile(c.expr)
		if e != nil {
			t.Errorf("failed to compile expression '%s': %s", c.expr, e.Error())
		}
		if res := expr.Eval(env); res.(float64) != c.expected {
			t.Errorf("result for expression '%s' is %v, but expected is %v", c.expr, res, c.expected)
		}
	}
}

func TestLogical(t *testing.T) {
	cases := []struct {
		expr     string
		expected bool
	}{
		{"true", true},
		{"false", false},
		{"true == true", true},
		{"true == false", false},
		{"true != true", false},
		{"true != false", true},
		{"5 > 3", true},
		{"5 < 3", false},
		{"5.2 > 3", true},
		{"5.2 < 3", false},
		{"5 > 3.1", true},
		{"5 < 3.1", false},
		{"5.1 > 3.3", true},
		{"5.1 < 3.3", false},
		{"5 >= 3", true},
		{"5 <= 3", false},
		{"5.2 >= 3", true},
		{"5.2 <= 3", false},
		{"5 >= 3.1", true},
		{"5 <= 3.1", false},
		{"5.1 >= 3.3", true},
		{"5.1 <= 3.3", false},
		{"5 != 3", true},
		{"5.2 != 3.2", true},
		{"5.2 != 3", true},
		{"5 != 3.2", true},
		{"5 == 3", false},
		{"5.2 == 3.2", false},
		{"5.2 == 3", false},
		{"5 == 3.2", false},
		{"!(5 > 3)", false},
		{"5>3 && 3>1", true},
		{"5<3 || 3<1", false},
		{"4<=4 || 3<1", true},
		{"4<4 || 3>=1", true},
	}

	for _, c := range cases {
		expr, e := Compile(c.expr)
		if e != nil {
			t.Errorf("failed to compile expression '%s': %s", c.expr, e.Error())
		}
		if res := expr.Eval(nil); res.(bool) != c.expected {
			t.Errorf("result for expression '%s' is %v, but expected is %v", c.expr, res, c.expected)
		}
	}
}

func TestBitwise(t *testing.T) {
	cases := []struct {
		expr     string
		expected int64
	}{
		{"0x0C & 0x04", 0x04},
		{"0x08 | 0x04", 0x0C},
		{"0x0C ^ 0x04", 0x08},
		{"0x01 << 2", 0x04},
		{"0x04 >> 2", 0x01},
		{"~0x04", ^0x04},
	}

	for _, c := range cases {
		expr, e := Compile(c.expr)
		if e != nil {
			t.Errorf("failed to compile expression '%s': %s", c.expr, e.Error())
		}
		if res := expr.Eval(nil); res.(int64) != c.expected {
			t.Errorf("result for expression '%s' is 0x%X, but expected is 0x%X", c.expr, res, c.expected)
		}
	}
}
