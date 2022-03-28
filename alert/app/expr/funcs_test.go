/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package expr

import (
	"math"
	"testing"
)

func TestMax(t *testing.T) {
	cases := []struct {
		args     []interface{}
		expected float64
	}{
		{[]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}, 5},
		{[]interface{}{int64(1), int64(2), float64(3), int64(4), float64(5)}, 5},
		{[]interface{}{int64(-1), int64(-2), float64(-3), int64(-4), float64(-5)}, -1},
		{[]interface{}{int64(-1), int64(-1), float64(-1), int64(-1), float64(-1)}, -1},
		{[]interface{}{int64(-1), int64(0), float64(-1), int64(-1), float64(-1)}, 0},
	}

	for _, c := range cases {
		r := fnMax(c.args)
		switch v := r.(type) {
		case int64:
			if v != int64(c.expected) {
				t.Errorf("max(%v) = %v, want %v", c.args, v, int64(c.expected))
			}
		case float64:
			if v != c.expected {
				t.Errorf("max(%v) = %v, want %v", c.args, v, c.expected)
			}
		default:
			t.Errorf("unknown result type max(%v)", c.args)
		}
	}
}

func TestMin(t *testing.T) {
	cases := []struct {
		args     []interface{}
		expected float64
	}{
		{[]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}, 1},
		{[]interface{}{int64(5), int64(4), float64(3), int64(2), float64(1)}, 1},
		{[]interface{}{int64(-1), int64(-2), float64(-3), int64(-4), float64(-5)}, -5},
		{[]interface{}{int64(-1), int64(-1), float64(-1), int64(-1), float64(-1)}, -1},
		{[]interface{}{int64(1), int64(0), float64(1), int64(1), float64(1)}, 0},
	}

	for _, c := range cases {
		r := fnMin(c.args)
		switch v := r.(type) {
		case int64:
			if v != int64(c.expected) {
				t.Errorf("min(%v) = %v, want %v", c.args, v, int64(c.expected))
			}
		case float64:
			if v != c.expected {
				t.Errorf("min(%v) = %v, want %v", c.args, v, c.expected)
			}
		default:
			t.Errorf("unknown result type min(%v)", c.args)
		}
	}
}

func TestSumAvg(t *testing.T) {
	cases := []struct {
		args     []interface{}
		expected float64
	}{
		{[]interface{}{int64(1)}, 1},
		{[]interface{}{int64(1), int64(2), int64(3), int64(4), int64(5)}, 15},
		{[]interface{}{int64(5), int64(4), float64(3), int64(2), float64(1)}, 15},
		{[]interface{}{int64(-1), int64(-2), float64(-3), int64(-4), float64(-5)}, -15},
		{[]interface{}{int64(-1), int64(-1), float64(-1), int64(-1), float64(-1)}, -5},
		{[]interface{}{int64(1), int64(0), float64(1), int64(1), float64(1)}, 4},
	}

	for _, c := range cases {
		r := fnSum(c.args)
		switch v := r.(type) {
		case float64:
			if v != c.expected {
				t.Errorf("sum(%v) = %v, want %v", c.args, v, c.expected)
			}
		default:
			t.Errorf("unknown result type sum(%v)", c.args)
		}
	}

	for _, c := range cases {
		r := fnAvg(c.args)
		expected := c.expected / float64(len(c.args))
		switch v := r.(type) {
		case float64:
			if v != expected {
				t.Errorf("avg(%v) = %v, want %v", c.args, v, expected)
			}
		default:
			t.Errorf("unknown result type avg(%v)", c.args)
		}
	}
}

func TestSqrt(t *testing.T) {
	cases := []struct {
		arg      interface{}
		expected float64
	}{
		{int64(0), 0},
		{int64(1), 1},
		{int64(256), 16},
		{10.0, math.Sqrt(10)},
		{10000.0, math.Sqrt(10000)},
	}

	for _, c := range cases {
		r := fnSqrt([]interface{}{c.arg})
		switch v := r.(type) {
		case float64:
			if v != c.expected {
				t.Errorf("sqrt(%v) = %v, want %v", c.arg, v, c.expected)
			}
		default:
			t.Errorf("unknown result type sqrt(%v)", c.arg)
		}
	}
}

func TestFloor(t *testing.T) {
	cases := []struct {
		arg      interface{}
		expected float64
	}{
		{int64(0), 0},
		{int64(1), 1},
		{int64(-1), -1},
		{10.4, 10},
		{-10.4, -11},
		{10.8, 10},
		{-10.8, -11},
	}

	for _, c := range cases {
		r := fnFloor([]interface{}{c.arg})
		switch v := r.(type) {
		case int64:
			if v != int64(c.expected) {
				t.Errorf("floor(%v) = %v, want %v", c.arg, v, int64(c.expected))
			}
		case float64:
			if v != c.expected {
				t.Errorf("floor(%v) = %v, want %v", c.arg, v, c.expected)
			}
		default:
			t.Errorf("unknown result type floor(%v)", c.arg)
		}
	}
}

func TestCeil(t *testing.T) {
	cases := []struct {
		arg      interface{}
		expected float64
	}{
		{int64(0), 0},
		{int64(1), 1},
		{int64(-1), -1},
		{10.4, 11},
		{-10.4, -10},
		{10.8, 11},
		{-10.8, -10},
	}

	for _, c := range cases {
		r := fnCeil([]interface{}{c.arg})
		switch v := r.(type) {
		case int64:
			if v != int64(c.expected) {
				t.Errorf("ceil(%v) = %v, want %v", c.arg, v, int64(c.expected))
			}
		case float64:
			if v != c.expected {
				t.Errorf("ceil(%v) = %v, want %v", c.arg, v, c.expected)
			}
		default:
			t.Errorf("unknown result type ceil(%v)", c.arg)
		}
	}
}

func TestRound(t *testing.T) {
	cases := []struct {
		arg      interface{}
		expected float64
	}{
		{int64(0), 0},
		{int64(1), 1},
		{int64(-1), -1},
		{10.4, 10},
		{-10.4, -10},
		{10.8, 11},
		{-10.8, -11},
	}

	for _, c := range cases {
		r := fnRound([]interface{}{c.arg})
		switch v := r.(type) {
		case int64:
			if v != int64(c.expected) {
				t.Errorf("round(%v) = %v, want %v", c.arg, v, int64(c.expected))
			}
		case float64:
			if v != c.expected {
				t.Errorf("round(%v) = %v, want %v", c.arg, v, c.expected)
			}
		default:
			t.Errorf("unknown result type round(%v)", c.arg)
		}
	}
}

func TestLog(t *testing.T) {
	cases := []struct {
		arg      interface{}
		expected float64
	}{
		{int64(1), math.Log(1)},
		{0.1, math.Log(0.1)},
		{10.4, math.Log(10.4)},
		{10.8, math.Log(10.8)},
	}

	for _, c := range cases {
		r := fnLog([]interface{}{c.arg})
		switch v := r.(type) {
		case float64:
			if v != c.expected {
				t.Errorf("log(%v) = %v, want %v", c.arg, v, c.expected)
			}
		default:
			t.Errorf("unknown result type log(%v)", c.arg)
		}
	}
}

func TestLog10(t *testing.T) {
	cases := []struct {
		arg      interface{}
		expected float64
	}{
		{int64(1), math.Log10(1)},
		{0.1, math.Log10(0.1)},
		{10.4, math.Log10(10.4)},
		{10.8, math.Log10(10.8)},
		{int64(100), math.Log10(100)},
	}

	for _, c := range cases {
		r := fnLog10([]interface{}{c.arg})
		switch v := r.(type) {
		case float64:
			if v != c.expected {
				t.Errorf("log10(%v) = %v, want %v", c.arg, v, c.expected)
			}
		default:
			t.Errorf("unknown result type log10(%v)", c.arg)
		}
	}
}

func TestAbs(t *testing.T) {
	cases := []struct {
		arg      interface{}
		expected float64
	}{
		{int64(1), 1},
		{int64(0), 0},
		{int64(-1), 1},
		{10.4, 10.4},
		{-10.4, 10.4},
	}

	for _, c := range cases {
		r := fnAbs([]interface{}{c.arg})
		switch v := r.(type) {
		case int64:
			if v != int64(c.expected) {
				t.Errorf("abs(%v) = %v, want %v", c.arg, v, int64(c.expected))
			}
		case float64:
			if v != c.expected {
				t.Errorf("abs(%v) = %v, want %v", c.arg, v, c.expected)
			}
		default:
			t.Errorf("unknown result type abs(%v)", c.arg)
		}
	}
}

func TestIf(t *testing.T) {
	cases := []struct {
		args     []interface{}
		expected float64
	}{
		{[]interface{}{true, int64(10), int64(20)}, 10},
		{[]interface{}{false, int64(10), int64(20)}, 20},
		{[]interface{}{true, 10.3, 20.6}, 10.3},
		{[]interface{}{false, 10.3, 20.6}, 20.6},
		{[]interface{}{true, int64(10), 20.6}, 10},
		{[]interface{}{false, int64(10), 20.6}, 20.6},
	}

	for _, c := range cases {
		r := fnIf(c.args)
		switch v := r.(type) {
		case int64:
			if v != int64(c.expected) {
				t.Errorf("if(%v) = %v, want %v", c.args, v, int64(c.expected))
			}
		case float64:
			if v != c.expected {
				t.Errorf("if(%v) = %v, want %v", c.args, v, c.expected)
			}
		default:
			t.Errorf("unknown result type if(%v)", c.args)
		}
	}
}
