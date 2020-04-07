package expr

import (
	"math"
)

type builtInFunc struct {
	minArgs, maxArgs int
	call             func([]interface{}) interface{}
}

func fnMin(args []interface{}) interface{} {
	res := args[0]
	for _, arg := range args[1:] {
		switch v1 := res.(type) {
		case int64:
			switch v2 := arg.(type) {
			case int64:
				if v2 < v1 {
					res = v2
				}
			case float64:
				res = math.Min(float64(v1), v2)
			default:
				panic(ErrorUnsupportedDataType)
			}
		case float64:
			switch v2 := arg.(type) {
			case int64:
				res = math.Min(v1, float64(v2))
			case float64:
				res = math.Min(v1, v2)
			default:
				panic(ErrorUnsupportedDataType)
			}
		default:
			panic(ErrorUnsupportedDataType)
		}
	}
	return res
}

func fnMax(args []interface{}) interface{} {
	res := args[0]
	for _, arg := range args[1:] {
		switch v1 := res.(type) {
		case int64:
			switch v2 := arg.(type) {
			case int64:
				if v2 > v1 {
					res = v2
				}
			case float64:
				res = math.Max(float64(v1), v2)
			default:
				panic(ErrorUnsupportedDataType)
			}
		case float64:
			switch v2 := arg.(type) {
			case int64:
				res = math.Max(v1, float64(v2))
			case float64:
				res = math.Max(v1, v2)
			default:
				panic(ErrorUnsupportedDataType)
			}
		default:
			panic(ErrorUnsupportedDataType)
		}
	}
	return res
}

func fnSum(args []interface{}) interface{} {
	res := float64(0)
	for _, arg := range args {
		switch v := arg.(type) {
		case int64:
			res += float64(v)
		case float64:
			res += v
		default:
			panic(ErrorUnsupportedDataType)
		}
	}
	return res
}

func fnAvg(args []interface{}) interface{} {
	return fnSum(args).(float64) / float64(len(args))
}

func fnSqrt(args []interface{}) interface{} {
	switch v := args[0].(type) {
	case int64:
		return math.Sqrt(float64(v))
	case float64:
		return math.Sqrt(v)
	default:
		panic(ErrorUnsupportedDataType)
	}
}

func fnFloor(args []interface{}) interface{} {
	switch v := args[0].(type) {
	case int64:
		return v
	case float64:
		return math.Floor(v)
	default:
		panic(ErrorUnsupportedDataType)
	}
}

func fnCeil(args []interface{}) interface{} {
	switch v := args[0].(type) {
	case int64:
		return v
	case float64:
		return math.Ceil(v)
	default:
		panic(ErrorUnsupportedDataType)
	}
}

func fnRound(args []interface{}) interface{} {
	switch v := args[0].(type) {
	case int64:
		return v
	case float64:
		return math.Round(v)
	default:
		panic(ErrorUnsupportedDataType)
	}
}

func fnLog(args []interface{}) interface{} {
	switch v := args[0].(type) {
	case int64:
		return math.Log(float64(v))
	case float64:
		return math.Log(v)
	default:
		panic(ErrorUnsupportedDataType)
	}
}

func fnLog10(args []interface{}) interface{} {
	switch v := args[0].(type) {
	case int64:
		return math.Log10(float64(v))
	case float64:
		return math.Log10(v)
	default:
		panic(ErrorUnsupportedDataType)
	}
}

func fnAbs(args []interface{}) interface{} {
	switch v := args[0].(type) {
	case int64:
		if v < 0 {
			return -v
		}
		return v
	case float64:
		return math.Abs(v)
	default:
		panic(ErrorUnsupportedDataType)
	}
}

func fnIf(args []interface{}) interface{} {
	v, ok := args[0].(bool)
	if !ok {
		panic(ErrorUnsupportedDataType)
	}
	if v {
		return args[1]
	} else {
		return args[2]
	}
}

var funcs = map[string]builtInFunc{
	"min":   builtInFunc{minArgs: 1, maxArgs: -1, call: fnMin},
	"max":   builtInFunc{minArgs: 1, maxArgs: -1, call: fnMax},
	"sum":   builtInFunc{minArgs: 1, maxArgs: -1, call: fnSum},
	"avg":   builtInFunc{minArgs: 1, maxArgs: -1, call: fnAvg},
	"sqrt":  builtInFunc{minArgs: 1, maxArgs: 1, call: fnSqrt},
	"ceil":  builtInFunc{minArgs: 1, maxArgs: 1, call: fnCeil},
	"floor": builtInFunc{minArgs: 1, maxArgs: 1, call: fnFloor},
	"round": builtInFunc{minArgs: 1, maxArgs: 1, call: fnRound},
	"log":   builtInFunc{minArgs: 1, maxArgs: 1, call: fnLog},
	"log10": builtInFunc{minArgs: 1, maxArgs: 1, call: fnLog10},
	"abs":   builtInFunc{minArgs: 1, maxArgs: 1, call: fnAbs},
	"if":    builtInFunc{minArgs: 3, maxArgs: 3, call: fnIf},
}
