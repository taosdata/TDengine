package common

func InSlice[T string | int64 | int32 | int | float64 | float32 | ValueType](ele T, slice []T) bool {
	for _, e := range slice {
		if e == ele {
			return true
		}
	}
	return false
}
