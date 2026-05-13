package tmq

import (
	"fmt"
	"reflect"
)

type ConfigValue interface{}
type ConfigMap map[string]ConfigValue

func (m ConfigMap) Get(key string, defval ConfigValue) (ConfigValue, error) {
	return m.get(key, defval)
}

func (m ConfigMap) get(key string, defval ConfigValue) (ConfigValue, error) {
	v, ok := m[key]
	if !ok {
		return defval, nil
	}

	if defval != nil && reflect.TypeOf(defval) != reflect.TypeOf(v) {
		return nil, fmt.Errorf("%s expects type %T, not %T", key, defval, v)
	}

	return v, nil
}

func (m ConfigMap) Clone() ConfigMap {
	m2 := make(ConfigMap)
	for k, v := range m {
		m2[k] = v
	}
	return m2
}
