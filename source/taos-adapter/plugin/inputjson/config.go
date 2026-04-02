package inputjson

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Enable bool
	Rules  []*Rule
}

type Rule struct {
	Endpoint       string
	DB             string
	DBKey          string
	SuperTable     string
	SuperTableKey  string
	SubTable       string
	SubTableKey    string
	TimeKey        string
	TimeFormat     string
	Timezone       string
	TimeFieldName  string
	Transformation string
	Fields         []*Field
}

type Field struct {
	Key      string
	Optional bool
}

func (c *Config) setValue(v *viper.Viper) error {
	c.Enable = v.GetBool("input_json.enable")
	if !c.Enable {
		return nil
	}
	// if enabled , rules must be provided
	rules := v.Get("input_json.rules")
	rulesList, ok := rules.([]interface{})
	if !ok {
		return fmt.Errorf("input_json.rules is not valid")
	}
	if len(rulesList) == 0 {
		return fmt.Errorf("input_json.rules is empty")
	}
	var err error
	for index, ruleConfig := range rulesList {
		r, ok := ruleConfig.(map[string]interface{})
		if !ok {
			return fmt.Errorf("input_json.rules item is not map object")
		}
		rule := &Rule{}
		rule.Endpoint, err = getStringConfig(r, "endpoint", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d endpoint is not valid:%s", index, err)
		}
		rule.DB, err = getStringConfig(r, "db", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d db is not valid:%s", index, err)
		}
		rule.DBKey, err = getStringConfig(r, "dbKey", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d dbKey is not valid:%s", index, err)
		}
		rule.SuperTable, err = getStringConfig(r, "superTable", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d superTable is not valid:%s", index, err)
		}
		rule.SuperTableKey, err = getStringConfig(r, "superTableKey", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d superTableKey is not valid:%s", index, err)
		}
		rule.SubTable, err = getStringConfig(r, "subTable", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d subTable is not valid:%s", index, err)
		}
		rule.SubTableKey, err = getStringConfig(r, "subTableKey", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d subTableKey is not valid:%s", index, err)
		}
		rule.TimeKey, err = getStringConfig(r, "timeKey", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d timeKey is not valid:%s", index, err)
		}
		rule.TimeFormat, err = getStringConfig(r, "timeFormat", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d timeFormat is not valid:%s", index, err)
		}
		rule.Timezone, err = getStringConfig(r, "timezone", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d timezone is not valid:%s", index, err)
		}
		rule.Transformation, err = getStringConfig(r, "transformation", "")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d transformation is not valid:%s", index, err)
		}

		rule.TimeFieldName, err = getStringConfig(r, "timeFieldName", "ts")
		if err != nil {
			return fmt.Errorf("input_json.rules item index:%d timeFieldName is not valid:%s", index, err)
		}

		fields, exist := r["fields"]
		if !exist {
			return fmt.Errorf("input_json.rules item index:%d fields is missing", index)
		}
		fieldList, ok := fields.([]interface{})
		if !ok {
			return fmt.Errorf("input_json.rules item index:%d fields is not array", index)
		}

		for fieldIndex, fieldConfig := range fieldList {
			fieldMap, ok := fieldConfig.(map[string]interface{})
			if !ok {
				return fmt.Errorf("input_json.rules item index:%d, fields item index:%d is not map object", index, fieldIndex)
			}
			field, err := parseField(fieldMap)
			if err != nil {
				return fmt.Errorf("input_json.rules item index:%d, fields item index:%d is not valid:%s", index, fieldIndex, err)
			}
			rule.Fields = append(rule.Fields, field)
		}

		c.Rules = append(c.Rules, rule)
	}
	return nil
}

func getStringConfig(mapData map[string]interface{}, key string, defaultVal string) (string, error) {
	k := strings.ToLower(key)
	value, exist := mapData[k]
	if !exist {
		return defaultVal, nil
	}
	strValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s is not string, value: %v", key, value)
	}
	return strValue, nil
}

func getBoolConfig(mapData map[string]interface{}, key string) (bool, error) {
	k := strings.ToLower(key)
	value, exist := mapData[k]
	if !exist {
		return false, nil
	}
	boolValue, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%s is not bool, value: %v", key, value)
	}
	return boolValue, nil
}

func parseField(mapData map[string]interface{}) (*Field, error) {
	field := &Field{}
	var err error
	field.Key, err = getStringConfig(mapData, "key", "")
	if err != nil {
		return nil, err
	}
	field.Optional, err = getBoolConfig(mapData, "optional")
	if err != nil {
		return nil, err
	}
	return field, nil
}

func (c *Config) validate() error {
	if !c.Enable {
		return nil
	}
	if len(c.Rules) == 0 {
		return fmt.Errorf("input_json.rules is empty")
	}
	for _, rule := range c.Rules {
		if rule.Endpoint == "" {
			return fmt.Errorf("input_json.rules.endpoint is empty")
		}
		if !isValidEndpoint(rule.Endpoint) {
			return fmt.Errorf("input_json.rules.endpoint:%s is not valid", rule.Endpoint)
		}
		if rule.DB == "" && rule.DBKey == "" {
			return fmt.Errorf("endpoint:%s db and dbKey are empty", rule.Endpoint)
		}
		if rule.DB != "" && rule.DBKey != "" {
			return fmt.Errorf("endpoint:%s db and dbKey are both set", rule.Endpoint)
		}
		if rule.SuperTable == "" && rule.SuperTableKey == "" {
			return fmt.Errorf("endpoint:%s superTable and superTableKey are empty", rule.Endpoint)
		}
		if rule.SuperTable != "" && rule.SuperTableKey != "" {
			return fmt.Errorf("endpoint:%s superTable and superTableKey are both set", rule.Endpoint)
		}
		if rule.SubTable == "" && rule.SubTableKey == "" {
			return fmt.Errorf("endpoint:%s subTable and subTableKey are empty", rule.Endpoint)
		}
		if rule.SubTable != "" && rule.SubTableKey != "" {
			return fmt.Errorf("endpoint:%s subTable and subTableKey are both set", rule.Endpoint)
		}
		if rule.TimeKey != "" && rule.TimeFormat == "" {
			return fmt.Errorf("endpoint:%s timeFormat is empty", rule.Endpoint)
		}
		if !isValidIdentifier(rule.DB) {
			return fmt.Errorf("endpoint:%s db:%s is not valid", rule.Endpoint, rule.DB)
		}
		if !isValidIdentifier(rule.SuperTable) {
			return fmt.Errorf("endpoint:%s superTable:%s is not valid", rule.Endpoint, rule.SuperTable)
		}
		if len(rule.Fields) == 0 {
			return fmt.Errorf("endpoint:%s fields is empty", rule.Endpoint)
		}
		fieldKeyMap := make(map[string]struct{}, len(rule.Fields))
		for _, field := range rule.Fields {
			if field.Key == "" {
				return fmt.Errorf("endpoint:%s fields contains empty key", rule.Endpoint)
			}
			if !isValidIdentifier(field.Key) {
				return fmt.Errorf("endpoint:%s fields key:%s is not valid", rule.Endpoint, field.Key)
			}
			if _, exist := fieldKeyMap[field.Key]; exist {
				return fmt.Errorf("endpoint:%s fields contains duplicate key:%s", rule.Endpoint, field.Key)
			}
			fieldKeyMap[field.Key] = struct{}{}
		}
	}
	return nil
}

func isValidEndpoint(endpoint string) bool {
	if len(endpoint) == 0 {
		return false
	}

	for _, char := range endpoint {
		if !isLetter(char) && !isDigit(char) && char != '_' && char != '-' {
			return false
		}
	}
	return true
}

func isLetter(char rune) bool {
	return (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z')
}

func isDigit(char rune) bool {
	return char >= '0' && char <= '9'
}

func isValidIdentifier(name string) bool {
	if name == "" {
		return true
	}
	if strings.Contains(name, "`") {
		return false
	}
	return true
}
func initViper(v *viper.Viper) {
	v.SetDefault("input_json.enable", false)
}

func init() {
	initViper(viper.GetViper())
}
