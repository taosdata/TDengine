package inputjson

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bytedance/sonic/decoder"
	jsoniter "github.com/json-iterator/go"
	"github.com/minio/simdjson-go"
	"github.com/sirupsen/logrus"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/tidwall/gjson"
)

func TruncateLogBytes(msg []byte) []byte {
	if len(msg) > log.MaxLogSqlLength {
		return append(msg[:log.MaxLogSqlLength], []byte("...(truncated)")...)
	}
	return msg
}

func UnmarshalSonic(data []byte, v interface{}) error {
	dc := decoder.NewStreamDecoder(bytes.NewReader(data))
	dc.UseNumber()
	return dc.Decode(v)
}

var j = jsoniter.Config{
	UseNumber:              true,
	EscapeHTML:             true,
	SortMapKeys:            true,
	ValidateJsonRawMessage: true,
}.Froze()

func UnmarshalJsoniter(data []byte, v interface{}) error {
	return j.Unmarshal(data, v)
}

func parseRecordJsoniter(rule *ParsedRule, jsonData []byte, logger *logrus.Entry) ([]*record, error) {
	var result []map[string]interface{}
	err := UnmarshalJsoniter(jsonData, &result)
	if err != nil {
		logger.Errorf("error unmarshaling json data:%s, err:%s", jsonData, err)
		return nil, fmt.Errorf("error unmarshaling json data: %s", err)
	}
	return doParseRecord(rule, result, logger)
}
func parseRecordSonic(rule *ParsedRule, jsonData []byte, logger *logrus.Entry) ([]*record, error) {
	var result []map[string]interface{}
	err := UnmarshalSonic(jsonData, &result)
	if err != nil {
		logger.Errorf("error unmarshaling json data:%s, err:%s", jsonData, err)
		return nil, fmt.Errorf("error unmarshaling json data: %s", err)
	}
	return doParseRecord(rule, result, logger)
}
func parseRecordGJson(rule *ParsedRule, jsonData []byte, logger *logrus.Entry) ([]*record, error) {
	if !gjson.ValidBytes(jsonData) {
		logger.Errorf("invalid json data:%s", TruncateLogBytes(jsonData))
		return nil, fmt.Errorf("invalid json data")
	}
	result := gjson.ParseBytes(jsonData)
	if !result.IsArray() {
		logger.Errorf("expected JSON array, got: %s", TruncateLogBytes(jsonData))
		return nil, fmt.Errorf("expected JSON array, got: %s", TruncateLogBytes(jsonData))
	}
	records := make([]*record, 0, result.Get("#").Int())
	fieldBuilder := &strings.Builder{}
	valueBuilder := &strings.Builder{}
	timeBuf := make([]byte, 0, 35)

	result.ForEach(func(_, item gjson.Result) bool {
		rec, err := parseSingleRecordWithGJSON(rule, item, fieldBuilder, valueBuilder, timeBuf, logger)
		if err != nil {
			logger.Errorf("parse record error: %s", err)
			return false
		}
		records = append(records, rec)
		fieldBuilder.Reset()
		valueBuilder.Reset()
		return true
	})

	if len(records) != int(result.Get("#").Int()) {
		return nil, fmt.Errorf("failed to parse all records")
	}

	return records, nil
}

func parseSingleRecordWithGJSON(rule *ParsedRule, item gjson.Result,
	fieldBuilder, valueBuilder *strings.Builder, timeBuf []byte, logger *logrus.Entry) (*record, error) {

	rec := &record{}
	var err error

	// parse DB
	var db string
	if rule.DBKey != "" {
		dbResult := item.Get(rule.DBKey)
		if !dbResult.Exists() {
			logger.Errorf("db key %s not found in item: %v", rule.DBKey, item)
			return nil, fmt.Errorf("db key %s not found in item", rule.DBKey)
		}
		if dbResult.Type == gjson.Null {
			logger.Errorf("db key %s has nil value in item: %v", rule.DBKey, item)
			return nil, fmt.Errorf("db key %s has nil value in item", rule.DBKey)
		}
		db, err = castToStringGJSON(dbResult)
		if err != nil {
			logger.Errorf("cast db key %s to string error in item: %v, value: %v err:%s", rule.DBKey, item, dbResult.Raw, err)
			return nil, fmt.Errorf("cast db key %s to string error in item: %v, value: %v err:%s", rule.DBKey, item, dbResult.Raw, err)
		}
		db = escapeIdentifier(db)
		if db == "" {
			logger.Errorf("db key %s is empty in item: %v", rule.DBKey, item)
			return nil, fmt.Errorf("db key %s is empty in item", rule.DBKey)
		}
	} else {
		db = rule.DB
	}

	// parse SuperTable
	var stb string
	if rule.SuperTableKey != "" {
		stbResult := item.Get(rule.SuperTableKey)
		if !stbResult.Exists() {
			logger.Errorf("super_table key %s not found in item: %v", rule.SuperTableKey, item)
			return nil, fmt.Errorf("super table key %s not found in item", rule.SuperTableKey)
		}
		if stbResult.Type == gjson.Null {
			logger.Errorf("super_table key %s has nil value in item: %v", rule.SuperTableKey, item)
			return nil, fmt.Errorf("super table key %s has nil value in item", rule.SuperTableKey)
		}
		stb, err = castToStringGJSON(stbResult)
		if err != nil {
			logger.Errorf("cast super table key %s to string error in item: %v, value: %v err:%s", rule.SuperTableKey, item, stbResult.Raw, err)
			return nil, fmt.Errorf("cast super table key %s to string error in item: %v, value: %v err:%s", rule.SuperTableKey, item, stbResult.Raw, err)
		}
		stb = escapeIdentifier(stb)
		if stb == "" {
			logger.Errorf("super table key %s is empty in item: %v", rule.SuperTableKey, item)
			return nil, fmt.Errorf("super table key %s is empty in item", rule.SuperTableKey)
		}
	} else {
		stb = rule.SuperTable
	}
	rec.stb = fmt.Sprintf("`%s`.`%s`", db, stb)

	// parse SubTable
	if rule.SubTableKey != "" {
		subTableResult := item.Get(rule.SubTableKey)
		if !subTableResult.Exists() {
			logger.Errorf("sub table key %s not found in item: %v", rule.SubTableKey, item)
			return nil, fmt.Errorf("sub table key %s not found in item", rule.SubTableKey)
		}
		if subTableResult.Type == gjson.Null {
			logger.Errorf("sub table key %s has nil value in item: %v", rule.SubTableKey, item)
			return nil, fmt.Errorf("sub table key %s has nil value in item", rule.SubTableKey)
		}
		rec.subTable, err = castToStringGJSON(subTableResult)
		if err != nil {
			logger.Errorf("cast sub table key %s to string error in item: %v, value: %v err:%s", rule.SubTableKey, item, subTableResult, err)
			return nil, fmt.Errorf("cast sub table key %s to string error in item: %v, value: %v err:%s", rule.SubTableKey, item, subTableResult, err)
		}
		rec.subTable = escapeIdentifier(rec.subTable)
		if rec.subTable == "" {
			logger.Errorf("sub table key %s is empty in item: %v", rule.SubTableKey, item)
			return nil, fmt.Errorf("sub table key %s is empty in item", rule.SubTableKey)
		}
	} else {
		rec.subTable = rule.SubTable
	}

	// tableName part
	valueBuilder.WriteByte('\'')
	writeEscapeStringValue(valueBuilder, rec.subTable)
	valueBuilder.WriteByte('\'')

	// parse Time
	if rule.TimeKey != "" {
		timeResult := item.Get(rule.TimeKey)
		if !timeResult.Exists() {
			logger.Errorf("time key %s not found in item: %v", rule.TimeKey, item)
			return nil, fmt.Errorf("time key %s not found in item", rule.TimeKey)
		}
		if timeResult.Type == gjson.Null {
			logger.Errorf("time key %s has nil value in item: %v", rule.TimeKey, item)
			return nil, fmt.Errorf("time key %s has nil value in item", rule.TimeKey)
		}
		timeStr, err := castToStringGJSON(timeResult)
		if err != nil {
			logger.Errorf("cast time key %s to string error in item: %v, value: %v err:%s", rule.TimeKey, item, timeResult, err)
			return nil, fmt.Errorf("cast time key %s to string error in item: %v, value: %v err:%s", rule.TimeKey, item, timeResult, err)
		}
		rec.ts, err = parseTime(timeStr, rule.TimeFormat, rule.Timezone)
		if err != nil {
			logger.Errorf("parse time error:%s, raw_time:%s format:%s", err, timeStr, rule.TimeFormat)
			return nil, fmt.Errorf("parse time error:%s, raw_time:%s format:%s", err, timeStr, rule.TimeFormat)
		}
	} else {
		rec.ts = time.Now()
	}

	// time part
	valueBuilder.WriteString(",'")
	timeBuf = timeBuf[:0]
	timeBuf = rec.ts.AppendFormat(timeBuf, time.RFC3339Nano)
	valueBuilder.Write(timeBuf)
	valueBuilder.WriteByte('\'')

	// parse Fields
	inCompleteValue := false
	for index, fieldKey := range rule.FieldKeys {
		fieldResult := item.Get(fieldKey)

		if !fieldResult.Exists() {
			if rule.FieldOptionals[index] {
				if !inCompleteValue {
					fieldBuilder.Reset()
					generateFieldKeyStatement(fieldBuilder, rule, index)
					inCompleteValue = true
					continue
				}
			} else {
				logger.Errorf("field key %s not found in item: %v", fieldKey, item)
				return nil, fmt.Errorf("field key %s not found in json", fieldKey)
			}
		}

		if inCompleteValue {
			writeFieldName(fieldBuilder, fieldKey)
		}

		valueBuilder.WriteByte(',')
		err = writeValueFromGJSON(valueBuilder, fieldResult)
		if err != nil {
			logger.Errorf("write field to buffer error error:%s, field:%s, err:%s", fieldKey, fieldKey, err)
			return nil, fmt.Errorf("write field to buffer error:%s, field:%s, err:%s", fieldKey, fieldKey, err)
		}
	}

	if inCompleteValue {
		rec.keys = fieldBuilder.String()
	} else {
		rec.keys = rule.SqlAllColumns
	}
	rec.values = valueBuilder.String()

	return rec, nil
}

func writeValueFromGJSON(builder *strings.Builder, result gjson.Result) error {
	switch result.Type {
	case gjson.String:
		builder.WriteByte('\'')
		writeEscapeStringValue(builder, result.Str)
		builder.WriteByte('\'')
	case gjson.Number:
		builder.WriteString(result.Raw)
	case gjson.True:
		builder.WriteString("true")
	case gjson.False:
		builder.WriteString("false")
	case gjson.Null:
		builder.WriteString("null")
	default:
		return fmt.Errorf("unsupported value type: %s", result.Type)
	}
	return nil
}

func castToStringGJSON(result gjson.Result) (string, error) {
	switch result.Type {
	case gjson.String:
		return result.Str, nil
	case gjson.Number:

		return result.Raw, nil
	case gjson.True:
		return "true", nil
	case gjson.False:
		return "false", nil
	case gjson.Null:
		return "null", nil
	default:
		return "", fmt.Errorf("unsupported value type: %s", result.Type)
	}
}

func parseRecordSimdJson(rule *ParsedRule, jsonData []byte, logger *logrus.Entry) ([]*record, error) {
	parsed, err := simdjson.Parse(jsonData, nil)
	if err != nil {
		logger.Errorf("error parsing json data:%s, err:%s", jsonData, err)
		return nil, fmt.Errorf("error parsing json data: %s", err)
	}

	iter := parsed.Iter()
	if iter.Advance() != simdjson.TypeRoot {
		logger.Errorf("expected root element")
		return nil, fmt.Errorf("expected root element")
	}

	rootType, root, err := iter.Root(nil)
	if err != nil {
		logger.Errorf("error getting root element: %s", err)
		return nil, fmt.Errorf("error getting root element: %s", err)
	}
	if rootType != simdjson.TypeArray {
		logger.Errorf("expected json array")
		return nil, fmt.Errorf("expected json array")
	}

	arrayIter, err := root.Array(nil)
	if err != nil {
		logger.Errorf("error getting array iterator: %s", err)
		return nil, fmt.Errorf("error getting array iterator: %s", err)
	}

	return doParseRecordSIMD(rule, arrayIter, logger)
}

func doParseRecordSIMD(rule *ParsedRule, arrayIter *simdjson.Array, logger *logrus.Entry) ([]*record, error) {
	var records []*record
	fieldBuilder := &strings.Builder{}
	valueBuilder := &strings.Builder{}
	timeBuf := make([]byte, 0, 35)
	var err error
	var obj *simdjson.Object
	i := arrayIter.Iter()
	for {
		t := i.Advance()
		if t == simdjson.TypeNone {
			break
		}
		objType := i.Type()
		if objType != simdjson.TypeObject {
			logger.Errorf("expected json object")
			err = fmt.Errorf("expected json object")
			return nil, err
		}

		obj, err = i.Object(nil)
		if err != nil {
			logger.Errorf("error getting object from iterator: %s", err)
			return nil, err
		}

		rec := &record{}

		// parse DB
		var db string
		if rule.DBKey != "" {
			elem := obj.FindKey(rule.DBKey, nil)
			if elem == nil {
				err = fmt.Errorf("db key %s not found in item", rule.DBKey)
				logger.Errorf("db key %s not found in item", rule.DBKey)
				return nil, err
			}

			if elem.Type == simdjson.TypeNull {
				err = fmt.Errorf("db key %s has nil value in item", rule.DBKey)
				logger.Errorf("db key %s has nil value in item", rule.DBKey)
				return nil, err
			}

			db, err = simdjsonCastToString(elem)
			if err != nil {
				err = fmt.Errorf("cast db key %s to string error: %s", rule.DBKey, err)
				logger.Errorf("cast db key %s to string error, err:%s", rule.DBKey, err)
				return nil, err
			}

			db = escapeIdentifier(db)
			if db == "" {
				err = fmt.Errorf("db key %s is empty in item", rule.DBKey)
				logger.Errorf("db key %s is empty in item", rule.DBKey)
				return nil, err
			}
		} else {
			db = rule.DB
		}

		// parse SuperTable
		var stb string
		if rule.SuperTableKey != "" {
			elem := obj.FindKey(rule.SuperTableKey, nil)
			if elem == nil {
				err = fmt.Errorf("super table key %s not found in item", rule.SuperTableKey)
				logger.Errorf("super table key %s not found in item", rule.SuperTableKey)
				return nil, err
			}

			if elem.Type == simdjson.TypeNull {
				err = fmt.Errorf("super table key %s has nil value in item", rule.SuperTableKey)
				logger.Errorf("super table key %s has nil value in item", rule.SuperTableKey)
				return nil, err
			}

			stb, err = simdjsonCastToString(elem)
			if err != nil {
				err = fmt.Errorf("cast super table key %s to string error: %s", rule.SuperTableKey, err)
				logger.Errorf("cast super table key %s to string error, err:%s", rule.SuperTableKey, err)
				return nil, err
			}

			stb = escapeIdentifier(stb)
			if stb == "" {
				err = fmt.Errorf("super table key %s is empty in item", rule.SuperTableKey)
				logger.Errorf("super table key %s is empty in item", rule.SuperTableKey)
				return nil, err
			}
		} else {
			stb = rule.SuperTable
		}
		rec.stb = fmt.Sprintf("`%s`.`%s`", db, stb)

		// parse SubTable
		if rule.SubTableKey != "" {
			elem := obj.FindKey(rule.SubTableKey, nil)
			if elem == nil {
				err = fmt.Errorf("sub table key %s not found in item", rule.SubTableKey)
				logger.Errorf("sub table key %s not found in item", rule.SubTableKey)
				return nil, err
			}

			if elem.Type == simdjson.TypeNull {
				err = fmt.Errorf("sub table key %s has nil value in item", rule.SubTableKey)
				logger.Errorf("sub table key %s has nil value in item", rule.SubTableKey)
				return nil, err
			}

			rec.subTable, err = simdjsonCastToString(elem)
			if err != nil {
				err = fmt.Errorf("cast sub table key %s to string error: %s", rule.SubTableKey, err)
				logger.Errorf("cast sub table key %s to string error, err:%s", rule.SubTableKey, err)
				return nil, err
			}
			if rec.subTable == "" {
				err = fmt.Errorf("sub table key %s is empty in item", rule.SubTableKey)
				logger.Errorf("sub table key %s is empty in item", rule.SubTableKey)
				return nil, err
			}
		} else {
			rec.subTable = rule.SubTable
		}
		// tableName part
		valueBuilder.Reset()
		valueBuilder.WriteByte('\'')
		writeEscapeStringValue(valueBuilder, rec.subTable)
		valueBuilder.WriteByte('\'')

		// parse Time
		if rule.TimeKey != "" {
			elem := obj.FindKey(rule.TimeKey, nil)
			if elem == nil {
				logger.Errorf("time key %s not found in item", rule.TimeKey)
				err = fmt.Errorf("time key %s not found in item", rule.TimeKey)
				return nil, err
			}

			if elem.Type == simdjson.TypeNull {
				logger.Errorf("time key %s has nil value in item", rule.TimeKey)
				err = fmt.Errorf("time key %s has nil value in item", rule.TimeKey)
				return nil, err
			}

			timeStr, err := simdjsonCastToString(elem)
			if err != nil {
				logger.Errorf("cast time key %s to string error, err:%s", rule.TimeKey, err)
				err = fmt.Errorf("cast time key %s to string error: %s", rule.TimeKey, err)
				return nil, err
			}

			rec.ts, err = parseTime(timeStr, rule.TimeFormat, rule.Timezone)
			if err != nil {
				logger.Errorf("parse time error:%s, raw_time:%s format:%s", err, timeStr, rule.TimeFormat)
				err = fmt.Errorf("parse time error:%s, raw_time:%s format:%s", err, timeStr, rule.TimeFormat)
				return nil, err
			}
		} else {
			rec.ts = time.Now()
		}

		// time part
		valueBuilder.WriteString(",'")
		timeBuf = timeBuf[:0]
		timeBuf = rec.ts.AppendFormat(timeBuf, time.RFC3339Nano)
		valueBuilder.Write(timeBuf)
		valueBuilder.WriteByte('\'')

		// parse Fields
		inCompleteValue := false
		for index, fieldKey := range rule.FieldKeys {
			elem := obj.FindKey(fieldKey, nil)
			if elem == nil {
				if rule.FieldOptionals[index] {
					if !inCompleteValue {
						fieldBuilder.Reset()
						generateFieldKeyStatement(fieldBuilder, rule, index)
						inCompleteValue = true
					}
					continue
				} else {
					logger.Errorf("field key %s not found in item", fieldKey)
					err = fmt.Errorf("field key %s not found in item", fieldKey)
					return nil, err
				}
			}

			if inCompleteValue {
				writeFieldName(fieldBuilder, fieldKey)
			}

			valueBuilder.WriteByte(',')
			err = simdjsonWriteValue(valueBuilder, elem)
			if err != nil {
				logger.Errorf("write field to buffer error error:%s, field:%s, err:%s", fieldKey, fieldKey, err)
				err = fmt.Errorf("write field to buffer error:%s, field:%s, err:%s", fieldKey, fieldKey, err)
				return nil, err
			}
		}

		if inCompleteValue {
			rec.keys = fieldBuilder.String()
			fieldBuilder.Reset()
		} else {
			rec.keys = rule.SqlAllColumns
		}
		rec.values = valueBuilder.String()
		valueBuilder.Reset()
		records = append(records, rec)
	}
	return records, nil
}

func simdjsonCastToString(elem *simdjson.Element) (string, error) {
	return elem.Iter.StringCvt()
}
func simdjsonWriteValue(builder *strings.Builder, elem *simdjson.Element) error {
	str, err := elem.Iter.StringCvt()
	if err != nil {
		return err
	}
	switch elem.Type {
	case simdjson.TypeString:
		builder.WriteByte('\'')
		writeEscapeStringValue(builder, str)
		builder.WriteByte('\'')
	default:
		builder.WriteString(str)
	}
	return nil
}

var benchParsedRules = &ParsedRule{
	TransformationExpr:   nil,
	DBKey:                "db",
	SuperTableKey:        "stb",
	SubTableKey:          "tb",
	TimeKey:              "ts",
	TimeFormat:           "unix_ms",
	Timezone:             time.UTC,
	TimeFieldName:        "ts",
	SqlAllColumns:        "`ts`,`t1`,`t2`,`c1`,`c2`,`c3`",
	FieldKeys:            []string{"t1", "t2", "c1", "c2", "c3"},
	FieldOptionals:       []bool{false, false, false, false, false},
	TransformationString: "",
}

type BenchData struct {
	Db  string  `json:"db"`
	Stb string  `json:"stb"`
	Tb  string  `json:"tb"`
	T1  int     `json:"t1"`
	T2  string  `json:"t2"`
	Ts  int64   `json:"ts"`
	C1  float64 `json:"c1"`
	C2  int     `json:"c2"`
	C3  float64 `json:"c3"`
}

var benchData []byte

func GenerateBenchData() ([]byte, error) {
	if benchData != nil {
		return benchData, nil
	}
	result := make([]*BenchData, 10000)
	for i := 0; i < 10000; i++ {
		result[i] = &BenchData{
			Db:  "testdb",
			Stb: "sensortable",
			Tb:  fmt.Sprintf("sensor_%d", i%100),
			T1:  i,
			T2:  fmt.Sprintf("sensor_name_%d", i),
			Ts:  time.Now().UnixMilli(),
			C1:  float64(i) * 1.1,
			C2:  i * 10,
			C3:  float64(i) * 0.1,
		}
	}
	bs, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	benchData = bs
	return benchData, nil
}

func BenchmarkParseRecordStd(b *testing.B) {
	logger := log.GetLogger("test").WithField("benchmark", "std")
	bs, err := GenerateBenchData()
	if err != nil {
		b.Error(err)
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := parseRecord(benchParsedRules, bs, logger)
		if err != nil {
			b.Error(err)
			return
		}
		_ = rs
	}
}

func BenchmarkParseRecordJsoniter(b *testing.B) {
	logger := log.GetLogger("test").WithField("benchmark", "jsoniter")
	bs, err := GenerateBenchData()
	if err != nil {
		b.Error(err)
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := parseRecordJsoniter(benchParsedRules, bs, logger)
		if err != nil {
			b.Error(err)
			return
		}
		_ = rs
	}
}

func BenchmarkParseRecordSonic(b *testing.B) {
	logger := log.GetLogger("test").WithField("benchmark", "sonic")
	bs, err := GenerateBenchData()
	if err != nil {
		b.Error(err)
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := parseRecordSonic(benchParsedRules, bs, logger)
		if err != nil {
			b.Error(err)
			return
		}
		_ = rs
	}
}

func BenchmarkParseRecordGJson(b *testing.B) {
	logger := log.GetLogger("test").WithField("benchmark", "gjson")
	bs, err := GenerateBenchData()
	if err != nil {
		b.Error(err)
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := parseRecordGJson(benchParsedRules, bs, logger)
		if err != nil {
			b.Error(err)
			return
		}
		_ = rs
	}
}

func BenchmarkParseRecordSimdJson(b *testing.B) {
	logger := log.GetLogger("test").WithField("benchmark", "simdjson")
	bs, err := GenerateBenchData()
	if err != nil {
		b.Error(err)
		return
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := parseRecordSimdJson(benchParsedRules, bs, logger)
		if err != nil {
			b.Error(err)
			return
		}
		_ = rs
	}
}
