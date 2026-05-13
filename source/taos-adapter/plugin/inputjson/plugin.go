package inputjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/blues/jsonata-go"
	"github.com/gin-gonic/gin"
	"github.com/ncruces/go-strftime"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/taosdata/taosadapter/v3/config"
	"github.com/taosdata/taosadapter/v3/db/async"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	errors2 "github.com/taosdata/taosadapter/v3/driver/errors"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/monitor"
	"github.com/taosdata/taosadapter/v3/plugin"
	"github.com/taosdata/taosadapter/v3/tools/connectpool"
	"github.com/taosdata/taosadapter/v3/tools/generator"
	"github.com/taosdata/taosadapter/v3/tools/iptool"
)

const MAXSQLLength = 1024 * 1024 * 1

var logger = log.GetLogger("PLG").WithField("mod", "input_json")

type Plugin struct {
	conf    Config
	rules   map[string]*ParsedRule
	metrics map[string]*monitor.InputJsonMetric
}

type ParsedRule struct {
	TransformationExpr   *jsonata.Expr
	DB                   string
	DBKey                string
	SuperTable           string
	SuperTableKey        string
	SubTable             string
	SubTableKey          string
	TimeKey              string
	TimeFormat           string
	Timezone             *time.Location
	TimeFieldName        string
	SqlAllColumns        string
	FieldKeys            []string
	FieldOptionals       []bool
	TransformationString string
}

func (p *Plugin) Init(r gin.IRouter) error {
	return p.initWithViper(r, viper.GetViper())
}

func (p *Plugin) initWithViper(r gin.IRouter, v *viper.Viper) error {
	err := p.conf.setValue(v)
	if err != nil {
		return err
	}
	if !p.conf.Enable {
		logger.Debug("input_json disabled")
		return nil
	}
	err = p.conf.validate()
	if err != nil {
		return err
	}
	err = p.ParseRulesFromConfig()
	if err != nil {
		return fmt.Errorf("parse rules from config failed: %s", err)
	}
	p.metrics = make(map[string]*monitor.InputJsonMetric, len(p.rules))
	for endpoint := range p.rules {
		p.metrics[endpoint] = monitor.NewInputJsonMetric(endpoint)
	}
	r.POST(":endpoint", plugin.Auth(errorResponse), p.HandleRequest)
	return nil
}

func (p *Plugin) ParseRulesFromConfig() error {
	p.rules = make(map[string]*ParsedRule, len(p.conf.Rules))
	builder := &strings.Builder{}
	for _, rule := range p.conf.Rules {
		var e *jsonata.Expr
		var err error
		if rule.Transformation != "" {
			// compile jsonata expression
			e, err = jsonata.Compile(rule.Transformation)
			if err != nil {
				return fmt.Errorf("error compiling jsonata expression: %s, expr: %s", err, rule.Transformation)
			}
		}
		format := rule.TimeFormat
		if !isPredefinedTimeFormat(rule.TimeFormat) {
			// convert strftime format
			format, err = strftime.Layout(rule.TimeFormat)
			if err != nil {
				return fmt.Errorf("error compiling strftime time format: %v, format: %s", err, rule.TimeFormat)
			}
		}
		loc := time.Local
		if rule.Timezone != "" {
			// load location
			loc, err = time.LoadLocation(rule.Timezone)
			if err != nil {
				return fmt.Errorf("error loading time location: %v, timezone: %s", err, rule.Timezone)
			}
		}
		fieldKeys := make([]string, len(rule.Fields))
		fieldOptionals := make([]bool, len(rule.Fields))
		for i, field := range rule.Fields {
			fieldKeys[i] = field.Key
			fieldOptionals[i] = field.Optional
		}

		parsedRule := &ParsedRule{
			TransformationExpr:   e,
			DB:                   rule.DB,
			DBKey:                rule.DBKey,
			SuperTable:           rule.SuperTable,
			SuperTableKey:        rule.SuperTableKey,
			SubTable:             rule.SubTable,
			SubTableKey:          rule.SubTableKey,
			TimeKey:              rule.TimeKey,
			TimeFormat:           format,
			Timezone:             loc,
			TimeFieldName:        rule.TimeFieldName,
			FieldKeys:            fieldKeys,
			FieldOptionals:       fieldOptionals,
			TransformationString: rule.Transformation,
		}
		builder.Reset()
		generateFieldKeyStatement(builder, parsedRule, len(rule.Fields))
		parsedRule.SqlAllColumns = builder.String()
		builder.Reset()
		p.rules[rule.Endpoint] = parsedRule
	}

	return nil
}

type record struct {
	stb      string
	subTable string
	ts       time.Time
	keys     string
	values   string
}
type dryRunResp struct {
	Code int      `json:"code"`
	Desc string   `json:"desc"`
	Json string   `json:"json"`
	Sql  []string `json:"sql"`
}

type message struct {
	Code     int    `json:"code"`
	Desc     string `json:"desc"`
	Affected int    `json:"affected,omitempty"`
}

func errorResponse(c *gin.Context, httpCode int, err error) {
	var taosErr *errors2.TaosError
	errCode := 0xffff
	errMessage := ""
	ok := errors.As(err, &taosErr)
	if ok {
		respCode, exists := config.ErrorStatusMap[taosErr.Code]
		if exists {
			httpCode = respCode
		}
		errCode = int(taosErr.Code)
		errMessage = taosErr.ErrStr
	} else {
		if errors.Is(err, commonpool.ErrWhitelistForbidden) {
			httpCode = http.StatusForbidden
			errMessage = fmt.Sprintf("whitelist forbidden: %s", err)
		} else if errors.Is(err, connectpool.ErrTimeout) || errors.Is(err, connectpool.ErrMaxWait) {
			httpCode = http.StatusGatewayTimeout
			errMessage = fmt.Sprintf("get connect from pool error: %s", err)
		} else {
			errMessage = err.Error()
		}
	}

	errorRespWithErrorCode(c, httpCode, errCode, errMessage)
}

func errorRespWithErrorCode(c *gin.Context, httpCode int, errCode int, desc string) {
	c.JSON(httpCode, message{
		Code: errCode,
		Desc: desc,
	})
}

func successResponse(c *gin.Context, affectedRows int) {
	c.JSON(http.StatusOK, message{
		Code:     0,
		Affected: affectedRows,
	})
}

func (p *Plugin) HandleRequest(c *gin.Context) {
	endpoint := c.Param("endpoint")
	rule, ok := p.rules[endpoint]
	if !ok {
		logger.Errorf("no rule found for endpoint: %s", endpoint)
		errorResponse(c, http.StatusNotFound, fmt.Errorf("no rule found for endpoint: %s", endpoint))
		return
	}
	var reqID int64
	var err error
	if reqIDStr := c.Query("req_id"); len(reqIDStr) != 0 {
		if reqID, err = strconv.ParseInt(reqIDStr, 10, 64); err != nil {
			logger.Errorf("illegal param, req_id must be numeric:%s, err:%s", reqIDStr, err)
			errorResponse(c, http.StatusBadRequest, fmt.Errorf("illegal param, req_id must be numeric %s", err))
			return
		}
	}
	if reqID == 0 {
		reqID = generator.GetReqID()
		logger.Tracef("request:%s, client_ip:%s, req_id not set, generate new QID:0x%x", c.Request.RequestURI, c.ClientIP(), reqID)
	}
	c.Set(config.ReqIDKey, reqID)
	logger := logger.WithField(config.ReqIDKey, reqID)
	isDebug := log.IsDebug()
	dryRun := c.Query("dry_run") == "true"
	if dryRun {
		logger.Debug("dry_run mode enabled, no data will be inserted")
	}
	user, password, token, err := plugin.GetAuth(c)
	if err != nil {
		logger.Errorf("get auth error:%s", err)
		errorResponse(c, http.StatusUnauthorized, fmt.Errorf("get auth error: %s", err))
		return
	}
	s := log.GetLogNow(isDebug)
	taosConn, err := commonpool.GetConnection(user, password, token, iptool.GetRealIP(c.Request))
	logger.Debugf("get connection finish, cost:%s", log.GetLogDuration(isDebug, s))
	if err != nil {
		logger.Errorf("get connection from pool error, err:%s", err)
		errorResponse(c, http.StatusInternalServerError, err)
		return
	}
	if dryRun {
		logger.Tracef("put connection")
		_ = taosConn.Put()
	} else {
		defer func() {
			logger.Tracef("put connection")
			_ = taosConn.Put()
		}()
	}
	// transform json payload if transformation is defined
	var jsonData []byte
	jsonData, err = c.GetRawData()
	if err != nil {
		logger.Errorf("get raw data error:%s", err)
		errorResponse(c, http.StatusBadRequest, fmt.Errorf("error reading request body: %s", err))
		return
	}
	transformedJsonData := jsonData
	if rule.TransformationExpr != nil {
		// precision loss may occur here. e.g. large int64 may be converted to float64
		// it's recommended to use string for large int64 values in json

		// if the result is a singleton array, extract the value inside the array, see
		// https://docs.jsonata.org/predicate#singleton-array-and-value-equivalence
		// so that users should use [] in jsonata expression to generate array of records,
		// e.g. use $each($, function($v,$k) { ... })[] instead of $each($, function($v,$k) { ... })
		start := log.GetLogNow(isDebug)
		transformedJsonData, err = rule.TransformationExpr.EvalBytes(jsonData)
		logger.Debugf("transformed json cost: %s", log.GetLogDuration(isDebug, start))
		if err != nil {
			logger.Errorf("transform json data error:%s, input_json:%s, transformation:%s", err, jsonData, rule.TransformationString)
			errorResponse(c, http.StatusBadRequest, fmt.Errorf("transform json data error: %s", err))
			return
		}
	}
	// parse jsonData according to rule
	start := log.GetLogNow(isDebug)
	records, err := parseRecord(rule, transformedJsonData, logger)
	logger.Debugf("parse records cost: %s", log.GetLogDuration(isDebug, start))
	if err != nil {
		logger.Errorf("parse records error:%s", err)
		errorResponse(c, http.StatusBadRequest, fmt.Errorf("error parsing records: %s", err))
		return
	}
	metric := p.metrics[endpoint]
	recordCount := float64(len(records))
	if metric != nil && !dryRun {
		metric.TotalRows.Add(recordCount)
		metric.InflightRows.Add(recordCount)
	}
	start = log.GetLogNow(isDebug)
	sqlArray, err := generateSql(records, MAXSQLLength)
	logger.Debugf("generate sql cost: %s", log.GetLogDuration(isDebug, start))
	logger.Tracef("generate sql array: %v", sqlArray)
	if err != nil {
		logger.Errorf("generate sql error:%s", err)
		errorResponse(c, http.StatusBadRequest, fmt.Errorf("generate sql error: %s", err))
	}
	if dryRun {
		resp := dryRunResp{
			Json: string(transformedJsonData),
			Sql:  sqlArray,
		}
		c.JSON(200, resp)
	} else {
		start = log.GetLogNow(isDebug)
		affectedRows, err := execute(taosConn.TaosConnection, reqID, sqlArray, logger, isDebug)
		logger.Debugf("execute sql cost: %s", log.GetLogDuration(isDebug, start))
		if metric != nil {
			metric.InflightRows.Sub(recordCount)
		}
		if err != nil {
			logger.Errorf("execute sql error:%s", err)
			if metric != nil {
				metric.FailRows.Add(recordCount)
			}
			errorResponse(c, http.StatusInternalServerError, err)
			return
		}
		if metric != nil {
			metric.SuccessRows.Add(recordCount)
			metric.AffectedRows.Add(recordCount)
		}
		logger.Tracef("insert success, affected rows:%d", affectedRows)
		successResponse(c, affectedRows)
	}
}

// parseRecord parses the json data into records according to the rule
// 1. If dbKey, superTableKey, subTableKey and timeKey are defined in the rule, it will use the values from the json data, otherwise it will use the static values from the rule.
// 2. If dbKey, superTableKey, subTableKey and timeKey are defined but not found in the json data, it will return an error.
// 3. If the value is invalid (nil, empty string, other unsupported types), it will return an error as well, if the value is boolean or number, it will be converted to string.
// 4. If timeKey is not defined, it will use the current time.
// 5. If field key is not found in the json data, it will return an error, unless the field is marked as optional in the rule.
func parseRecord(rule *ParsedRule, jsonData []byte, logger *logrus.Entry) ([]*record, error) {
	var result []map[string]interface{}
	err := Unmarshal(jsonData, &result)
	if err != nil {
		logger.Errorf("error unmarshaling json data:%s, err:%s", jsonData, err)
		return nil, fmt.Errorf("error unmarshaling json data: %s", err)
	}
	return doParseRecord(rule, result, logger)
}

func doParseRecord(rule *ParsedRule, result []map[string]interface{}, logger *logrus.Entry) ([]*record, error) {
	records := make([]*record, len(result))
	fieldBuilder := &strings.Builder{}
	valueBuilder := &strings.Builder{}
	timeBuf := make([]byte, 0, 35)
	var err error
	for i, item := range result {
		rec := &record{}
		// parse DB
		var db string
		if rule.DBKey != "" {
			v, ok := item[rule.DBKey]
			if !ok {
				logger.Errorf("db key %s not found in item: %v", rule.DBKey, item)
				return nil, fmt.Errorf("db key %s not found in item", rule.DBKey)
			}
			if v == nil {
				logger.Errorf("db key %s has nil value in item: %v", rule.DBKey, item)
				return nil, fmt.Errorf("db key %s has nil value in item", rule.DBKey)
			}
			db, err = castToString(v)
			if err != nil {
				logger.Errorf("cast db key %s to string error in item: %v, value: %v err:%s", rule.DBKey, item, v, err)
				return nil, fmt.Errorf("cast db key %s to string error in item: %v, value: %v err:%s", rule.DBKey, item, v, err)
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
			v, ok := item[rule.SuperTableKey]
			if !ok {
				logger.Errorf("super table key %s not found in item: %v", rule.SuperTableKey, item)
				return nil, fmt.Errorf("super table key %s not found in item", rule.SuperTableKey)
			}
			if v == nil {
				logger.Errorf("super table key %s has nil value in item: %v", rule.SuperTableKey, item)
				return nil, fmt.Errorf("super table key %s has nil value in item", rule.SuperTableKey)
			}
			stb, err = castToString(v)
			if err != nil {
				logger.Errorf("cast super table key %s to string error in item: %v, value: %v err:%s", rule.SuperTableKey, item, v, err)
				return nil, fmt.Errorf("cast super table key %s to string error in item: %v, value: %v err:%s", rule.SuperTableKey, item, v, err)
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
			v, ok := item[rule.SubTableKey]
			if !ok {
				logger.Errorf("sub table key %s not found in item: %v", rule.SubTableKey, item)
				return nil, fmt.Errorf("sub table key %s not found in item", rule.SubTableKey)
			}
			if v == nil {
				logger.Errorf("sub table key %s has nil value in item: %v", rule.SubTableKey, item)
				return nil, fmt.Errorf("sub table key %s has nil value in item", rule.SubTableKey)
			}
			rec.subTable, err = castToString(v)
			if err != nil {
				logger.Errorf("cast sub table key %s to string error in item: %v, value: %v err:%s", rule.SubTableKey, item, v, err)
				return nil, fmt.Errorf("cast sub table key %s to string error in item: %v, value: %v err:%s", rule.SubTableKey, item, v, err)
			}
			if rec.subTable == "" {
				logger.Errorf("sub table key %s is empty in item: %v", rule.SubTableKey, item)
				return nil, fmt.Errorf("sub table key %s is empty in item", rule.SubTableKey)
			}
		} else {
			rec.subTable = rule.SubTable
		}
		// tableName part
		// e.g. 'sub_table_1'
		valueBuilder.Reset()
		valueBuilder.WriteByte('\'')
		writeEscapeStringValue(valueBuilder, rec.subTable)
		valueBuilder.WriteByte('\'')
		// parse Time
		if rule.TimeKey != "" {
			v, ok := item[rule.TimeKey]
			if !ok {
				logger.Errorf("time key %s not found in item: %v", rule.TimeKey, item)
				return nil, fmt.Errorf("time key %s not found in item", rule.TimeKey)
			}
			if v == nil {
				logger.Errorf("time key %s has nil value in item: %v", rule.TimeKey, item)
				return nil, fmt.Errorf("time key %s has nil value in item", rule.TimeKey)
			}
			timeStr, err := castToString(v)
			if err != nil {
				logger.Errorf("cast time key %s to string error in item: %v, value: %v err:%s", rule.TimeKey, item, v, err)
				return nil, fmt.Errorf("cast time key %s to string error in item: %v, value: %v err:%s", rule.TimeKey, item, v, err)
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
		// e.g. ,'2024-06-10T15:04:05.999999999Z'
		valueBuilder.WriteString(",'")
		timeBuf = timeBuf[:0]
		timeBuf = rec.ts.AppendFormat(timeBuf, time.RFC3339Nano)
		valueBuilder.Write(timeBuf)
		valueBuilder.WriteByte('\'')
		// parse Fields
		inCompleteValue := false
		for index, fieldKey := range rule.FieldKeys {
			v, ok := item[fieldKey]
			if !ok {
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
			err = writeValue(valueBuilder, v)
			if err != nil {
				logger.Errorf("write field to buffer error error:%s, field:%s, err:%s", fieldKey, fieldKey, err)
				return nil, fmt.Errorf("write field to buffer error:%s, field:%s, err:%s", fieldKey, fieldKey, err)
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
		records[i] = rec
	}
	return records, nil
}

func castToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	case bool:
		return fmt.Sprintf("%v", v), nil
	default:
		return "", fmt.Errorf("unsupported value type: %T,value: %v", value, value)
	}
}

func writeValue(builder *strings.Builder, value interface{}) error {
	if value == nil {
		builder.WriteString("null")
		return nil
	}
	switch v := value.(type) {
	case string:
		builder.WriteByte('\'')
		writeEscapeStringValue(builder, v)
		builder.WriteByte('\'')
	case json.Number:
		builder.WriteString(v.String())
	case bool:
		if v {
			builder.WriteString("true")
		} else {
			builder.WriteString("false")
		}
	default:
		return fmt.Errorf("unsupported value type: %T,value: %v", value, value)
	}
	return nil
}

func writeEscapeStringValue(builder *strings.Builder, s string) {
	for _, v := range s {
		switch v {
		case 0:
			continue
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		case '\'':
			builder.WriteString(`''`)
		case '"':
			builder.WriteString(`\"`)
		case '\\':
			builder.WriteString(`\\`)
		default:
			builder.WriteRune(v)
		}
	}
}

// escapeIdentifier escapes backticks in identifiers
// e.g. table`name -> table“name
func escapeIdentifier(identifier string) string {
	return strings.ReplaceAll(identifier, "`", "``")
}

const InsertSqlStatement = "insert into "

// sortRecords sorts the records by stb, subTable, keys, ts
func sortRecords(records []*record) {
	slices.SortFunc(records, func(a, b *record) int {
		// sorting by stb, subTable, keys, ts
		if a.stb != b.stb {
			return strings.Compare(a.stb, b.stb)
		}
		if a.subTable != b.subTable {
			return strings.Compare(a.subTable, b.subTable)
		}
		if a.keys != b.keys {
			return strings.Compare(a.keys, b.keys)
		}
		return a.ts.Compare(b.ts)
	})
}

// generateSql generates the insert SQL statements from the records
// it will split the SQL statements if the length exceeds maxSqlLength
func generateSql(records []*record, maxSqlLength int) ([]string, error) {
	sortRecords(records)
	sqlBuilder := &strings.Builder{}
	tmpBuilder := &bytes.Buffer{}
	var sqlArray []string
	lastStb := ""
	lastKey := ""
	containsStb := false
	sqlBuilder.WriteString(InsertSqlStatement)
	for i := 0; i < len(records); i++ {
		rec := records[i]
		// new supertable or different field keys
		// need to write supertable part
		if rec.stb != lastStb || rec.keys != lastKey {
			writeSuperTableStatement(tmpBuilder, rec.stb, rec.keys)
			lastStb = rec.stb
			lastKey = rec.keys
			containsStb = true
		} else {
			containsStb = false
		}
		// write values part
		tmpBuilder.WriteByte('(')
		tmpBuilder.WriteString(rec.values)
		tmpBuilder.WriteByte(')')
		if tmpBuilder.Len()+len(InsertSqlStatement) >= maxSqlLength {
			// value too large to fit in one sql
			return nil, fmt.Errorf("single record sql length exceeds max sql length: %d, sql value: %s", maxSqlLength, log.GetLogSql(tmpBuilder.String()))
		}
		if sqlBuilder.Len()+tmpBuilder.Len() >= maxSqlLength {
			// flush current sql
			sqlArray = append(sqlArray, sqlBuilder.String())
			sqlBuilder.Reset()
			sqlBuilder.WriteString(InsertSqlStatement)
			if !containsStb {
				// need to write supertable part
				writeSuperTableStatement(sqlBuilder, rec.stb, rec.keys)
			}
		}
		sqlBuilder.Write(tmpBuilder.Bytes())
		tmpBuilder.Reset()
	}
	if sqlBuilder.Len() > len(InsertSqlStatement) {
		sqlArray = append(sqlArray, sqlBuilder.String())
	}
	return sqlArray, nil
}

// writeSuperTableStatement writes the insert into supertable part of the insert statement
// e.g. `db`.`stb` (`tbname`,`ts`,`field1`,`field2`) values
func writeSuperTableStatement(builder io.StringWriter, superTable string, keys string) {
	// strings.Builder and bytes.Buffer will not return error on WriteString
	_, _ = builder.WriteString(superTable)
	_, _ = builder.WriteString("(`tbname`,")
	_, _ = builder.WriteString(keys)
	_, _ = builder.WriteString(")values")
}

// generateFieldKeyStatement generates the field key part of the insert statement
// e.g. `ts`,`field1`,`field2`
func generateFieldKeyStatement(builder *strings.Builder, rule *ParsedRule, count int) {
	builder.WriteByte('`')
	builder.WriteString(rule.TimeFieldName)
	builder.WriteByte('`')
	for i := 0; i < count; i++ {
		writeFieldName(builder, rule.FieldKeys[i])
	}
}

// writeFieldName writes a field name to the builder with proper formatting
// e.g. ,`fieldName`
func writeFieldName(builder *strings.Builder, fieldKey string) {
	builder.WriteString(",`")
	builder.WriteString(fieldKey)
	builder.WriteByte('`')
}

// execute executes the given SQL statements and returns the total affected rows
func execute(conn unsafe.Pointer, reqID int64, sqlArray []string, logger *logrus.Entry, isDebug bool) (int, error) {
	totalAffectedRows := 0
	for i := 0; i < len(sqlArray); i++ {
		affectedRows, err := async.GlobalAsync.TaosExecWithAffectedRows(conn, logger, isDebug, sqlArray[i], reqID)
		if err != nil {
			logger.Errorf("error executing sql: %v, sql: %s", err, sqlArray[i])
			return 0, err
		}
		totalAffectedRows += affectedRows
	}
	return totalAffectedRows, nil
}

func (p *Plugin) Start() error {
	return nil
}

func (p *Plugin) Stop() error {
	return nil
}

func (p *Plugin) String() string {
	return "input_json"
}

func (p *Plugin) Version() string {
	return "v1"
}

func init() {
	plugin.Register(&Plugin{})
}
