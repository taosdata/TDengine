package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/araddon/dateparse"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/httpclient"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/grafana/grafana-plugin-sdk-go/data"
)

// Make sure Datasource implements required interfaces. This is important to do
// since otherwise we will only get a not implemented error response from plugin in
// runtime. In this example datasource instance implements backend.QueryDataHandler,
// backend.CheckHealthHandler interfaces. Plugin should not implement all these
// interfaces-only those which are required for a particular task.
var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// NewDatasource creates a new datasource instance.
func NewDatasource(ctx context.Context, settings backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	ops, err := settings.HTTPClientOptions(ctx)

	if err != nil {
		return nil, fmt.Errorf("http client options: %w", err)
	}
	client, err := httpclient.New(ops)
	if err != nil {
		return nil, fmt.Errorf("new httpclient error: %w", err)
	}

	ds := Datasource{
		settings: settings,
		client:   client,
		token:    settings.DecryptedSecureJSONData["token"],
	}
	endpoint, err := ds.detectEndpoint()
	if err != nil {
		return nil, fmt.Errorf("detect datasource endpoint error: %w", err)
	}
	ds.endpoint = endpoint
	return &ds, nil
}

// Datasource is an example datasource which can respond to data queries, reports
// its health and has streaming skills.
type Datasource struct {
	settings backend.DataSourceInstanceSettings
	client   *http.Client
	token    string
	endpoint string
}

// Dispose here tells plugin SDK that plugin wants to clean up resources when a new instance
// created. As soon as datasource settings change detected by SDK old datasource instance will
// be disposed and a new one will be created using NewSampleDatasource factory function.
func (d *Datasource) Dispose() {
	// Clean up datasource instance resources.
	d.client.CloseIdleConnections()
}

type dataResponse struct {
	refId string
	data  backend.DataResponse
}

// QueryData handles multiple queries and returns multiple responses.
// req contains the queries []DataQuery (where each query contains RefID as a unique identifier).
// The QueryDataResponse contains a map of RefID to the response for each query, and each response
// contains Frames ([]*Frame).
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	// when logging at a non-Debug level, make sure you don't include sensitive information in the message
	// (like the *backend.QueryDataRequest)
	log.DefaultLogger.Debug("## QueryData called", "queries", req.Queries)

	// create response struct
	response := backend.NewQueryDataResponse()

	var wait sync.WaitGroup
	ch := make(chan dataResponse, len(req.Queries))
	// loop over queries and execute them individually.
	for _, q := range req.Queries {
		wait.Add(1)
		go func(query backend.DataQuery, w *sync.WaitGroup) {
			defer w.Done()
			res := d.query(ctx, req.PluginContext, query)
			ch <- dataResponse{refId: query.RefID, data: res}
		}(q, &wait)
	}

	go func(w *sync.WaitGroup) {
		defer close(ch)
		w.Wait()
	}(&wait)

	// save the response in a hashmap
	// based on with RefID as identifier
	for res := range ch {
		response.Responses[res.refId] = res.data
	}

	return response, nil
}

func (d *Datasource) query(ctx context.Context, _ backend.PluginContext, query backend.DataQuery) (response backend.DataResponse) {
	// Unmarshal the JSON into our queryModel.
	qm, ok, msg := getQueryModel(query)
	if !ok {
		return backend.ErrDataResponse(http.StatusBadRequest, msg)
	}

	// query data from datasource
	result, err := d.queryDataFromDatasource(ctx, qm)
	if err != nil {
		log.DefaultLogger.Error("## query data error ", "error", err)
		return backend.ErrDataResponse(http.StatusBadRequest, err.Error())
	}
	if len(result.Data) == 0 || len(result.Data[0]) == 0 {
		return
	}
	forceTableFormat := isTableFormat(qm.FormatType)
	frame, err := buildFrame(result, qm, forceTableFormat)
	if err != nil {
		log.DefaultLogger.Error("build frame error", "error", err)
		return backend.ErrDataResponse(http.StatusBadRequest, err.Error())
	}
	// Convert Long format to Wide format using Grafana SDK.
	tsSchema := frame.TimeSeriesSchema()
	if !forceTableFormat && tsSchema.Type == data.TimeSeriesTypeLong {
		sortFrameRowsByTime(frame)
		frame, err = data.LongToWide(frame, nil)
		if err != nil {
			return backend.ErrDataResponse(http.StatusBadRequest, fmt.Sprintf("LongToWide conversion error: %s", err.Error()))
		}
	} else {
		// Not a time series or already Wide format, set Meta
		if frame.Meta == nil {
			frame.Meta = &data.FrameMeta{}
		}
		if forceTableFormat {
			frame.Meta.Type = data.FrameTypeTable
		} else {
			frame.Meta.Type = determineFrameType(frame.Fields)
		}
	}

	response.Frames = append(response.Frames, frame)
	return response
}

func buildFrame(result *dataResult, qm *queryModel, forceTableFormat bool) (*data.Frame, error) {
	timeColIdx := findTimeColumnIndex(result.ColumnMeta)
	columnOrder := buildColumnOrder(len(result.ColumnMeta), timeColIdx)
	if forceTableFormat {
		columnOrder = buildNaturalColumnOrder(len(result.ColumnMeta))
	}
	frame := data.NewFrame("")
	aliasList := splitAliasList(qm.Alias)
	valueAliasIdx := 0
	keepNilPrimaryTime := forceTableFormat

	for _, srcIdx := range columnOrder {
		colMeta := result.ColumnMeta[srcIdx]
		name := toString(colMeta[0])
		if srcIdx != timeColIdx && valueAliasIdx < len(aliasList) && aliasList[valueAliasIdx] != "" {
			name = strings.ReplaceAll(aliasList[valueAliasIdx], "[[col]]", name)
		}
		if srcIdx != timeColIdx {
			valueAliasIdx++
		}
		frame.Fields = append(frame.Fields, data.NewField(name, nil, getTypeArrayForColumn(colMeta[1], srcIdx == timeColIdx, keepNilPrimaryTime)))
	}

	timeLayout, err := detectTimeLayout(result.Data, timeColIdx)
	if err != nil && !keepNilPrimaryTime {
		return nil, fmt.Errorf("ts parse layout error: %w", err)
	}

	for _, srcRow := range result.Data {
		row, skip, err := convertRow(srcRow, result.ColumnMeta, columnOrder, timeColIdx, timeLayout, keepNilPrimaryTime)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		frame.AppendRow(row...)
	}

	return frame, nil
}

func splitAliasList(alias string) []string {
	if strings.TrimSpace(alias) == "" {
		return nil
	}
	parts := strings.Split(alias, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func isTableFormat(formatType string) bool {
	return strings.EqualFold(strings.TrimSpace(formatType), "table")
}

func getTypeArrayForColumn(tp interface{}, primaryTimeCol, keepNilPrimaryTime bool) interface{} {
	if primaryTimeCol && keepNilPrimaryTime && isTimestampTypeForInterface(tp) {
		return []*time.Time{}
	}
	if !primaryTimeCol && isTimestampTypeForInterface(tp) {
		return []*time.Time{}
	}
	return getTypeArray(tp)
}

func buildNaturalColumnOrder(columnCount int) []int {
	order := make([]int, 0, columnCount)
	for idx := 0; idx < columnCount; idx++ {
		order = append(order, idx)
	}
	return order
}

func findTimeColumnIndex(columnMeta [][]interface{}) int {
	for idx, colMeta := range columnMeta {
		if len(colMeta) > 1 && trans2CTypeStr(colMeta[1]) == CTypeTimestampStr {
			return idx
		}
	}
	return -1
}

func buildColumnOrder(columnCount, timeColIdx int) []int {
	order := make([]int, 0, columnCount)
	if timeColIdx >= 0 {
		order = append(order, timeColIdx)
	}
	for idx := 0; idx < columnCount; idx++ {
		if idx == timeColIdx {
			continue
		}
		order = append(order, idx)
	}
	return order
}

func detectTimeLayout(rows [][]interface{}, timeColIdx int) (string, error) {
	if timeColIdx < 0 {
		return "", nil
	}
	for _, row := range rows {
		if timeColIdx >= len(row) || row[timeColIdx] == nil {
			continue
		}
		return dateparse.ParseFormat(toString(row[timeColIdx]))
	}
	return "", fmt.Errorf("timestamp column is empty")
}

func convertRow(srcRow []interface{}, columnMeta [][]interface{}, columnOrder []int, timeColIdx int, timeLayout string, keepNilPrimaryTime bool) ([]interface{}, bool, error) {
	row := make([]interface{}, 0, len(columnOrder))
	for _, srcIdx := range columnOrder {
		if srcIdx >= len(srcRow) {
			return nil, false, fmt.Errorf("row column index %d out of range", srcIdx)
		}
		value := srcRow[srcIdx]
		if srcIdx == timeColIdx {
			if value == nil {
				if keepNilPrimaryTime {
					row = append(row, nil)
					continue
				}
				return nil, true, nil
			}
			parsed, err := time.Parse(timeLayout, toString(value))
			if err != nil {
				return nil, false, fmt.Errorf("ts parse error: %w", err)
			}
			if keepNilPrimaryTime {
				parsedCopy := parsed
				row = append(row, &parsedCopy)
			} else {
				row = append(row, parsed)
			}
			continue
		}
		converted, err := convertNonTimeValue(value, columnMeta[srcIdx][1], timeLayout)
		if err != nil {
			return nil, false, err
		}
		row = append(row, converted)
	}
	return row, false, nil
}

func convertNonTimeValue(value interface{}, columnType interface{}, timeLayout string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	if isIntegerTypesForInterface(columnType) {
		converted, err := toFloat(value)
		if err != nil {
			return nil, fmt.Errorf("parse numeric data error: %w", err)
		}
		return &converted, nil
	}

	switch trans2CTypeStr(columnType) {
	case CTypeFloatStr, CTypeDoubleStr:
		converted, err := toFloat(value)
		if err != nil {
			return nil, fmt.Errorf("parse numeric data error: %w", err)
		}
		return &converted, nil
	case CTypeBoolStr:
		converted, err := toBool(value)
		if err != nil {
			return nil, fmt.Errorf("parse bool data error: %w", err)
		}
		return &converted, nil
	case CTypeTimestampStr:
		parsed, err := parseTimestampValue(value, timeLayout)
		if err != nil {
			return nil, fmt.Errorf("parse timestamp data error: %w", err)
		}
		return &parsed, nil
	default:
		converted := toString(value)
		return &converted, nil
	}
}

func parseTimestampValue(value interface{}, timeLayout string) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case *time.Time:
		if v == nil {
			return time.Time{}, fmt.Errorf("timestamp value is nil")
		}
		return *v, nil
	}

	strValue := toString(value)
	if len(timeLayout) > 0 {
		parsed, err := time.Parse(timeLayout, strValue)
		if err == nil {
			return parsed, nil
		}
	}
	return dateparse.ParseAny(strValue)
}

func (d *Datasource) queryDataFromDatasource(ctx context.Context, query *queryModel) (*dataResult, error) {
	body, err := d.doHttpPost(ctx, d.endpoint, query.Sql)
	if err != nil {
		log.DefaultLogger.Error("## query data error ", "error", err)
		return nil, err
	}

	var result dataResult
	if err = json.Unmarshal(body, &result); err != nil {
		log.DefaultLogger.Error("## unmarshal result data error ", "error", err)
		return nil, err
	}

	if result.Code != 0 {
		err = fmt.Errorf("query data by sql %s error, response code is %d ", query.Sql, result.Code)
		log.DefaultLogger.Error("query data error", "error", err)
		return nil, err
	}

	return &result, nil
}

// CheckHealth handles health checks sent from Grafana to the plugin.
// The main use case for these health checks is the test button on the
// datasource configuration page which allows users to verify that
// a datasource is working as expected.
func (d *Datasource) CheckHealth(ctx context.Context, req *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	// when logging at a non-Debug level, make sure you don't include sensitive information in the message
	// (like the *backend.QueryDataRequest)
	log.DefaultLogger.Debug("CheckHealth called")

	var status = backend.HealthStatusOk
	var message = "Data source is working"

	if _, err := d.queryDataFromDatasource(ctx, &queryModel{Sql: "show databases"}); err != nil {
		status = backend.HealthStatusError
		message = fmt.Sprintf("failed get connect to tdengine. %s", err.Error())
	}

	return &backend.CheckHealthResult{
		Status:  status,
		Message: message,
	}, nil
}

const sqlEndPoint = "/rest/sql"
const utcSqlEndPoint = "/rest/sqlutc"

var (
	timeFromMacroPattern   = regexp.MustCompile(`\$__timeFrom(?:\s*\(\s*\))?`)
	timeToMacroPattern     = regexp.MustCompile(`\$__timeTo(?:\s*\(\s*\))?`)
	timeFilterMacroPattern = regexp.MustCompile(`\$__timeFilter\s*\(\s*([^)]+?)\s*\)`)
)

func (d *Datasource) detectEndpoint() (string, error) {
	respData, err := d.doHttpPost(context.Background(), d.settings.URL+sqlEndPoint, "select server_version()")
	if err != nil {
		log.DefaultLogger.Error("query server version error ", "error", err)
		return "", err
	}

	var ver serverVer
	if err = json.Unmarshal(respData, &ver); err != nil {
		log.DefaultLogger.Error("unmarshall server version data ", "error", err)
		return "", err
	}
	if len(ver.Data) != 1 || len(ver.Data[0]) != 1 {
		log.DefaultLogger.Error("get server version data error, resp data is ", "resp data", string(respData))
		return "", err
	}

	if is30(ver.Data[0][0]) {
		return d.settings.URL + sqlEndPoint, nil
	}
	return d.settings.URL + utcSqlEndPoint, nil
}

func (d *Datasource) doHttpPost(ctx context.Context, url, data string) (respData []byte, err error) {
	if len(d.token) > 0 {
		url += "?token=" + d.token
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(data))
	if err != nil {
		log.DefaultLogger.Error("query error", "data", data, "error", err)
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		log.DefaultLogger.Error("query error", "error", err)
		return nil, err
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http request for [%s] received code: %d, status %s ", data,
			resp.StatusCode, resp.Status)
	}

	return io.ReadAll(resp.Body)
}

func getQueryModel(query backend.DataQuery) (model *queryModel, success bool, errMsg string) {
	err := json.Unmarshal(query.JSON, &model)
	if err != nil {
		log.DefaultLogger.Error("## unmarshal query to query model error ", "query", query.JSON, "error", err)
		return nil, false, fmt.Sprintf("json unmarshal: %v", err.Error())
	}

	if len(model.QueryType) == 0 {
		model.QueryType = "SQL"
	}

	if model.QueryType != "SQL" {
		log.DefaultLogger.Error("## queryType error, only support SQL queryType")
		return nil, false, "queryType error, only support SQL queryType"
	}
	if hasDeprecatedTimeShift(model.TimeShiftPeriod, model.TimeShiftUnit) {
		log.DefaultLogger.Error("## timeShift is deprecated, use panel time shift instead")
		return nil, false, "timeShift is deprecated, use panel Query options -> Time shift instead"
	}

	sql := model.Sql
	if len(sql) == 0 {
		log.DefaultLogger.Error("## generateSql can not get SQL")
		return nil, false, "generateSql can not get SQL"
	}
	timeZone, err := time.LoadLocation("")
	if err != nil {
		log.DefaultLogger.Error("## get time location zone: %w", "error", err)
		return nil, false, fmt.Sprintf("get time location zone: %v", err)
	}
	if query.Interval > 0 {
		sql = strings.ReplaceAll(sql, "$interval", fmt.Sprint(query.Interval.Seconds())+"s")
	}
	fromValue := "'" + fmt.Sprint(query.TimeRange.From.In(timeZone).Format(time.RFC3339Nano)) + "'"
	toValue := "'" + fmt.Sprint(query.TimeRange.To.In(timeZone).Format(time.RFC3339Nano)) + "'"

	sql = strings.ReplaceAll(sql, "$from", fromValue)
	sql = strings.ReplaceAll(sql, "$begin", fromValue)
	sql = strings.ReplaceAll(sql, "$to", toValue)
	sql = strings.ReplaceAll(sql, "$end", toValue)

	sql, err = applyGrafanaTimeMacros(sql, fromValue, toValue)
	if err != nil {
		log.DefaultLogger.Error("## apply Grafana time macros error", "error", err)
		return nil, false, err.Error()
	}
	log.DefaultLogger.Debug("## query sql", "sql", sql)

	model.Sql = sql
	success = true
	return
}

func applyGrafanaTimeMacros(sql, fromValue, toValue string) (string, error) {
	sql = timeFromMacroPattern.ReplaceAllString(sql, fromValue)
	sql = timeToMacroPattern.ReplaceAllString(sql, toValue)
	sql = timeFilterMacroPattern.ReplaceAllString(sql, "($1 >= "+fromValue+" AND $1 <= "+toValue+")")

	if strings.Contains(sql, "$__timeFilter") {
		return "", fmt.Errorf("macro $__timeFilter requires a column argument, e.g. $__timeFilter(ts)")
	}
	return sql, nil
}

func hasDeprecatedTimeShift(period interface{}, unit string) bool {
	if len(strings.TrimSpace(unit)) > 0 {
		return true
	}

	if period == nil {
		return false
	}

	if v, ok := period.(string); ok {
		return len(strings.TrimSpace(v)) > 0
	}

	n, err := toFloat(period)
	if err != nil {
		return true
	}
	return n != 0
}

func sortFrameRowsByTime(frame *data.Frame) {
	if len(frame.Fields) == 0 {
		return
	}

	timeFieldIdx := -1
	for idx, field := range frame.Fields {
		if field.Type() == data.FieldTypeTime || field.Type() == data.FieldTypeNullableTime {
			timeFieldIdx = idx
			break
		}
	}
	if timeFieldIdx < 0 {
		return
	}

	timeField := frame.Fields[timeFieldIdx]
	if timeField.Len() < 2 {
		return
	}

	needsSort := false
	for i := 1; i < timeField.Len(); i++ {
		prev, prevOK := timeField.ConcreteAt(i - 1)
		curr, currOK := timeField.ConcreteAt(i)
		if !prevOK || !currOK {
			return
		}
		if prev.(time.Time).After(curr.(time.Time)) {
			needsSort = true
			break
		}
	}
	if !needsSort {
		return
	}

	indexes := make([]int, timeField.Len())
	for i := range indexes {
		indexes[i] = i
	}
	sort.SliceStable(indexes, func(i, j int) bool {
		left, _ := timeField.ConcreteAt(indexes[i])
		right, _ := timeField.ConcreteAt(indexes[j])
		return left.(time.Time).Before(right.(time.Time))
	})

	for _, field := range frame.Fields {
		values := make([]interface{}, field.Len())
		for idx := range indexes {
			values[idx] = field.At(indexes[idx])
		}
		for idx, value := range values {
			field.Set(idx, value)
		}
	}
}

// determineFrameType determines the Frame format based on field types.
// Reference Grafana SDK's logic:
// - Has string/bool fields = TimeSeriesLong (dimensions in string columns)
// - No string fields = TimeSeriesWide
// - No time field but has numeric fields = NumericWide
func determineFrameType(fields []*data.Field) data.FrameType {
	hasTime := false
	hasString := false
	hasNumeric := false

	for _, field := range fields {
		switch field.Type() {
		case data.FieldTypeTime, data.FieldTypeNullableTime:
			hasTime = true
		case data.FieldTypeString, data.FieldTypeNullableString,
			data.FieldTypeBool, data.FieldTypeNullableBool:
			hasString = true
		case data.FieldTypeFloat64, data.FieldTypeNullableFloat64,
			data.FieldTypeInt64, data.FieldTypeNullableInt64,
			data.FieldTypeFloat32, data.FieldTypeNullableFloat32,
			data.FieldTypeInt32, data.FieldTypeNullableInt32,
			data.FieldTypeInt8, data.FieldTypeNullableInt8,
			data.FieldTypeInt16, data.FieldTypeNullableInt16,
			data.FieldTypeUint8, data.FieldTypeNullableUint8,
			data.FieldTypeUint16, data.FieldTypeNullableUint16,
			data.FieldTypeUint32, data.FieldTypeNullableUint32,
			data.FieldTypeUint64, data.FieldTypeNullableUint64:
			hasNumeric = true
		}
	}

	// No time column
	if !hasTime {
		if hasNumeric {
			return data.FrameTypeNumericWide
		}
		return data.FrameTypeTable
	}

	// Has time column
	if !hasNumeric {
		return data.FrameTypeTable
	}
	if hasString {
		// Has string fields = Long format (dimensions in string columns)
		// Grafana alerting system will use string column values as labels
		return data.FrameTypeTimeSeriesLong
	}
	// No string fields = Wide format
	return data.FrameTypeTimeSeriesWide
}
