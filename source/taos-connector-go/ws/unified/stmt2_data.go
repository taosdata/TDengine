package unified

import (
	"database/sql/driver"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/types"
)

// normalizeStmt2Value converts one compatibility-layer value into stmt2 bind value.
// queryMode controls timestamp encoding:
//   - query mode: RFC3339Nano string
//   - insert mode: integer timestamp by precision
func normalizeStmt2Value(v driver.Value, queryMode bool) (driver.Value, error) {
	switch typed := v.(type) {
	case nil:
		return nil, nil
	case bool, int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, string, []byte, time.Time:
		return typed, nil
	case types.TaosBool:
		return bool(typed), nil
	case types.TaosTinyint:
		return int8(typed), nil
	case types.TaosSmallint:
		return int16(typed), nil
	case types.TaosInt:
		return int32(typed), nil
	case types.TaosBigint:
		return int64(typed), nil
	case types.TaosUTinyint:
		return uint8(typed), nil
	case types.TaosUSmallint:
		return uint16(typed), nil
	case types.TaosUInt:
		return uint32(typed), nil
	case types.TaosUBigint:
		return uint64(typed), nil
	case types.TaosFloat:
		return float32(typed), nil
	case types.TaosDouble:
		return float64(typed), nil
	case types.TaosBinary:
		return []byte(typed), nil
	case types.TaosVarBinary:
		return []byte(typed), nil
	case types.TaosNchar:
		return string(typed), nil
	case types.TaosJson:
		return []byte(typed), nil
	case types.TaosGeometry:
		return []byte(typed), nil
	case types.TaosBlob:
		return []byte(typed), nil
	case types.TaosDecimal:
		return string(typed), nil
	case types.TaosTimestamp:
		if queryMode {
			return typed.T.Format(time.RFC3339Nano), nil
		}
		return common.TimeToTimestamp(typed.T, typed.Precision), nil
	default:
		return nil, newInvalidStateErrorf("unsupported stmt2 value type %T", v)
	}
}

func isDecimalBindType(columnType *types.ColumnType) bool {
	return columnType != nil && columnType.Type == types.TaosDecimalType
}

func normalizeStmt2DecimalValue(v driver.Value) (driver.Value, error) {
	switch typed := v.(type) {
	case nil:
		return nil, nil
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	case types.TaosDecimal:
		return string(typed), nil
	default:
		return nil, newInvalidStateErrorf("unsupported decimal value type %T", v)
	}
}

func validateDecimalFieldType(field *commonstmt.Stmt2AllField) error {
	if field == nil || field.FieldType == common.TSDB_DATA_TYPE_NULL {
		return nil
	}
	if field.FieldType == common.TSDB_DATA_TYPE_DECIMAL || field.FieldType == common.TSDB_DATA_TYPE_DECIMAL64 {
		return nil
	}
	return newInvalidStateErrorf("decimal bind type requires decimal column, got %s", common.GetTypeName(int(field.FieldType)))
}

func getBindColumnTypes(bindType *param.ColumnType, expected int, bindName string) ([]*types.ColumnType, error) {
	if bindType == nil {
		return nil, nil
	}
	columnTypes, err := bindType.GetValue()
	if err != nil {
		return nil, newInvalidStateErrorf("invalid %s bind types: %v", bindName, err)
	}
	if len(columnTypes) != expected {
		return nil, newInvalidStateErrorf("expected %d %s bind types, got %d", expected, bindName, len(columnTypes))
	}
	return columnTypes, nil
}

// normalizeStmt2Column converts one Param column into stmt2 bind column data.
func normalizeStmt2Column(paramColumn *param.Param, bindType *types.ColumnType, field *commonstmt.Stmt2AllField, queryMode bool) ([]driver.Value, error) {
	values := paramColumn.GetValues()
	normalized := make([]driver.Value, len(values))
	if isDecimalBindType(bindType) {
		if err := validateDecimalFieldType(field); err != nil {
			return nil, err
		}
		for i := 0; i < len(values); i++ {
			v, err := normalizeStmt2DecimalValue(values[i])
			if err != nil {
				return nil, err
			}
			normalized[i] = v
		}
		return normalized, nil
	}
	for i := 0; i < len(values); i++ {
		v, err := normalizeStmt2Value(values[i], queryMode)
		if err != nil {
			return nil, err
		}
		normalized[i] = v
	}
	return normalized, nil
}

// normalizeStmt2Columns converts all Param columns into stmt2 bind columns.
func normalizeStmt2Columns(columns []*param.Param, bindType *param.ColumnType, fields []*commonstmt.Stmt2AllField, queryMode bool) ([][]driver.Value, error) {
	columnTypes, err := getBindColumnTypes(bindType, len(columns), "column")
	if err != nil {
		return nil, err
	}
	if len(fields) > 0 && len(fields) != len(columns) {
		return nil, newInvalidStateErrorf("expected %d columns by prepared fields, got %d", len(fields), len(columns))
	}
	normalized := make([][]driver.Value, len(columns))
	for i := 0; i < len(columns); i++ {
		var columnType *types.ColumnType
		if columnTypes != nil {
			columnType = columnTypes[i]
		}
		var field *commonstmt.Stmt2AllField
		if len(fields) > i {
			field = fields[i]
		}
		col, err := normalizeStmt2Column(columns[i], columnType, field, queryMode)
		if err != nil {
			return nil, err
		}
		normalized[i] = col
	}
	return normalized, nil
}

func normalizeStmt2TagValues(tags *param.Param, bindType *param.ColumnType, fields []*commonstmt.Stmt2AllField) ([]driver.Value, error) {
	values := tags.GetValues()
	columnTypes, err := getBindColumnTypes(bindType, len(values), "tag")
	if err != nil {
		return nil, err
	}
	if len(fields) > 0 && len(fields) != len(values) {
		return nil, newInvalidStateErrorf("expected %d tags by prepared fields, got %d", len(fields), len(values))
	}

	normalized := make([]driver.Value, len(values))
	for i := 0; i < len(values); i++ {
		var columnType *types.ColumnType
		if columnTypes != nil {
			columnType = columnTypes[i]
		}
		var field *commonstmt.Stmt2AllField
		if len(fields) > i {
			field = fields[i]
		}
		if isDecimalBindType(columnType) {
			if err := validateDecimalFieldType(field); err != nil {
				return nil, err
			}
			v, err := normalizeStmt2DecimalValue(values[i])
			if err != nil {
				return nil, err
			}
			normalized[i] = v
			continue
		}
		v, err := normalizeStmt2Value(values[i], false)
		if err != nil {
			return nil, err
		}
		normalized[i] = v
	}
	return normalized, nil
}

func splitStmt2InsertFields(fields []*commonstmt.Stmt2AllField) (tagFields []*commonstmt.Stmt2AllField, colFields []*commonstmt.Stmt2AllField) {
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		if field == nil {
			continue
		}
		switch field.BindType {
		case commonstmt.TAOS_FIELD_TAG:
			tagFields = append(tagFields, field)
		case commonstmt.TAOS_FIELD_COL:
			colFields = append(colFields, field)
		}
	}
	return tagFields, colFields
}

func queryStmtFields(fields []*commonstmt.Stmt2AllField) []*commonstmt.Stmt2AllField {
	if len(fields) == 0 {
		return nil
	}
	out := make([]*commonstmt.Stmt2AllField, 0, len(fields))
	for i := 0; i < len(fields); i++ {
		field := fields[i]
		if field == nil {
			continue
		}
		if field.BindType == commonstmt.TAOS_FIELD_QUERY || field.BindType == commonstmt.TAOS_FIELD_COL {
			out = append(out, field)
		}
	}
	return out
}

// buildStmt2InsertBindData builds one stmt2 bind block for insert path.
func buildStmt2InsertBindData(tableName string, tags *param.Param, tagBindType *param.ColumnType, params []*param.Param, paramBindType *param.ColumnType, fields []*commonstmt.Stmt2AllField) (*commonstmt.TaosStmt2BindData, error) {
	tagFields, colFields := splitStmt2InsertFields(fields)
	item := &commonstmt.TaosStmt2BindData{
		TableName: tableName,
	}
	if tags != nil {
		normalizedTags, err := normalizeStmt2TagValues(tags, tagBindType, tagFields)
		if err != nil {
			return nil, err
		}
		item.Tags = normalizedTags
	}
	if len(params) > 0 {
		normalizedCols, err := normalizeStmt2Columns(params, paramBindType, colFields, false)
		if err != nil {
			return nil, err
		}
		item.Cols = normalizedCols
	}
	return item, nil
}

// buildStmt2QueryBindData builds stmt2 bind data for query path.
// Query supports exactly one bind block.
func buildStmt2QueryBindData(params []*param.Param, bindType *param.ColumnType, fields []*commonstmt.Stmt2AllField) ([]*commonstmt.TaosStmt2BindData, error) {
	if len(params) == 0 {
		return nil, newInvalidStateErrorf("no query params")
	}
	cols, err := normalizeStmt2Columns(params, bindType, queryStmtFields(fields), true)
	if err != nil {
		return nil, err
	}
	return []*commonstmt.TaosStmt2BindData{
		{
			Cols: cols,
		},
	}, nil
}
