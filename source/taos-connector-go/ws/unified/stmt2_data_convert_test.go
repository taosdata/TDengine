package unified

import (
	"database/sql/driver"
	"reflect"
	"testing"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/types"
)

func assertEqual(t *testing.T, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected value:\nwant: %#v\ngot:  %#v", want, got)
	}
}

func TestNormalizeStmt2DecimalValue_AllPaths(t *testing.T) {
	tests := []struct {
		name    string
		value   driver.Value
		expect  driver.Value
		wantErr bool
	}{
		{name: "nil", value: nil, expect: nil},
		{name: "string", value: "1.2300", expect: "1.2300"},
		{name: "bytes", value: []byte("2.3400"), expect: "2.3400"},
		{name: "taos_decimal", value: types.TaosDecimal("3.4500"), expect: "3.4500"},
		{name: "unsupported", value: int32(1), wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeStmt2DecimalValue(tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertEqual(t, got, tt.expect)
		})
	}
}

func TestValidateDecimalFieldType_AllPaths(t *testing.T) {
	if err := validateDecimalFieldType(nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := validateDecimalFieldType(&commonstmt.Stmt2AllField{FieldType: common.TSDB_DATA_TYPE_NULL}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := validateDecimalFieldType(&commonstmt.Stmt2AllField{FieldType: common.TSDB_DATA_TYPE_DECIMAL}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := validateDecimalFieldType(&commonstmt.Stmt2AllField{FieldType: common.TSDB_DATA_TYPE_DECIMAL64}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := validateDecimalFieldType(&commonstmt.Stmt2AllField{FieldType: common.TSDB_DATA_TYPE_INT}); err == nil {
		t.Fatalf("expected non-decimal field type error")
	}
}

func TestGetBindColumnTypes_AllPaths(t *testing.T) {
	columnTypes, err := getBindColumnTypes(nil, 1, "column")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if columnTypes != nil {
		t.Fatalf("expected nil column types when bind type is nil")
	}

	_, err = getBindColumnTypes(param.NewColumnType(2).AddInt(), 1, "column")
	if err == nil {
		t.Fatalf("expected invalid bind type error")
	}

	_, err = getBindColumnTypes(param.NewColumnType(1).AddInt(), 2, "column")
	if err == nil {
		t.Fatalf("expected bind type length mismatch error")
	}

	columnTypes, err = getBindColumnTypes(param.NewColumnType(1).AddInt(), 1, "column")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(columnTypes) != 1 {
		t.Fatalf("unexpected column type length: %d", len(columnTypes))
	}
}

func TestNormalizeStmt2ColumnAndColumns_ErrorPaths(t *testing.T) {
	_, err := normalizeStmt2Column(
		param.NewParam(1).AddDecimal("1.2300"),
		&types.ColumnType{Type: types.TaosDecimalType},
		&commonstmt.Stmt2AllField{FieldType: common.TSDB_DATA_TYPE_INT},
		false,
	)
	if err == nil {
		t.Fatalf("expected decimal field type mismatch error")
	}

	_, err = normalizeStmt2Column(
		param.NewParam(1).AddValue(int32(1)),
		&types.ColumnType{Type: types.TaosDecimalType},
		&commonstmt.Stmt2AllField{FieldType: common.TSDB_DATA_TYPE_DECIMAL},
		false,
	)
	if err == nil {
		t.Fatalf("expected decimal value conversion error")
	}

	_, err = normalizeStmt2Column(
		param.NewParam(1).AddValue(struct{}{}),
		nil,
		nil,
		false,
	)
	if err == nil {
		t.Fatalf("expected general value conversion error")
	}

	_, err = normalizeStmt2Columns(
		[]*param.Param{param.NewParam(1).AddInt(1), param.NewParam(1).AddInt(2)},
		nil,
		[]*commonstmt.Stmt2AllField{
			{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
		},
		false,
	)
	if err == nil {
		t.Fatalf("expected field length mismatch error")
	}

	_, err = normalizeStmt2Columns(
		[]*param.Param{param.NewParam(1).AddInt(1), param.NewParam(1).AddInt(2)},
		param.NewColumnType(1).AddInt(),
		nil,
		false,
	)
	if err == nil {
		t.Fatalf("expected bind type length mismatch error")
	}
}

func TestNormalizeStmt2TagValues_AllPaths(t *testing.T) {
	_, err := normalizeStmt2TagValues(
		param.NewParam(2).AddNchar("a").AddNchar("b"),
		nil,
		[]*commonstmt.Stmt2AllField{
			{FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_TAG},
		},
	)
	if err == nil {
		t.Fatalf("expected tag field length mismatch error")
	}

	_, err = normalizeStmt2TagValues(
		param.NewParam(2).AddNchar("a").AddNchar("b"),
		param.NewColumnType(1).AddNchar(8),
		nil,
	)
	if err == nil {
		t.Fatalf("expected tag bind type length mismatch error")
	}

	_, err = normalizeStmt2TagValues(
		param.NewParam(1).AddDecimal("1.2300"),
		param.NewColumnType(1).AddDecimal(),
		[]*commonstmt.Stmt2AllField{
			{FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_TAG},
		},
	)
	if err == nil {
		t.Fatalf("expected decimal tag field mismatch error")
	}

	_, err = normalizeStmt2TagValues(
		param.NewParam(1).AddValue(int32(1)),
		param.NewColumnType(1).AddDecimal(),
		[]*commonstmt.Stmt2AllField{
			{FieldType: common.TSDB_DATA_TYPE_DECIMAL, BindType: commonstmt.TAOS_FIELD_TAG},
		},
	)
	if err == nil {
		t.Fatalf("expected decimal tag value conversion error")
	}

	_, err = normalizeStmt2TagValues(
		param.NewParam(1).AddValue(struct{}{}),
		nil,
		nil,
	)
	if err == nil {
		t.Fatalf("expected unsupported tag value error")
	}

	got, err := normalizeStmt2TagValues(
		param.NewParam(1).AddDecimal("9.9900"),
		param.NewColumnType(1).AddDecimal(),
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, got, []driver.Value{"9.9900"})

	got, err = normalizeStmt2TagValues(
		param.NewParam(1).AddNchar("tag"),
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertEqual(t, got, []driver.Value{"tag"})
}

func TestSplitStmt2InsertFieldsAndQueryStmtFields_AllPaths(t *testing.T) {
	fields := []*commonstmt.Stmt2AllField{
		nil,
		{Name: "tb", FieldType: common.TSDB_DATA_TYPE_BINARY, BindType: commonstmt.TAOS_FIELD_TBNAME},
		{Name: "tag", FieldType: common.TSDB_DATA_TYPE_NCHAR, BindType: commonstmt.TAOS_FIELD_TAG},
		{Name: "col", FieldType: common.TSDB_DATA_TYPE_INT, BindType: commonstmt.TAOS_FIELD_COL},
		{Name: "unknown", FieldType: common.TSDB_DATA_TYPE_INT, BindType: int8(99)},
	}
	tagFields, colFields := splitStmt2InsertFields(fields)
	if len(tagFields) != 1 || tagFields[0].Name != "tag" {
		t.Fatalf("unexpected tag fields: %+v", tagFields)
	}
	if len(colFields) != 1 || colFields[0].Name != "col" {
		t.Fatalf("unexpected col fields: %+v", colFields)
	}

	if queryStmtFields(nil) != nil {
		t.Fatalf("expected nil query fields for nil input")
	}

	queryFields := queryStmtFields(fields)
	if len(queryFields) != 1 || queryFields[0].Name != "col" {
		t.Fatalf("unexpected query fields: %+v", queryFields)
	}
}

func TestBuildStmt2BindData_AdditionalPaths(t *testing.T) {
	data, err := buildStmt2InsertBindData("tb_only", nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data.TableName != "tb_only" || len(data.Tags) != 0 || len(data.Cols) != 0 {
		t.Fatalf("unexpected insert bind data: %+v", data)
	}

	_, err = buildStmt2QueryBindData(
		[]*param.Param{param.NewParam(1).AddValue(struct{}{})},
		nil,
		nil,
	)
	if err == nil {
		t.Fatalf("expected query conversion error")
	}

	_, err = buildStmt2InsertBindData(
		"tb_bad_tag",
		param.NewParam(1).AddValue(struct{}{}),
		nil,
		nil,
		nil,
		nil,
	)
	if err == nil {
		t.Fatalf("expected insert tag conversion error")
	}

	_, err = buildStmt2InsertBindData(
		"tb_bad_col",
		nil,
		nil,
		[]*param.Param{param.NewParam(1).AddValue(struct{}{})},
		nil,
		nil,
	)
	if err == nil {
		t.Fatalf("expected insert col conversion error")
	}
}
