package sqlparser

import (
	"bytes"
	"testing"
)

func TestQueryCoverage_MarkerInterfaces(t *testing.T) {
	(&TableNameExpr{}).iTableExpr()
	(&SubqueryTableExpr{}).iTableExpr()
	(&JoinTableExpr{}).iTableExpr()
	(&RawExpr{}).iExpr()
}

func TestQueryCoverage_ExprToSQL(t *testing.T) {
	if got := exprToSQL(nil); got != "" {
		t.Fatalf("exprToSQL(nil) = %q, want empty", got)
	}
	got := exprToSQL(NewLiteralExpr(&Scanner{}, Token{Bytes: []byte("1")}, LiteralInt))
	if got != "1" {
		t.Fatalf("exprToSQL(literal) = %q, want 1", got)
	}
}

func TestQueryCoverage_SearchConditionListFormat(t *testing.T) {
	tb := &TrackedBuffer{Buffer: &bytes.Buffer{}}
	expr := &RawExpr{
		Kind: "search_condition_list",
		Args: []Expr{
			&RawExpr{Kind: "col", Name: "a"},
			&RawExpr{Kind: "col", Name: "b"},
		},
	}
	expr.Format(tb)
	if got := tb.String(); got != "(a, b)" {
		t.Fatalf("unexpected formatted search_condition_list: %q", got)
	}
}

func TestQueryCoverage_Bytes2DToStrings(t *testing.T) {
	if got := bytes2DToStrings(nil); got != nil {
		t.Fatalf("bytes2DToStrings(nil) = %#v, want nil", got)
	}
	got := bytes2DToStrings([][]byte{[]byte("a"), []byte("b")})
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("bytes2DToStrings unexpected output: %#v", got)
	}
}

func TestQueryCoverage_DataTypeFromTypeName(t *testing.T) {
	lx := &Scanner{}
	cases := []struct {
		name string
		want uint8
	}{
		{"bool", TSDB_DATA_TYPE_BOOL},
		{"tinyint", TSDB_DATA_TYPE_TINYINT},
		{"smallint", TSDB_DATA_TYPE_SMALLINT},
		{"int", TSDB_DATA_TYPE_INT},
		{"integer", TSDB_DATA_TYPE_INT},
		{"bigint", TSDB_DATA_TYPE_BIGINT},
		{"float", TSDB_DATA_TYPE_FLOAT},
		{"double", TSDB_DATA_TYPE_DOUBLE},
		{"timestamp", TSDB_DATA_TYPE_TIMESTAMP},
		{"tinyint unsigned", TSDB_DATA_TYPE_UTINYINT},
		{"smallint unsigned", TSDB_DATA_TYPE_USMALLINT},
		{"int unsigned", TSDB_DATA_TYPE_UINT},
		{"bigint unsigned", TSDB_DATA_TYPE_UBIGINT},
		{"json", TSDB_DATA_TYPE_JSON},
		{"mediumblob", TSDB_DATA_TYPE_MEDIUMBLOB},
		{"blob", TSDB_DATA_TYPE_BLOB},
		{"binary(8)", TSDB_DATA_TYPE_BINARY},
		{"nchar(8)", TSDB_DATA_TYPE_NCHAR},
		{"varchar(8)", TSDB_DATA_TYPE_VARCHAR},
		{"varbinary(8)", TSDB_DATA_TYPE_VARBINARY},
		{"geometry(8)", TSDB_DATA_TYPE_GEOMETRY},
		{"decimal(10)", 0},
		{"decimal(10, 2)", 0},
		{"binary", TSDB_DATA_TYPE_BINARY},
		{"nchar", TSDB_DATA_TYPE_NCHAR},
		{"varchar", TSDB_DATA_TYPE_VARCHAR},
		{"varbinary", TSDB_DATA_TYPE_VARBINARY},
		{"decimal", 0},
		{"unknown_x", TSDB_DATA_TYPE_INT},
	}

	for _, tc := range cases {
		dt := dataTypeFromTypeName(lx, tc.name)
		if tc.want == 0 {
			if dt.Type == 0 {
				t.Fatalf("dataTypeFromTypeName(%q) produced zero type", tc.name)
			}
			continue
		}
		if dt.Type != tc.want {
			t.Fatalf("dataTypeFromTypeName(%q) type=%d want=%d", tc.name, dt.Type, tc.want)
		}
	}
}
