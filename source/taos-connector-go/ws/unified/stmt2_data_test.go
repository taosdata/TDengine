package unified

import (
	"testing"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/types"
)

// TestBuildStmt2InsertBindData verifies the expected behavior for this scenario.
func TestBuildStmt2InsertBindData(t *testing.T) {
	ts := time.Unix(1711111111, 0)
	data, err := buildStmt2InsertBindData(
		"tb1",
		param.NewParam(1).AddNchar("tag1"),
		param.NewColumnType(1).AddNchar(8),
		[]*param.Param{
			param.NewParam(2).AddTimestamp(ts, common.PrecisionMilliSecond).AddNull(),
			param.NewParam(2).AddInt(1).AddInt(2),
		},
		param.NewColumnType(2).AddTimestamp().AddInt(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if data.TableName != "tb1" {
		t.Fatalf("unexpected table name: %s", data.TableName)
	}
	if len(data.Tags) != 1 || data.Tags[0] != "tag1" {
		t.Fatalf("unexpected tags: %+v", data.Tags)
	}
	if len(data.Cols) != 2 {
		t.Fatalf("unexpected col count: %d", len(data.Cols))
	}
	expectTS := common.TimeToTimestamp(ts, common.PrecisionMilliSecond)
	if data.Cols[0][0] != expectTS || data.Cols[0][1] != nil {
		t.Fatalf("unexpected timestamp col: %+v", data.Cols[0])
	}
	if data.Cols[1][0] != int32(1) || data.Cols[1][1] != int32(2) {
		t.Fatalf("unexpected int col: %+v", data.Cols[1])
	}
}

// TestBuildStmt2QueryBindData verifies the expected behavior for this scenario.
func TestBuildStmt2QueryBindData(t *testing.T) {
	ts := time.Unix(1711111111, 123456789)
	data, err := buildStmt2QueryBindData([]*param.Param{
		param.NewParam(1).AddTimestamp(ts, common.PrecisionNanoSecond),
		param.NewParam(1).AddInt(9),
	}, param.NewColumnType(2).AddTimestamp().AddInt(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 1 {
		t.Fatalf("expect one query bind item, got %d", len(data))
	}
	if len(data[0].Cols) != 2 {
		t.Fatalf("unexpected query col count: %d", len(data[0].Cols))
	}
	if data[0].Cols[0][0] != ts.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected query timestamp value: %+v", data[0].Cols[0][0])
	}
	if data[0].Cols[1][0] != int32(9) {
		t.Fatalf("unexpected query int value: %+v", data[0].Cols[1][0])
	}
}

// TestBuildStmt2DataErrors verifies the expected behavior for this scenario.
func TestBuildStmt2DataErrors(t *testing.T) {
	_, err := buildStmt2InsertBindData("", nil, nil,
		[]*param.Param{
			param.NewParam(1).AddValue(struct{}{}),
		},
		nil,
		nil,
	)
	if err == nil {
		t.Fatal("expect insert normalize error")
	}
	_, err = buildStmt2QueryBindData(nil, nil, nil)
	if err == nil {
		t.Fatal("expect query error")
	}
}

// TestBuildStmt2InsertBindDataWithDecimalBindType verifies the expected behavior for this scenario.
func TestBuildStmt2InsertBindDataWithDecimalBindType(t *testing.T) {
	data, err := buildStmt2InsertBindData(
		"tb1",
		param.NewParam(1).AddDecimal("12.3400"),
		param.NewColumnType(1).AddDecimal(),
		[]*param.Param{
			param.NewParam(2).AddValue([]byte("56.7800")).AddDecimal("90.1200"),
		},
		param.NewColumnType(1).AddDecimal(),
		[]*commonstmt.Stmt2AllField{
			{
				Name:      "v1",
				FieldType: common.TSDB_DATA_TYPE_DECIMAL64,
				BindType:  commonstmt.TAOS_FIELD_COL,
			},
			{
				Name:      "t1",
				FieldType: common.TSDB_DATA_TYPE_DECIMAL,
				BindType:  commonstmt.TAOS_FIELD_TAG,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Tags) != 1 || data.Tags[0] != "12.3400" {
		t.Fatalf("unexpected decimal tags: %+v", data.Tags)
	}
	if len(data.Cols) != 1 || len(data.Cols[0]) != 2 {
		t.Fatalf("unexpected decimal cols: %+v", data.Cols)
	}
	if data.Cols[0][0] != "56.7800" || data.Cols[0][1] != "90.1200" {
		t.Fatalf("unexpected decimal col values: %+v", data.Cols[0])
	}
}

// TestBuildStmt2InsertBindDataWithDecimalBindTypeRejectsNonDecimalField verifies the expected behavior for this scenario.
func TestBuildStmt2InsertBindDataWithDecimalBindTypeRejectsNonDecimalField(t *testing.T) {
	_, err := buildStmt2InsertBindData(
		"tb1",
		nil,
		nil,
		[]*param.Param{
			param.NewParam(1).AddDecimal("1.2300"),
		},
		param.NewColumnType(1).AddDecimal(),
		[]*commonstmt.Stmt2AllField{
			{
				Name:      "v1",
				FieldType: common.TSDB_DATA_TYPE_INT,
				BindType:  commonstmt.TAOS_FIELD_COL,
			},
		},
	)
	if err == nil {
		t.Fatal("expect decimal/non-decimal field mismatch error")
	}
}

// TestBuildStmt2QueryBindDataWithDecimalBindType verifies the expected behavior for this scenario.
func TestBuildStmt2QueryBindDataWithDecimalBindType(t *testing.T) {
	data, err := buildStmt2QueryBindData([]*param.Param{
		param.NewParam(1).AddValue(types.TaosDecimal("10.2500")),
	}, param.NewColumnType(1).AddDecimal(), []*commonstmt.Stmt2AllField{
		{
			Name:      "q1",
			FieldType: common.TSDB_DATA_TYPE_DECIMAL,
			BindType:  commonstmt.TAOS_FIELD_QUERY,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 1 || len(data[0].Cols) != 1 || data[0].Cols[0][0] != "10.2500" {
		t.Fatalf("unexpected decimal query bind data: %+v", data)
	}
}
