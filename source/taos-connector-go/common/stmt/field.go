package stmt

import (
	"database/sql/driver"
	"fmt"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/types"
)

type StmtField struct {
	Name      string `json:"name"`
	FieldType int8   `json:"field_type"`
	Precision uint8  `json:"precision"`
	Scale     uint8  `json:"scale"`
	Bytes     int32  `json:"bytes"`
}

func (s *StmtField) GetType() (*types.ColumnType, error) {
	switch s.FieldType {
	case common.TSDB_DATA_TYPE_BOOL:
		return &types.ColumnType{Type: types.TaosBoolType}, nil
	case common.TSDB_DATA_TYPE_TINYINT:
		return &types.ColumnType{Type: types.TaosTinyintType}, nil
	case common.TSDB_DATA_TYPE_SMALLINT:
		return &types.ColumnType{Type: types.TaosSmallintType}, nil
	case common.TSDB_DATA_TYPE_INT:
		return &types.ColumnType{Type: types.TaosIntType}, nil
	case common.TSDB_DATA_TYPE_BIGINT:
		return &types.ColumnType{Type: types.TaosBigintType}, nil
	case common.TSDB_DATA_TYPE_UTINYINT:
		return &types.ColumnType{Type: types.TaosUTinyintType}, nil
	case common.TSDB_DATA_TYPE_USMALLINT:
		return &types.ColumnType{Type: types.TaosUSmallintType}, nil
	case common.TSDB_DATA_TYPE_UINT:
		return &types.ColumnType{Type: types.TaosUIntType}, nil
	case common.TSDB_DATA_TYPE_UBIGINT:
		return &types.ColumnType{Type: types.TaosUBigintType}, nil
	case common.TSDB_DATA_TYPE_FLOAT:
		return &types.ColumnType{Type: types.TaosFloatType}, nil
	case common.TSDB_DATA_TYPE_DOUBLE:
		return &types.ColumnType{Type: types.TaosDoubleType}, nil
	case common.TSDB_DATA_TYPE_BINARY:
		return &types.ColumnType{Type: types.TaosBinaryType}, nil
	case common.TSDB_DATA_TYPE_VARBINARY:
		return &types.ColumnType{Type: types.TaosVarBinaryType}, nil
	case common.TSDB_DATA_TYPE_NCHAR:
		return &types.ColumnType{Type: types.TaosNcharType}, nil
	case common.TSDB_DATA_TYPE_TIMESTAMP:
		return &types.ColumnType{Type: types.TaosTimestampType}, nil
	case common.TSDB_DATA_TYPE_JSON:
		return &types.ColumnType{Type: types.TaosJsonType}, nil
	case common.TSDB_DATA_TYPE_GEOMETRY:
		return &types.ColumnType{Type: types.TaosGeometryType}, nil
	}
	return nil, fmt.Errorf("unsupported type: %d, name %s", s.FieldType, s.Name)
}

//revive:disable
const (
	TAOS_FIELD_COL = iota + 1
	TAOS_FIELD_TAG
	TAOS_FIELD_QUERY
	TAOS_FIELD_TBNAME
)

//revive:enable

type TaosStmt2BindData struct {
	TableName string
	Tags      []driver.Value   // row format
	Cols      [][]driver.Value // column format
}
