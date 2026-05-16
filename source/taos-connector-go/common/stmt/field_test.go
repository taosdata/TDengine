package stmt

import (
	"testing"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/types"
)

func TestGetType(t *testing.T) {
	tests := []struct {
		name      string
		fieldType int8
		want      *types.ColumnType
		wantErr   bool
	}{
		{
			name:      "Test Bool Type",
			fieldType: common.TSDB_DATA_TYPE_BOOL,
			want:      &types.ColumnType{Type: types.TaosBoolType},
			wantErr:   false,
		},
		{
			name:      "Test TinyInt Type",
			fieldType: common.TSDB_DATA_TYPE_TINYINT,
			want:      &types.ColumnType{Type: types.TaosTinyintType},
			wantErr:   false,
		},
		{
			name:      "Test SmallInt Type",
			fieldType: common.TSDB_DATA_TYPE_SMALLINT,
			want:      &types.ColumnType{Type: types.TaosSmallintType},
			wantErr:   false,
		},
		{
			name:      "Test Int Type",
			fieldType: common.TSDB_DATA_TYPE_INT,
			want:      &types.ColumnType{Type: types.TaosIntType},
			wantErr:   false,
		},
		{
			name:      "Test BigInt Type",
			fieldType: common.TSDB_DATA_TYPE_BIGINT,
			want:      &types.ColumnType{Type: types.TaosBigintType},
			wantErr:   false,
		},
		{
			name:      "Test UTinyInt Type",
			fieldType: common.TSDB_DATA_TYPE_UTINYINT,
			want:      &types.ColumnType{Type: types.TaosUTinyintType},
			wantErr:   false,
		},
		{
			name:      "Test USmallInt Type",
			fieldType: common.TSDB_DATA_TYPE_USMALLINT,
			want:      &types.ColumnType{Type: types.TaosUSmallintType},
			wantErr:   false,
		},
		{
			name:      "Test UInt Type",
			fieldType: common.TSDB_DATA_TYPE_UINT,
			want:      &types.ColumnType{Type: types.TaosUIntType},
			wantErr:   false,
		},
		{
			name:      "Test UBigInt Type",
			fieldType: common.TSDB_DATA_TYPE_UBIGINT,
			want:      &types.ColumnType{Type: types.TaosUBigintType},
			wantErr:   false,
		},
		{
			name:      "Test Float Type",
			fieldType: common.TSDB_DATA_TYPE_FLOAT,
			want:      &types.ColumnType{Type: types.TaosFloatType},
			wantErr:   false,
		},
		{
			name:      "Test Double Type",
			fieldType: common.TSDB_DATA_TYPE_DOUBLE,
			want:      &types.ColumnType{Type: types.TaosDoubleType},
			wantErr:   false,
		},
		{
			name:      "Test Binary Type",
			fieldType: common.TSDB_DATA_TYPE_BINARY,
			want:      &types.ColumnType{Type: types.TaosBinaryType},
			wantErr:   false,
		},
		{
			name:      "Test VarBinary Type",
			fieldType: common.TSDB_DATA_TYPE_VARBINARY,
			want:      &types.ColumnType{Type: types.TaosVarBinaryType},
			wantErr:   false,
		},
		{
			name:      "Test Nchar Type",
			fieldType: common.TSDB_DATA_TYPE_NCHAR,
			want:      &types.ColumnType{Type: types.TaosNcharType},
			wantErr:   false,
		},
		{
			name:      "Test Timestamp Type",
			fieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			want:      &types.ColumnType{Type: types.TaosTimestampType},
			wantErr:   false,
		},
		{
			name:      "Test Json Type",
			fieldType: common.TSDB_DATA_TYPE_JSON,
			want:      &types.ColumnType{Type: types.TaosJsonType},
			wantErr:   false,
		},
		{
			name:      "Test Geometry Type",
			fieldType: common.TSDB_DATA_TYPE_GEOMETRY,
			want:      &types.ColumnType{Type: types.TaosGeometryType},
			wantErr:   false,
		},
		{
			name:      "Test Unsupported Type",
			fieldType: 0, // An undefined type
			want:      nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StmtField{
				FieldType: tt.fieldType,
			}

			got, err := s.GetType()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil && tt.want != nil && got.Type != tt.want.Type {
				t.Errorf("GetType() = %v, want %v", got, tt.want)
			}
		})
	}
}
