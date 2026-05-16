package stmt

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
)

type customInt int

func TestMarshalBinary(t *testing.T) {
	largeTableName := ""
	for i := 0; i < math.MaxUint16; i++ {
		largeTableName += "a"
	}
	type args struct {
		t         []*TaosStmt2BindData
		isInsert  bool
		fieldType []*Stmt2AllField
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "TestSetTableName",
			args: args{
				t: []*TaosStmt2BindData{
					{
						TableName: "test1",
					},
					{
						TableName: "",
					},
					{
						TableName: "test2",
					},
				},
				isInsert:  true,
				fieldType: nil,
			},
			want: []byte{
				// total Length
				0x2f, 0x00, 0x00, 0x00,
				// tableCount
				0x03, 0x00, 0x00, 0x00,
				// TagCount
				0x00, 0x00, 0x00, 0x00,
				// ColCount
				0x00, 0x00, 0x00, 0x00,
				// TableNamesOffset
				0x1c, 0x00, 0x00, 0x00,
				// TagsOffset
				0x00, 0x00, 0x00, 0x00,
				// ColOffset
				0x00, 0x00, 0x00, 0x00,
				// table names
				// TableNameLength
				0x06, 0x00,
				0x01, 0x00,
				0x06, 0x00,
				// test1
				0x74, 0x65, 0x73, 0x74, 0x31, 0x00,
				// nil
				0x00,
				// test2
				0x74, 0x65, 0x73, 0x74, 0x32, 0x00,
			},
			wantErr: false,
		},
		{
			name: "wrong TableName length",
			args: args{
				t: []*TaosStmt2BindData{
					{
						TableName: largeTableName,
					},
				},
				isInsert:  true,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "TestSetTableNameAndTags",
			args: args{
				t: []*TaosStmt2BindData{
					{
						TableName: "test1",
						Tags: []driver.Value{
							// ts 1726803356466
							time.Unix(1726803356, 466000000),
							// bool
							true,
							// tinyint
							int8(1),
							// smallint
							int16(2),
							// int
							int32(3),
							// bigint
							int64(4),
							// float
							float32(5.5),
							// double
							float64(6.6),
							// utinyint
							uint8(7),
							// usmallint
							uint16(8),
							// uint
							uint32(9),
							// ubigint
							uint64(10),
							// binary
							[]byte("binary"),
							// nchar
							"nchar",
							// geometry
							[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
							// varbinary
							[]byte("varbinary"),
						},
					},
					{
						TableName: "testnil",
						Tags: []driver.Value{
							// ts 1726803356466
							nil,
							// bool
							nil,
							// tinyint
							nil,
							// smallint
							nil,
							// int
							nil,
							// bigint
							nil,
							// float
							nil,
							// double
							nil,
							// utinyint
							nil,
							// usmallint
							nil,
							// uint
							nil,
							// ubigint
							nil,
							// binary
							nil,
							// nchar
							nil,
							// geometry
							nil,
							// varbinary
							nil,
						},
					},
					{
						TableName: "test2",
						Tags: []driver.Value{
							// ts 1726803356466
							time.Unix(1726803356, 466000000),
							// bool
							true,
							// tinyint
							int8(1),
							// smallint
							int16(2),
							// int
							int32(3),
							// bigint
							int64(4),
							// float
							float32(5.5),
							// double
							float64(6.6),
							// utinyint
							uint8(7),
							// usmallint
							uint16(8),
							// uint
							uint32(9),
							// ubigint
							uint64(10),
							// binary
							[]byte("binary"),
							// nchar
							"nchar",
							// geometry
							[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
							// varbinary
							[]byte("varbinary"),
						},
					},
				},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						Precision: common.PrecisionMilliSecond,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BINARY,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_NCHAR,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_VARBINARY,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want: []byte{
				// total Length
				0x8a, 0x04, 0x00, 0x00,
				// tableCount
				0x03, 0x00, 0x00, 0x00,
				// TagCount
				0x10, 0x00, 0x00, 0x00,
				// ColCount
				0x00, 0x00, 0x00, 0x00,
				// TableNamesOffset
				0x1c, 0x00, 0x00, 0x00,
				// TagsOffset
				0x36, 0x00, 0x00, 0x00,
				// ColOffset
				0x00, 0x00, 0x00, 0x00,
				// table names
				// TableNameLength
				0x06, 0x00,
				0x08, 0x00,
				0x06, 0x00,
				// test1
				0x74, 0x65, 0x73, 0x74, 0x31, 0x00,
				// testnil
				0x74, 0x65, 0x73, 0x74, 0x6e, 0x69, 0x6c, 0x00,
				// test2
				0x74, 0x65, 0x73, 0x74, 0x32, 0x00,

				// tags

				// tagsDataLength
				// table1 DataLength
				0x8c, 0x01, 0x00, 0x00,
				// table2 DataLength
				0x30, 0x01, 0x00, 0x00,
				// table3 DataLength
				0x8c, 0x01, 0x00, 0x00,

				// tagsData
				// table1 tags
				// tag1 timestamp
				// totalLength
				0x1a, 0x00, 0x00, 0x00,

				// type
				0x09, 0x00, 0x00, 0x00,

				// num
				0x01, 0x00, 0x00, 0x00,

				// isnull
				0x00,

				// haveLength
				0x00,

				//buffer length
				0x08, 0x00, 0x00, 0x00,

				// buffer
				0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

				// tag2 bool
				0x13, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,

				// tag3 tinyint
				0x13, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,

				// tag4 smallint
				0x14, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x02, 0x00, 0x00, 0x00,
				0x02, 0x00,

				// tag5 int
				0x16, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,

				// tag6 bigint
				0x1a, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// tag7 float
				0x16, 0x00, 0x00, 0x00,
				0x06, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x00, 0x00, 0xb0, 0x40,

				// tag8 double
				0x1a, 0x00, 0x00, 0x00,
				0x07, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0x40,

				// tag9 utinyint
				0x13, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x01, 0x00, 0x00, 0x00,
				0x07,

				// tag10 usmallint
				0x14, 0x00, 0x00, 0x00,
				0x0c, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x02, 0x00, 0x00, 0x00,
				0x08, 0x00,

				// tag11 uint
				0x16, 0x00, 0x00, 0x00,
				0x0d, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,

				// tag12 ubigint
				0x1a, 0x00, 0x00, 0x00,
				0x0e, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// tag13 binary
				0x1c, 0x00, 0x00, 0x00,
				0x08, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				// length
				0x06, 0x00, 0x00, 0x00,
				// buffer length
				0x06, 0x00, 0x00, 0x00,
				//buffer
				0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

				// tag14 nchar
				0x1b, 0x00, 0x00, 0x00,
				0x0a, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x05, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x6e, 0x63, 0x68, 0x61, 0x72,

				// tag15 geometry
				0x2b, 0x00, 0x00, 0x00,
				0x14, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x15, 0x00, 0x00, 0x00,
				0x15, 0x00, 0x00, 0x00,
				0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,

				// tag16 varbinary
				0x1f, 0x00, 0x00, 0x00,
				0x10, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x09, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,
				0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

				// table 2 tags
				// tag1 timestamp nil
				// TotalLength
				0x12, 0x00, 0x00, 0x00,
				// type
				0x09, 0x00, 0x00, 0x00,
				// num
				0x01, 0x00, 0x00, 0x00,
				// isnull
				0x01,
				// haveLength
				0x00,
				// buffer length
				0x00, 0x00, 0x00, 0x00,

				// tag2 bool nil
				0x12, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag3 tinyint nil
				0x12, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag4 smallint nil
				0x12, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag5 int nil
				0x12, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				//  tag6 bigint nil
				0x12, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag7 float nil
				0x12, 0x00, 0x00, 0x00,
				0x06, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag8 double nil
				0x12, 0x00, 0x00, 0x00,
				0x07, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag9 utinyint nil
				0x12, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag10 usmallint nil
				0x12, 0x00, 0x00, 0x00,
				0x0c, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag11 uint nil
				0x12, 0x00, 0x00, 0x00,
				0x0d, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag12 ubigint nil
				0x12, 0x00, 0x00, 0x00,
				0x0e, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag13 binary nil
				0x16, 0x00, 0x00, 0x00,
				0x08, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x01,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag14 nchar nil
				0x16, 0x00, 0x00, 0x00,
				0x0a, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x01,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag15 geometry nil
				0x16, 0x00, 0x00, 0x00,
				0x14, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x01,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,

				// tag16 varbinary nil
				0x16, 0x00, 0x00, 0x00,
				0x10, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,
				0x01,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,

				// table 3 tags
				// tag1 timestamp
				0x1a, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

				// tag2 bool
				0x13, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,

				// tag3 tinyint
				0x13, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,

				// tag4 smallint
				0x14, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x02, 0x00, 0x00, 0x00,
				0x02, 0x00,

				// tag5 int
				0x16, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,

				// tag6 bigint
				0x1a, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,

				// tag7 float
				0x00, 0x00, 0x00, 0x00,
				0x16, 0x00, 0x00, 0x00,
				0x06, 0x00, 0x00, 0x00,
				0x01,
				0x00,
				0x00, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,
				0x00, 0x00, 0xb0, 0x40,

				// tag8 double
				0x1a, 0x00, 0x00, 0x00,
				0x07, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0x40,

				// tag9 utinyint
				0x13, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x01, 0x00, 0x00, 0x00,
				0x07,

				// tag10 usmallint
				0x14, 0x00, 0x00, 0x00,
				0x0c, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x02, 0x00, 0x00, 0x00,
				0x08, 0x00,

				// tag11 uint
				0x16, 0x00, 0x00, 0x00,
				0x0d, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,

				// tag12 ubigint
				0x1a, 0x00, 0x00, 0x00,
				0x0e, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// tag13 binary
				0x1c, 0x00, 0x00, 0x00,
				0x08, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x06, 0x00, 0x00, 0x00,
				0x06, 0x00, 0x00, 0x00,
				0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

				// tag14 nchar
				0x1b, 0x00, 0x00, 0x00,
				0x0a, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x05, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x6e, 0x63, 0x68, 0x61, 0x72,

				// tag15 geometry
				0x2b, 0x00, 0x00, 0x00,
				0x14, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x15, 0x00, 0x00, 0x00,
				0x15, 0x00, 0x00, 0x00,
				0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,

				// tag16 varbinary
				0x1f, 0x00, 0x00, 0x00,
				0x10, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x09, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,
				0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,
			},
			wantErr: false,
		},
		{
			name: "TestAllData",
			args: args{
				t: []*TaosStmt2BindData{
					{
						TableName: "test1",
						Tags: []driver.Value{
							// ts 1726803356466
							time.Unix(1726803356, 466000000),
							// bool
							true,
							// tinyint
							int8(1),
							// smallint
							int16(2),
							// int
							int32(3),
							// bigint
							int64(4),
							// float
							float32(5.5),
							// double
							float64(6.6),
							// utinyint
							uint8(7),
							// usmallint
							uint16(8),
							// uint
							uint32(9),
							// ubigint
							uint64(10),
							// binary
							[]byte("binary"),
							// nchar
							"nchar",
							// geometry
							[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
							// varbinary
							[]byte("varbinary"),
						},
						Cols: [][]driver.Value{
							{
								// ts 1726803356466
								time.Unix(1726803356, 466000000),
								// ts 1726803357466
								time.Unix(1726803357, 466000000),
								// ts 1726803358466
								time.Unix(1726803358, 466000000),
							},
							{
								// BOOL
								true,
								nil,
								false,
							},
							{
								// TINYINT
								int8(11),
								nil,
								int8(12),
							},
							{
								// SMALLINT
								int16(11),
								nil,
								int16(12),
							},
							{
								// INT
								int32(11),
								nil,
								int32(12),
							},
							{
								// BIGINT
								int64(11),
								nil,
								int64(12),
							},
							{
								// FLOAT
								float32(11.2),
								nil,
								float32(12.2),
							},
							{
								// DOUBLE
								float64(11.2),
								nil,
								float64(12.2),
							},
							{
								// TINYINT UNSIGNED
								uint8(11),
								nil,
								uint8(12),
							},
							{
								// SMALLINT UNSIGNED
								uint16(11),
								nil,
								uint16(12),
							},
							{
								// INT UNSIGNED
								uint32(11),
								nil,
								uint32(12),
							},
							{
								// BIGINT UNSIGNED
								uint64(11),
								nil,
								uint64(12),
							},
							{
								// BINARY
								[]byte("binary1"),
								nil,
								[]byte("binary2"),
							},
							{
								// NCHAR
								"nchar1",
								nil,
								"nchar2",
							},
							{
								// GEOMETRY `point(100 100)`
								[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
								nil,
								[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
							},
							{
								// VARBINARY
								[]byte("varbinary1"),
								nil,
								[]byte("varbinary2"),
							},
						},
					},
				},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						Precision: common.PrecisionMilliSecond,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BINARY,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_NCHAR,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
						BindType:  TAOS_FIELD_TAG,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_VARBINARY,
						BindType:  TAOS_FIELD_TAG,
					},

					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						Precision: common.PrecisionMilliSecond,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BINARY,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_NCHAR,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_GEOMETRY,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_VARBINARY,
						BindType:  TAOS_FIELD_COL,
					},
				},
			},
			want: []byte{
				// TotalLength
				0x19, 0x04, 0x00, 0x00,
				// tableCount
				0x01, 0x00, 0x00, 0x00,
				// TagCount
				0x10, 0x00, 0x00, 0x00,
				// ColCount
				0x10, 0x00, 0x00, 0x00,
				// TableNamesOffset
				0x1c, 0x00, 0x00, 0x00,
				// TagsOffset
				0x24, 0x00, 0x00, 0x00,
				// ColsOffset
				0xb4, 0x01, 0x00, 0x00,

				// TableNameLength
				0x06, 0x00,
				// TableNameBuffer
				0x74, 0x65, 0x73, 0x74, 0x31, 0x00,

				// TagsDataLength
				0x8c, 0x01, 0x00, 0x00,

				// TagsBuffer

				// tag1 timestamp
				// TotalLength
				0x1a, 0x00, 0x00, 0x00,
				// type
				0x09, 0x00, 0x00, 0x00,
				// num
				0x01, 0x00, 0x00, 0x00,
				// isnull
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x08, 0x00, 0x00, 0x00,
				// buffer
				0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

				// tag2 bool
				0x13, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,

				// tag3 tinyint
				0x13, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x01, 0x00, 0x00, 0x00,
				0x01,

				// tag4 smallint
				0x14, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x02, 0x00, 0x00, 0x00,
				0x02, 0x00,

				// tag5 int
				0x16, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,

				// tag6 bigint
				0x1a, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// tag7 float
				0x16, 0x00, 0x00, 0x00,
				0x06, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x00, 0x00, 0xb0, 0x40,

				// tag8 double
				0x1a, 0x00, 0x00, 0x00,
				0x07, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x1a, 0x40,

				// tag9 utinyint
				0x13, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x01, 0x00, 0x00, 0x00,
				0x07,

				// tag10 usmallint
				0x14, 0x00, 0x00, 0x00,
				0x0c, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x02, 0x00, 0x00, 0x00,
				0x08, 0x00,

				// tag11 uint
				0x16, 0x00, 0x00, 0x00,
				0x0d, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,

				// tag12 ubigint
				0x1a, 0x00, 0x00, 0x00,
				0x0e, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// tag13 binary
				0x1c, 0x00, 0x00, 0x00,
				0x08, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				// haveLength
				0x01,
				// length
				0x06, 0x00, 0x00, 0x00,
				//buffer length
				0x06, 0x00, 0x00, 0x00,
				0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

				// tag14 nchar
				0x1b, 0x00, 0x00, 0x00,
				0x0a, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x05, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x6e, 0x63, 0x68, 0x61, 0x72,

				// tag15 geometry
				0x2b, 0x00, 0x00, 0x00,
				0x14, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x15, 0x00, 0x00, 0x00,
				0x15, 0x00, 0x00, 0x00,
				0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,

				// tag16 varbinary
				0x1f, 0x00, 0x00, 0x00,
				0x10, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x01,
				0x09, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,
				0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79,

				// ColDataLength
				0x61, 0x02, 0x00, 0x00,

				// ColBuffer
				// col1 timestamp
				// TotalLength
				0x2c, 0x00, 0x00, 0x00,
				// Type
				0x09, 0x00, 0x00, 0x00,
				// Num
				0x03, 0x00, 0x00, 0x00,
				// IsNull
				0x00, 0x00, 0x00,
				//haveLength
				0x00,
				// BufferLength
				0x18, 0x00, 0x00, 0x00,
				// Buffer
				0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,
				0x1a, 0x2f, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,
				0x02, 0x33, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

				// col2 bool
				0x17, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				// is null, row index 1 is null
				0x00, 0x01, 0x00,
				0x00,
				0x03, 0x00, 0x00, 0x00,

				// row0
				0x01,
				// row1
				0x00,
				// row2
				0x00,

				// col3 tinyint
				0x17, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x03, 0x00, 0x00, 0x00,

				0x0b,
				0x00,
				0x0c,

				// col4 smallint
				0x1a, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x06, 0x00, 0x00, 0x00,

				0x0b, 0x00,
				0x00, 0x00,
				0x0c, 0x00,

				// col5 int
				0x20, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x0c, 0x00, 0x00, 0x00,

				0x0b, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x0c, 0x00, 0x00, 0x00,

				// col6 bigint
				0x2c, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x18, 0x00, 0x00, 0x00,

				0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// col7 float
				0x20, 0x00, 0x00, 0x00,
				0x06, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x0c, 0x00, 0x00, 0x00,
				0x33, 0x33, 0x33, 0x41,
				0x00, 0x00, 0x00, 0x00,
				0x33, 0x33, 0x43, 0x41,

				// col8 double
				0x2c, 0x00, 0x00, 0x00,
				0x07, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x18, 0x00, 0x00, 0x00,

				0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x26, 0x40,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x28, 0x40,

				// col9 utinyint
				0x17, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x03, 0x00, 0x00, 0x00,

				0x0b,
				0x00,
				0x0c,

				// col10 usmallint
				0x1a, 0x00, 0x00, 0x00,
				0x0c, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x06, 0x00, 0x00, 0x00,

				0x0b, 0x00,
				0x00, 0x00,
				0x0c, 0x00,

				// col11 uint
				0x20, 0x00, 0x00, 0x00,
				0x0d, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x0c, 0x00, 0x00, 0x00,

				0x0b, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x0c, 0x00, 0x00, 0x00,

				// col12 ubigint
				0x2C, 0x00, 0x00, 0x00,
				0x0e, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x00,
				0x18, 0x00, 0x00, 0x00,

				0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				// col13 binary
				0x2e, 0x00, 0x00, 0x00,
				0x08, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				// have length
				0x01,
				// length
				0x07, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x07, 0x00, 0x00, 0x00,
				// buffer length
				0x0e, 0x00, 0x00, 0x00,
				// buffer
				0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x31,
				0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32,

				// col14 nchar
				0x2c, 0x00, 0x00, 0x00,
				0x0a, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x01,
				// length
				0x06, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x06, 0x00, 0x00, 0x00,
				// buffer length
				0x0c, 0x00, 0x00, 0x00,
				// buffer
				0x6e, 0x63, 0x68, 0x61, 0x72, 0x31,
				0x6e, 0x63, 0x68, 0x61, 0x72, 0x32,

				// col15 geometry
				0x4a, 0x00, 0x00, 0x00,
				0x14, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x01,
				// length
				0x15, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x15, 0x00, 0x00, 0x00,
				// buffer length
				0x2a, 0x00, 0x00, 0x00,
				// buffer
				0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,
				0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40,

				// col16 varbinary
				0x34, 0x00, 0x00, 0x00,
				0x10, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,
				0x00, 0x01, 0x00,
				0x01,
				// length
				0x0a, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x0a, 0x00, 0x00, 0x00,
				// buffer length
				0x14, 0x00, 0x00, 0x00,
				// buffer
				0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x31,
				0x76, 0x61, 0x72, 0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x32,
			},
			wantErr: false,
		},
		{
			name: "TestQuery",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Cols: [][]driver.Value{
							{
								// ts 1726803356466
								time.Unix(1726803356, 466000000).UTC(),
							},
							{
								// BOOL
								true,
							},
							{
								// TINYINT
								int8(11),
							},
							{
								// SMALLINT
								int16(11),
							},
							{
								// INT
								int32(11),
							},
							{
								// BIGINT
								int64(11),
							},
							{
								// FLOAT
								float32(11.2),
							},
							{
								// DOUBLE
								float64(11.2),
							},
							{
								// TINYINT UNSIGNED
								uint8(11),
							},
							{
								// SMALLINT UNSIGNED
								uint16(11),
							},
							{
								// INT UNSIGNED
								uint32(11),
							},
							{
								// BIGINT UNSIGNED
								uint64(11),
							},
							{
								// Bytes
								[]byte("binary1"),
							},
							{
								// String
								"nchar1",
							},
						},
					},
				},
				isInsert: false,
				fieldType: []*Stmt2AllField{
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
					{
						BindType: TAOS_FIELD_QUERY,
					},
				},
			},
			want: []byte{
				// total Length
				0x78, 0x01, 0x00, 0x00,
				// tableCount
				0x01, 0x00, 0x00, 0x00,
				// TagCount
				0x00, 0x00, 0x00, 0x00,
				// ColCount
				0x0e, 0x00, 0x00, 0x00,
				// TableNamesOffset
				0x00, 0x00, 0x00, 0x00,
				// TagsOffset
				0x00, 0x00, 0x00, 0x00,
				// ColOffset
				0x1c, 0x00, 0x00, 0x00,
				// cols
				// col length
				0x58, 0x01, 0x00, 0x00,
				//table 0 cols
				//col 0
				//total length
				0x2e, 0x00, 0x00, 0x00,
				//type
				0x08, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x01,
				// length
				0x18, 0x00, 0x00, 0x00,
				// buffer length
				0x18, 0x00, 0x00, 0x00,
				0x32, 0x30, 0x32, 0x34, 0x2d, 0x30, 0x39, 0x2d, 0x32, 0x30, 0x54, 0x30, 0x33, 0x3a, 0x33, 0x35, 0x3a, 0x35, 0x36, 0x2e, 0x34, 0x36, 0x36, 0x5a,

				//col 1
				//total length
				0x13, 0x00, 0x00, 0x00,
				//type
				0x01, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x01, 0x00, 0x00, 0x00,
				0x01,

				//col 2
				//total length
				0x13, 0x00, 0x00, 0x00,
				//type
				0x02, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x01, 0x00, 0x00, 0x00,
				0x0b,

				//col 3
				//total length
				0x14, 0x00, 0x00, 0x00,
				//type
				0x03, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x02, 0x00, 0x00, 0x00,
				0x0b, 0x00,

				//col 4
				//total length
				0x16, 0x00, 0x00, 0x00,
				//type
				0x04, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x04, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00,

				//col 5
				//total length
				0x1a, 0x00, 0x00, 0x00,
				//type
				0x05, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x08, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				//col 6
				//total length
				0x16, 0x00, 0x00, 0x00,
				//type
				0x06, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x04, 0x00, 0x00, 0x00,
				0x33, 0x33, 0x33, 0x41,

				//col 7
				//total length
				0x1a, 0x00, 0x00, 0x00,
				//type
				0x07, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x08, 0x00, 0x00, 0x00,
				0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x26, 0x40,

				//col 8
				//total length
				0x13, 0x00, 0x00, 0x00,
				//type
				0x0b, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x01, 0x00, 0x00, 0x00,
				0x0b,

				//col 9
				//total length
				0x14, 0x00, 0x00, 0x00,
				//type
				0x0c, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x02, 0x00, 0x00, 0x00,
				0x0b, 0x00,

				//col 10
				//total length
				0x16, 0x00, 0x00, 0x00,
				//type
				0x0d, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x04, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00,

				//col 11
				//total length
				0x1a, 0x00, 0x00, 0x00,
				//type
				0x0e, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x08, 0x00, 0x00, 0x00,
				0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				//col 12
				//total length
				0x1d, 0x00, 0x00, 0x00,
				//type
				0x08, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x01,
				// length
				0x07, 0x00, 0x00, 0x00,
				// buffer length
				0x07, 0x00, 0x00, 0x00,
				0x62, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x31,

				//col 13
				//total length
				0x1c, 0x00, 0x00, 0x00,
				//type
				0x08, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x01,
				// length
				0x06, 0x00, 0x00, 0x00,
				// buffer length
				0x06, 0x00, 0x00, 0x00,
				0x6e, 0x63, 0x68, 0x61, 0x72, 0x31,
			},
			wantErr: false,
		},
		{
			name: "Three Table",
			args: args{
				t: []*TaosStmt2BindData{
					{
						TableName: "table1",
						Cols: [][]driver.Value{
							{
								// ts 1726803356466
								time.Unix(1726803356, 466000000),
							},
							{
								int64(1),
							},
						},
						Tags: []driver.Value{int32(1)},
					},
					{
						TableName: "table2",
						Cols: [][]driver.Value{
							{
								// ts 1726803356466
								time.Unix(1726803356, 466000000),
							},
							{
								int64(2),
							},
						},
						Tags: []driver.Value{int32(2)},
					},
					{
						TableName: "table3",
						Cols: [][]driver.Value{
							{
								// ts 1726803356466
								time.Unix(1726803356, 466000000),
							},
							{
								int64(3),
							},
						},
						Tags: []driver.Value{int32(3)},
					},
				},
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						Precision: common.PrecisionMilliSecond,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
						BindType:  TAOS_FIELD_COL,
					},
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
				isInsert: true,
			},
			want: []byte{
				// TotalLength
				0x2d, 0x01, 0x00, 0x00,
				// tableCount
				0x03, 0x00, 0x00, 0x00,
				// TagCount
				0x01, 0x00, 0x00, 0x00,
				// ColCount
				0x02, 0x00, 0x00, 0x00,
				// TableNamesOffset
				0x1c, 0x00, 0x00, 0x00,
				// TagsOffset
				0x37, 0x00, 0x00, 0x00,
				// ColsOffset
				0x85, 0x00, 0x00, 0x00,
				// TableNameLength
				0x07, 0x00,
				0x07, 0x00,
				0x07, 0x00,
				// TableNameBuffer
				0x74, 0x61, 0x62, 0x6c, 0x65, 0x31, 0x00,
				0x74, 0x61, 0x62, 0x6c, 0x65, 0x32, 0x00,
				0x74, 0x61, 0x62, 0x6c, 0x65, 0x33, 0x00,
				// TagsDataLength
				0x16, 0x00, 0x00, 0x00,
				0x16, 0x00, 0x00, 0x00,
				0x16, 0x00, 0x00, 0x00,
				// TagsBuffer
				0x16, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,

				0x16, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x00, 0x00,

				0x16, 0x00, 0x00, 0x00,
				0x04, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x04, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00,

				// ColDataLength
				0x34, 0x00, 0x00, 0x00,
				0x34, 0x00, 0x00, 0x00,
				0x34, 0x00, 0x00, 0x00,

				// ColBuffer
				0x1a, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

				0x1a, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				0x1a, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

				0x1a, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

				0x1a, 0x00, 0x00, 0x00,
				0x09, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,

				0x1a, 0x00, 0x00, 0x00,
				0x05, 0x00, 0x00, 0x00,
				0x01, 0x00, 0x00, 0x00,
				0x00,
				0x00,
				0x08, 0x00, 0x00, 0x00,
				0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			wantErr: false,
		},
		{
			name: "empty",
			args: args{
				t:         nil,
				isInsert:  false,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong tag count",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Tags: []driver.Value{int32(1)},
					},
				},
				isInsert:  true,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong col count",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Cols: [][]driver.Value{
							{
								int32(1),
							},
						},
					},
				},
				isInsert:  true,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query has tag type",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Cols: [][]driver.Value{
							{
								int32(1),
							},
						},
					},
				},
				isInsert: false,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query has col type",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Cols: [][]driver.Value{
							{
								int32(1),
							},
						},
					},
				},
				isInsert: false,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
						BindType:  TAOS_FIELD_COL,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query has multi data",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Cols: [][]driver.Value{
							{
								int32(1),
							},
						},
					},
					{
						Cols: [][]driver.Value{
							{
								int32(1),
							},
						},
					},
				},
				isInsert:  false,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query has tablename",
			args: args{
				t: []*TaosStmt2BindData{
					{
						TableName: "table1",
					},
				},
				isInsert:  false,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query has tag",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Tags: []driver.Value{int32(1)},
					},
				},
				isInsert:  false,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query without data",
			args: args{
				t: []*TaosStmt2BindData{
					{},
				},
				isInsert:  false,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "query with multi rows",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Cols: [][]driver.Value{
							{
								int32(1),
								int32(1),
							},
						},
					},
				},
				isInsert:  false,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong bool",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{int32(1)},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_BOOL,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong tinyint",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_TINYINT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong smallint",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_SMALLINT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong int",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_INT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong bigint",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_BIGINT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong tinyint unsigned",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_UTINYINT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong smallint unsigned",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_USMALLINT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong int unsigned",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_UINT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong bigint unsigned",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_UBIGINT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong float",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_FLOAT,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong double",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_DOUBLE,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong binary",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_BINARY,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "wrong timestamp",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{true},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "insert nil timestamp",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Cols: [][]driver.Value{
							{
								time.Unix(1726803356, 466000000),
								nil,
							},
						},
					},
				},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						BindType:  TAOS_FIELD_COL,
					},
				},
			},
			want: []byte{
				// total Length
				0x43, 0x00, 0x00, 0x00,
				// tableCount
				0x01, 0x00, 0x00, 0x00,
				// TagCount
				0x00, 0x00, 0x00, 0x00,
				// ColCount
				0x01, 0x00, 0x00, 0x00,
				// TableNamesOffset
				0x00, 0x00, 0x00, 0x00,
				// TagsOffset
				0x00, 0x00, 0x00, 0x00,
				// ColOffset
				0x1c, 0x00, 0x00, 0x00,
				// cols
				// col length
				0x23, 0x00, 0x00, 0x00,
				//table 0 cols
				//col 0
				//total length
				0x23, 0x00, 0x00, 0x00,
				//type
				0x09, 0x00, 0x00, 0x00,
				//num
				0x02, 0x00, 0x00, 0x00,
				//is null
				0x00,
				0x01,
				// haveLength
				0x00,
				// buffer length
				0x10, 0x00, 0x00, 0x00,
				0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			wantErr: false,
		},
		{
			name: "query bool false",
			args: args{
				t: []*TaosStmt2BindData{{
					Cols: [][]driver.Value{
						{false},
					},
				}},
				isInsert: false,
				fieldType: []*Stmt2AllField{
					{
						BindType: TAOS_FIELD_QUERY,
					},
				},
			},
			want: []byte{
				// total Length
				0x33, 0x00, 0x00, 0x00,
				// tableCount
				0x01, 0x00, 0x00, 0x00,
				// TagCount
				0x00, 0x00, 0x00, 0x00,
				// ColCount
				0x01, 0x00, 0x00, 0x00,
				// TableNamesOffset
				0x00, 0x00, 0x00, 0x00,
				// TagsOffset
				0x00, 0x00, 0x00, 0x00,
				// ColOffset
				0x1c, 0x00, 0x00, 0x00,
				// cols
				// col length
				0x13, 0x00, 0x00, 0x00,
				//table 0 cols
				//col 0
				//total length
				0x13, 0x00, 0x00, 0x00,
				//type
				0x01, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x01, 0x00, 0x00, 0x00,
				0x00,
			},
			wantErr: false,
		},
		{
			name: "query unsupported type",
			args: args{
				t: []*TaosStmt2BindData{{
					Cols: [][]driver.Value{
						{customInt(1)},
					},
				}},
				isInsert:  false,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "insert unsupported type",
			args: args{
				t: []*TaosStmt2BindData{{
					Cols: [][]driver.Value{
						{int32(1)},
					},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_NULL,
						BindType:  TAOS_FIELD_COL,
					},
				},
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "nil",
			args: args{
				t: []*TaosStmt2BindData{
					{
						Cols: nil,
					},
				},
				isInsert:  true,
				fieldType: nil,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "int64 timestamp",
			args: args{
				t: []*TaosStmt2BindData{{
					Tags: []driver.Value{int64(1726803356466)},
				}},
				isInsert: true,
				fieldType: []*Stmt2AllField{
					{
						FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
						BindType:  TAOS_FIELD_TAG,
					},
				},
			},
			want: []byte{
				// total Length
				0x3a, 0x00, 0x00, 0x00,
				// tableCount
				0x01, 0x00, 0x00, 0x00,
				// TagCount
				0x01, 0x00, 0x00, 0x00,
				// ColCount
				0x00, 0x00, 0x00, 0x00,
				// TableNamesOffset
				0x00, 0x00, 0x00, 0x00,
				// TagsOffset
				0x1c, 0x00, 0x00, 0x00,
				// ColOffset
				0x00, 0x00, 0x00, 0x00,
				// tags
				// table length
				0x1a, 0x00, 0x00, 0x00,
				//table 0 tags
				//tag 0
				//total length
				0x1a, 0x00, 0x00, 0x00,
				//type
				0x09, 0x00, 0x00, 0x00,
				//num
				0x01, 0x00, 0x00, 0x00,
				//is null
				0x00,
				// haveLength
				0x00,
				// buffer length
				0x08, 0x00, 0x00, 0x00,
				0x32, 0x2b, 0x80, 0x0d, 0x92, 0x01, 0x00, 0x00,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MarshalStmt2Binary(tt.args.t, tt.args.isInsert, tt.args.fieldType)
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalStmt2Binary() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
			got, err = marshalStmt2BinaryLegacy(tt.args.t, tt.args.isInsert, tt.args.fieldType)
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalStmt2Binary() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMarshalStmt2Binary2ColRowsMismatch(t *testing.T) {
	fields := []*Stmt2AllField{
		{
			Name:      "c1",
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  TAOS_FIELD_COL,
		},
		{
			Name:      "c2",
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  TAOS_FIELD_COL,
		},
	}
	tests := []struct {
		name string
		cols [][]driver.Value
	}{
		{
			name: "first col longer",
			cols: [][]driver.Value{
				{int32(1), int32(2)},
				{int32(3)},
			},
		},
		{
			name: "first col shorter",
			cols: [][]driver.Value{
				{int32(1)},
				{int32(2), int32(3)},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			assert.NotPanics(t, func() {
				_, err = MarshalStmt2Binary([]*TaosStmt2BindData{
					{
						Cols: tt.cols,
					},
				}, true, fields)
			})
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), "col row count not match")
			}
		})
	}
}

func TestMarshalStmt2Binary2BoolWithNil(t *testing.T) {
	bindData := []*TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{true, nil, true},
			},
		},
	}
	fields := []*Stmt2AllField{
		{
			Name:      "b",
			FieldType: common.TSDB_DATA_TYPE_BOOL,
			BindType:  TAOS_FIELD_COL,
		},
	}
	want, err := marshalStmt2BinaryLegacy(bindData, true, fields)
	assert.NoError(t, err)
	got, err := MarshalStmt2Binary(bindData, true, fields)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestMarshalStmt2Binary2AllNullFixedBufferLength(t *testing.T) {
	bindData := []*TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{nil, nil},
			},
		},
	}
	fields := []*Stmt2AllField{
		{
			Name:      "c",
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  TAOS_FIELD_COL,
		},
	}
	want, err := marshalStmt2BinaryLegacy(bindData, true, fields)
	assert.NoError(t, err)
	got, err := MarshalStmt2Binary(bindData, true, fields)
	assert.NoError(t, err)
	assert.Equal(t, want, got)

	colOffset := int(binary.LittleEndian.Uint32(got[ColsOffsetPosition : ColsOffsetPosition+4]))
	colDataOffset := colOffset + 4
	bufferLengthOffset := colDataOffset + BindDataIsNullOffset + 2 + 1
	bufferLength := binary.LittleEndian.Uint32(got[bufferLengthOffset : bufferLengthOffset+4])
	assert.EqualValues(t, 0, bufferLength)
}

func TestMarshalStmt2Binary2TBNameFieldWithEmptyTableNameInsert(t *testing.T) {
	bindData := []*TaosStmt2BindData{
		{
			TableName: "",
			Cols: [][]driver.Value{
				{int32(1)},
			},
		},
	}
	fields := []*Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			BindType:  TAOS_FIELD_TBNAME,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  TAOS_FIELD_COL,
		},
	}
	want, err := marshalStmt2BinaryLegacy(bindData, true, fields)
	assert.NoError(t, err)
	got, err := MarshalStmt2Binary(bindData, true, fields)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestMarshalStmt2Binary2TBNameFieldWithEmptyTableNameQuery(t *testing.T) {
	bindData := []*TaosStmt2BindData{
		{
			Cols: [][]driver.Value{
				{int32(1)},
			},
		},
	}
	fields := []*Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			BindType:  TAOS_FIELD_TBNAME,
		},
	}
	want, err := marshalStmt2BinaryLegacy(bindData, false, fields)
	assert.NoError(t, err)
	got, err := MarshalStmt2Binary(bindData, false, fields)
	assert.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestIsVarDataTypeIncludesDecimal(t *testing.T) {
	assert.True(t, IsVarDataType(common.TSDB_DATA_TYPE_DECIMAL))
	assert.True(t, IsVarDataType(common.TSDB_DATA_TYPE_DECIMAL64))
	assert.True(t, IsVarDataType(common.TSDB_DATA_TYPE_BLOB))
	assert.False(t, IsVarDataType(common.TSDB_DATA_TYPE_INT))
	assert.False(t, IsVarDataType(-1))
	assert.False(t, IsVarDataType(common.TSDB_DATA_TYPE_MAX))
}

func TestMarshalStmt2Binary2InsertDecimalAndBlob(t *testing.T) {
	bindData := []*TaosStmt2BindData{
		{
			TableName: "tb1",
			Tags: []driver.Value{
				"12.3456",
				[]byte("98.7654"),
				[]byte{0x61, 0x62},
			},
			Cols: [][]driver.Value{
				{"1.2300", nil},
				{[]byte("4.5600"), "7.8900"},
				{[]byte{0x01, 0x02}, "blob_text"},
			},
		},
	}
	fields := []*Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			BindType:  TAOS_FIELD_TBNAME,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_DECIMAL,
			BindType:  TAOS_FIELD_TAG,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_DECIMAL64,
			BindType:  TAOS_FIELD_TAG,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_BLOB,
			BindType:  TAOS_FIELD_TAG,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_DECIMAL,
			BindType:  TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_DECIMAL64,
			BindType:  TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_BLOB,
			BindType:  TAOS_FIELD_COL,
		},
	}
	got, err := MarshalStmt2Binary(bindData, true, fields)
	assert.NoError(t, err)
	if assert.Greater(t, len(got), DataPosition) {
		assert.Equal(t, uint32(1), binary.LittleEndian.Uint32(got[CountPosition:CountPosition+4]))
		assert.Equal(t, uint32(3), binary.LittleEndian.Uint32(got[TagCountPosition:TagCountPosition+4]))
		assert.Equal(t, uint32(3), binary.LittleEndian.Uint32(got[ColCountPosition:ColCountPosition+4]))
	}
}

func TestMarshalStmt2Binary2InsertDecimalTypeMismatch(t *testing.T) {
	tests := []struct {
		name   string
		field  int8
		isTag  bool
		value  driver.Value
		errMsg string
	}{
		{
			name:   "decimal col type mismatch",
			field:  common.TSDB_DATA_TYPE_DECIMAL,
			isTag:  false,
			value:  int32(1),
			errMsg: "unsupported column type",
		},
		{
			name:   "decimal64 col type mismatch",
			field:  common.TSDB_DATA_TYPE_DECIMAL64,
			isTag:  false,
			value:  true,
			errMsg: "unsupported column type",
		},
		{
			name:   "decimal tag type mismatch",
			field:  common.TSDB_DATA_TYPE_DECIMAL,
			isTag:  true,
			value:  int32(1),
			errMsg: "unsupported tag type",
		},
		{
			name:   "decimal64 tag type mismatch",
			field:  common.TSDB_DATA_TYPE_DECIMAL64,
			isTag:  true,
			value:  float64(1.2),
			errMsg: "unsupported tag type",
		},
		{
			name:   "blob col type mismatch",
			field:  common.TSDB_DATA_TYPE_BLOB,
			isTag:  false,
			value:  int32(1),
			errMsg: "unsupported column type",
		},
		{
			name:   "blob tag type mismatch",
			field:  common.TSDB_DATA_TYPE_BLOB,
			isTag:  true,
			value:  true,
			errMsg: "unsupported tag type",
		},
	}

	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			item := &TaosStmt2BindData{TableName: "tb1"}
			field := &Stmt2AllField{
				FieldType: tc.field,
			}
			if tc.isTag {
				item.Tags = []driver.Value{tc.value}
				field.BindType = TAOS_FIELD_TAG
			} else {
				item.Cols = [][]driver.Value{{tc.value}}
				field.BindType = TAOS_FIELD_COL
			}
			_, err := MarshalStmt2Binary([]*TaosStmt2BindData{item}, true, []*Stmt2AllField{
				{
					FieldType: common.TSDB_DATA_TYPE_BINARY,
					BindType:  TAOS_FIELD_TBNAME,
				},
				field,
			})
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestWriteBindTagErrorBranches(t *testing.T) {
	buffer := make([]byte, 256)

	_, err := writeBindTag([]*Stmt2AllField{
		{
			Name:      "unsupported",
			FieldType: common.TSDB_DATA_TYPE_NULL,
		},
	}, []driver.Value{int32(1)}, buffer, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "tag field type not support")

	_, err = writeBindTag([]*Stmt2AllField{
		{
			Name:      "decimal",
			FieldType: common.TSDB_DATA_TYPE_DECIMAL,
		},
	}, []driver.Value{int32(1)}, buffer, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expect string or []byte")
}

func TestWriteBindTagDecimalAndBlob(t *testing.T) {
	buffer := make([]byte, 256)
	fields := []*Stmt2AllField{
		{
			Name:      "tag_decimal",
			FieldType: common.TSDB_DATA_TYPE_DECIMAL,
		},
		{
			Name:      "tag_blob",
			FieldType: common.TSDB_DATA_TYPE_BLOB,
		},
	}
	values := []driver.Value{
		"12.3400",
		[]byte{0x01, 0x02, 0x03},
	}
	end, err := writeBindTag(fields, values, buffer, 0)
	assert.NoError(t, err)
	if !assert.Greater(t, end, 0) {
		return
	}

	assert.Equal(t, uint32(common.TSDB_DATA_TYPE_DECIMAL), binary.LittleEndian.Uint32(buffer[DataTypeOffset:DataTypeOffset+4]))
	assert.Equal(t, uint32(1), binary.LittleEndian.Uint32(buffer[NumOffset:NumOffset+4]))
	assert.Equal(t, byte(1), buffer[HaveLengthOffset])
	assert.Equal(t, uint32(7), binary.LittleEndian.Uint32(buffer[HaveLengthOffset+1:HaveLengthOffset+1+4]))
	assert.Equal(t, uint32(7), binary.LittleEndian.Uint32(buffer[HaveLengthOffset+1+4:HaveLengthOffset+1+8]))
	assert.Equal(t, []byte("12.3400"), buffer[HaveLengthOffset+1+8:HaveLengthOffset+1+8+7])

	firstTotal := int(binary.LittleEndian.Uint32(buffer[TotalLengthOffset : TotalLengthOffset+4]))
	assert.Equal(t, uint32(common.TSDB_DATA_TYPE_BLOB), binary.LittleEndian.Uint32(buffer[firstTotal+DataTypeOffset:firstTotal+DataTypeOffset+4]))
	assert.Equal(t, uint32(1), binary.LittleEndian.Uint32(buffer[firstTotal+NumOffset:firstTotal+NumOffset+4]))
	assert.Equal(t, byte(1), buffer[firstTotal+HaveLengthOffset])
	assert.Equal(t, uint32(3), binary.LittleEndian.Uint32(buffer[firstTotal+HaveLengthOffset+1:firstTotal+HaveLengthOffset+1+4]))
	assert.Equal(t, uint32(3), binary.LittleEndian.Uint32(buffer[firstTotal+HaveLengthOffset+1+4:firstTotal+HaveLengthOffset+1+8]))
	assert.Equal(t, []byte{0x01, 0x02, 0x03}, buffer[firstTotal+HaveLengthOffset+1+8:firstTotal+HaveLengthOffset+1+8+3])

	secondTotal := int(binary.LittleEndian.Uint32(buffer[firstTotal+TotalLengthOffset : firstTotal+TotalLengthOffset+4]))
	assert.Equal(t, firstTotal+secondTotal, end)
}

func TestWriteBindColErrorBranches(t *testing.T) {
	buffer := make([]byte, 256)

	_, err := writeBindCol([]*Stmt2AllField{
		{
			Name:      "c1",
			FieldType: common.TSDB_DATA_TYPE_INT,
		},
	}, [][]driver.Value{}, buffer, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "col count not match")

	_, err = writeBindCol([]*Stmt2AllField{
		{
			Name:      "c1",
			FieldType: common.TSDB_DATA_TYPE_INT,
		},
		{
			Name:      "c2",
			FieldType: common.TSDB_DATA_TYPE_INT,
		},
	}, [][]driver.Value{
		{int32(1)},
		{},
	}, buffer, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "col row count not match")

	_, err = writeBindCol([]*Stmt2AllField{
		{
			Name:      "decimal",
			FieldType: common.TSDB_DATA_TYPE_DECIMAL,
		},
	}, [][]driver.Value{
		{int32(1)},
	}, buffer, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "col field type not support")
}

func TestWriteBindColDecimalAndBlob(t *testing.T) {
	buffer := make([]byte, 512)
	fields := []*Stmt2AllField{
		{
			Name:      "col_decimal",
			FieldType: common.TSDB_DATA_TYPE_DECIMAL,
		},
		{
			Name:      "col_blob",
			FieldType: common.TSDB_DATA_TYPE_BLOB,
		},
	}
	values := [][]driver.Value{
		{"1.2300", nil, []byte("9.9900")},
		{[]byte{0x11}, "blob_text", nil},
	}
	end, err := writeBindCol(fields, values, buffer, 0)
	assert.NoError(t, err)
	if !assert.Greater(t, end, 0) {
		return
	}

	rows := 3
	haveLengthOffset := IsNullOffset + rows
	variableLengthOffset := haveLengthOffset + 1
	variableBufferLengthOffset := variableLengthOffset + (4 * rows)
	variableBufferOffset := variableBufferLengthOffset + 4

	assert.Equal(t, uint32(common.TSDB_DATA_TYPE_DECIMAL), binary.LittleEndian.Uint32(buffer[DataTypeOffset:DataTypeOffset+4]))
	assert.Equal(t, uint32(rows), binary.LittleEndian.Uint32(buffer[NumOffset:NumOffset+4]))
	assert.Equal(t, byte(0), buffer[IsNullOffset])
	assert.Equal(t, byte(1), buffer[IsNullOffset+1])
	assert.Equal(t, byte(0), buffer[IsNullOffset+2])
	assert.Equal(t, byte(1), buffer[haveLengthOffset])
	assert.Equal(t, uint32(6), binary.LittleEndian.Uint32(buffer[variableLengthOffset:variableLengthOffset+4]))
	assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(buffer[variableLengthOffset+4:variableLengthOffset+8]))
	assert.Equal(t, uint32(6), binary.LittleEndian.Uint32(buffer[variableLengthOffset+8:variableLengthOffset+12]))
	assert.Equal(t, uint32(12), binary.LittleEndian.Uint32(buffer[variableBufferLengthOffset:variableBufferLengthOffset+4]))
	assert.Equal(t, []byte("1.23009.9900"), buffer[variableBufferOffset:variableBufferOffset+12])

	firstTotal := int(binary.LittleEndian.Uint32(buffer[TotalLengthOffset : TotalLengthOffset+4]))
	assert.Equal(t, uint32(common.TSDB_DATA_TYPE_BLOB), binary.LittleEndian.Uint32(buffer[firstTotal+DataTypeOffset:firstTotal+DataTypeOffset+4]))
	assert.Equal(t, uint32(rows), binary.LittleEndian.Uint32(buffer[firstTotal+NumOffset:firstTotal+NumOffset+4]))
	assert.Equal(t, byte(0), buffer[firstTotal+IsNullOffset])
	assert.Equal(t, byte(0), buffer[firstTotal+IsNullOffset+1])
	assert.Equal(t, byte(1), buffer[firstTotal+IsNullOffset+2])
	assert.Equal(t, byte(1), buffer[firstTotal+haveLengthOffset])
	assert.Equal(t, uint32(1), binary.LittleEndian.Uint32(buffer[firstTotal+variableLengthOffset:firstTotal+variableLengthOffset+4]))
	assert.Equal(t, uint32(9), binary.LittleEndian.Uint32(buffer[firstTotal+variableLengthOffset+4:firstTotal+variableLengthOffset+8]))
	assert.Equal(t, uint32(0), binary.LittleEndian.Uint32(buffer[firstTotal+variableLengthOffset+8:firstTotal+variableLengthOffset+12]))
	assert.Equal(t, uint32(10), binary.LittleEndian.Uint32(buffer[firstTotal+variableBufferLengthOffset:firstTotal+variableBufferLengthOffset+4]))
	assert.Equal(t, []byte{0x11}, buffer[firstTotal+variableBufferOffset:firstTotal+variableBufferOffset+1])
	assert.Equal(t, []byte("blob_text"), buffer[firstTotal+variableBufferOffset+1:firstTotal+variableBufferOffset+10])

	secondTotal := int(binary.LittleEndian.Uint32(buffer[firstTotal+TotalLengthOffset : firstTotal+TotalLengthOffset+4]))
	assert.Equal(t, firstTotal+secondTotal, end)
}

func TestWriteBindColFixedTypeMismatchBranches(t *testing.T) {
	tests := []struct {
		name      string
		fieldType int8
		value     driver.Value
		errMsg    string
	}{
		{name: "bool", fieldType: common.TSDB_DATA_TYPE_BOOL, value: "x", errMsg: "expect bool"},
		{name: "tinyint", fieldType: common.TSDB_DATA_TYPE_TINYINT, value: "x", errMsg: "expect int8"},
		{name: "smallint", fieldType: common.TSDB_DATA_TYPE_SMALLINT, value: "x", errMsg: "expect int16"},
		{name: "int", fieldType: common.TSDB_DATA_TYPE_INT, value: "x", errMsg: "expect int32"},
		{name: "bigint", fieldType: common.TSDB_DATA_TYPE_BIGINT, value: "x", errMsg: "expect int64"},
		{name: "float", fieldType: common.TSDB_DATA_TYPE_FLOAT, value: "x", errMsg: "expect float32"},
		{name: "double", fieldType: common.TSDB_DATA_TYPE_DOUBLE, value: "x", errMsg: "expect float64"},
		{name: "timestamp", fieldType: common.TSDB_DATA_TYPE_TIMESTAMP, value: "x", errMsg: "expect int64 or time.Time"},
		{name: "utinyint", fieldType: common.TSDB_DATA_TYPE_UTINYINT, value: "x", errMsg: "expect uint8"},
		{name: "usmallint", fieldType: common.TSDB_DATA_TYPE_USMALLINT, value: "x", errMsg: "expect uint16"},
		{name: "uint", fieldType: common.TSDB_DATA_TYPE_UINT, value: "x", errMsg: "expect uint32"},
		{name: "ubigint", fieldType: common.TSDB_DATA_TYPE_UBIGINT, value: "x", errMsg: "expect uint64"},
	}

	for i := 0; i < len(tests); i++ {
		tc := tests[i]
		t.Run(tc.name, func(t *testing.T) {
			_, err := writeBindCol([]*Stmt2AllField{
				{
					Name:      tc.name,
					FieldType: tc.fieldType,
				},
			}, [][]driver.Value{
				{tc.value},
			}, make([]byte, 256), 0)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.errMsg)
		})
	}
}

func TestWriteBindColTimestampInt64Branch(t *testing.T) {
	offset, err := writeBindCol([]*Stmt2AllField{
		{
			Name:      "ts",
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			Precision: common.PrecisionMilliSecond,
		},
	}, [][]driver.Value{
		{int64(1711111111000)},
	}, make([]byte, 256), 0)
	assert.NoError(t, err)
	assert.Greater(t, offset, 0)
}

func BenchmarkMarshalBinary(b *testing.B) {
	bindData := make([]*TaosStmt2BindData, 1000)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 1000; i++ {
		bindData[i] = &TaosStmt2BindData{
			TableName: fmt.Sprintf("d_%d", i),
			Cols: [][]driver.Value{
				{
					now,
				},
				{
					float32(i),
				},
				{
					int32(i),
				},
				{
					float32(i),
				},
			},
		}
	}
	fields := []*Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			BindType:  TAOS_FIELD_TBNAME,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			BindType:  TAOS_FIELD_COL,
			Precision: common.PrecisionMilliSecond,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			BindType:  TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			BindType:  TAOS_FIELD_COL,
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := MarshalStmt2Binary(bindData, true, fields); err != nil {
			b.Fatalf("MarshalStmt2Binary failed: %v", err)
		}
	}
}

func BenchmarkMarshalBinaryLegacy(b *testing.B) {
	bindData := make([]*TaosStmt2BindData, 1000)
	now := time.Now().UnixNano() / int64(time.Millisecond)
	for i := 0; i < 1000; i++ {
		bindData[i] = &TaosStmt2BindData{
			TableName: fmt.Sprintf("d_%d", i),
			Cols: [][]driver.Value{
				{
					now,
				},
				{
					float32(i),
				},
				{
					int32(i),
				},
				{
					float32(i),
				},
			},
		}
	}
	fields := []*Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_BINARY,
			BindType:  TAOS_FIELD_TBNAME,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_TIMESTAMP,
			BindType:  TAOS_FIELD_COL,
			Precision: common.PrecisionMilliSecond,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			BindType:  TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_INT,
			BindType:  TAOS_FIELD_COL,
		},
		{
			FieldType: common.TSDB_DATA_TYPE_FLOAT,
			BindType:  TAOS_FIELD_COL,
		},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshalStmt2BinaryLegacy(bindData, true, fields); err != nil {
			b.Fatalf("marshalStmt2BinaryLegacy failed: %v", err)
		}
	}
}
