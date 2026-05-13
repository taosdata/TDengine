package common

import (
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_parseDecimalType(t *testing.T) {
	type args struct {
		typeStr string
	}
	tests := []struct {
		name          string
		args          args
		wantPrecision int64
		wantScale     int64
		wantErr       assert.ErrorAssertionFunc
	}{
		{
			name: "len 0",
			args: args{
				typeStr: "",
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong precision format",
			args: args{
				typeStr: "DECIMAL(xx,1)",
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong scale format",
			args: args{
				typeStr: "DECIMAL(1,xx)",
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong type",
			args: args{
				typeStr: "DECIMAL(111)",
			},
			wantErr: assert.Error,
		},
		{
			name: "normal",
			args: args{
				typeStr: "DECIMAL(1,1)",
			},
			wantPrecision: 1,
			wantScale:     1,
			wantErr:       assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := parseDecimalType(tt.args.typeStr)
			if !tt.wantErr(t, err, fmt.Sprintf("parseDecimalType(%v)", tt.args.typeStr)) {
				return
			}
			assert.Equalf(t, tt.wantPrecision, got, "parseDecimalType(%v)", tt.args.typeStr)
			assert.Equalf(t, tt.wantScale, got1, "parseDecimalType(%v)", tt.args.typeStr)
		})
	}
}

func Test_hexCharToDigit(t *testing.T) {
	type args struct {
		char byte
	}
	tests := []struct {
		name string
		args args
		want uint8
	}{
		{
			name: "0",
			args: args{
				char: '0',
			},
			want: 0,
		},
		{
			name: "1",
			args: args{
				char: '1',
			},
			want: 1,
		},
		{
			name: "a",
			args: args{
				char: 'a',
			},
			want: 10,
		},
		{
			name: "f",
			args: args{
				char: 'f',
			},
			want: 15,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, hexCharToDigit(tt.args.char), "hexCharToDigit(%v)", tt.args.char)
		})
	}
}

func TestUnmarshalRestfulBody(t *testing.T) {
	ts, err := time.Parse(time.RFC3339Nano, "2025-03-20T09:11:11.634Z")
	assert.NoError(t, err)
	ts2, err := time.Parse(time.RFC3339Nano, "2025-03-20T09:11:12.634Z")
	assert.NoError(t, err)
	type args struct {
		body       io.Reader
		bufferSize int
	}
	tests := []struct {
		name    string
		args    args
		want    *TDEngineRestfulResp
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "all type",
			args: args{
				body:       strings.NewReader(`{"code":0,"column_meta":[["ts","TIMESTAMP",8],["c1","BOOL",1],["c2","TINYINT",1],["c3","SMALLINT",2],["c4","INT",4],["c5","BIGINT",8],["c6","TINYINT UNSIGNED",1],["c7","SMALLINT UNSIGNED",2],["c8","INT UNSIGNED",4],["c9","BIGINT UNSIGNED",8],["c10","FLOAT",4],["c11","DOUBLE",8],["c12","VARCHAR",20],["c13","NCHAR",20],["c14","VARBINARY",20],["c15","GEOMETRY",100],["c16","DECIMAL(20,4)",16],["c17","DECIMAL(10,4)",8],["c18","BLOB",4194304],["info","JSON",4095]],"data":[["2025-03-20T09:11:11.634Z",true,1,1,1,1,1,1,1,1,1,1,"test_binary","test_nchar","76617262696e617279","010100000000000000000059400000000000005940","-123.4000","1234.5600","626c6f62",{"a":1}],["2025-03-20T09:11:12.634Z",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,{"a":1}]],"rows":2}`),
				bufferSize: 1024,
			},
			want: &TDEngineRestfulResp{
				Code: 0,
				Rows: 2,
				Desc: "",
				ColNames: []string{
					"ts",
					"c1",
					"c2",
					"c3",
					"c4",
					"c5",
					"c6",
					"c7",
					"c8",
					"c9",
					"c10",
					"c11",
					"c12",
					"c13",
					"c14",
					"c15",
					"c16",
					"c17",
					"c18",
					"info",
				},
				ColTypes: []int{
					TSDB_DATA_TYPE_TIMESTAMP,
					TSDB_DATA_TYPE_BOOL,
					TSDB_DATA_TYPE_TINYINT,
					TSDB_DATA_TYPE_SMALLINT,
					TSDB_DATA_TYPE_INT,
					TSDB_DATA_TYPE_BIGINT,
					TSDB_DATA_TYPE_UTINYINT,
					TSDB_DATA_TYPE_USMALLINT,
					TSDB_DATA_TYPE_UINT,
					TSDB_DATA_TYPE_UBIGINT,
					TSDB_DATA_TYPE_FLOAT,
					TSDB_DATA_TYPE_DOUBLE,
					TSDB_DATA_TYPE_BINARY,
					TSDB_DATA_TYPE_NCHAR,
					TSDB_DATA_TYPE_VARBINARY,
					TSDB_DATA_TYPE_GEOMETRY,
					TSDB_DATA_TYPE_DECIMAL,
					TSDB_DATA_TYPE_DECIMAL64,
					TSDB_DATA_TYPE_BLOB,
					TSDB_DATA_TYPE_JSON,
				},
				ColLength: []int64{
					8,
					1,
					1,
					2,
					4,
					8,
					1,
					2,
					4,
					8,
					4,
					8,
					20,
					20,
					20,
					100,
					16,
					8,
					4194304,
					4095,
				},
				Precisions: []int64{
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					20,
					10,
					0,
					0,
				},
				Scales: []int64{
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					4,
					4,
					0,
					0,
				},
				Data: [][]driver.Value{
					{
						ts,
						true,
						int8(1),
						int16(1),
						int32(1),
						int64(1),
						uint8(1),
						uint16(1),
						uint32(1),
						uint64(1),
						float32(1),
						float64(1),
						"test_binary",
						"test_nchar",
						[]byte("varbinary"),
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
						"-123.4000",
						"1234.5600",
						[]byte("blob"),
						[]byte(`{"a":1}`),
					},
					{
						ts2,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						[]byte(`{"a":1}`),
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "message",
			args: args{
				body:       strings.NewReader(`{"code":65535,"desc":"wrong"}`),
				bufferSize: 1024,
			},
			want: &TDEngineRestfulResp{
				Code: 65535,
				Desc: "wrong",
			},
			wantErr: assert.NoError,
		},
		{
			name: "wrong decimal",
			args: args{
				body:       strings.NewReader(`{"code":0,"column_meta":[["c16","DECIMAL(xx,4)",16]],"data":[["-123.4000"],[null]],"rows":2}`),
				bufferSize: 1024,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong decimal length",
			args: args{
				body:       strings.NewReader(`{"code":0,"column_meta":[["c16","DECIMAL(10,4)",999]],"data":[["-123.4000"],[null]],"rows":2}`),
				bufferSize: 1024,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong type",
			args: args{
				body:       strings.NewReader(`{"code":0,"column_meta":[["c16","xxx",16]],"data":[["-123.4000"],[null]],"rows":2}`),
				bufferSize: 1024,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong timestamp",
			args: args{
				body:       strings.NewReader(`{"code":0,"column_meta":[["ts","TIMESTAMP",8]],"data":[["xxxx"],[null]],"rows":2}`),
				bufferSize: 1024,
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnmarshalRestfulBody(tt.args.body, tt.args.bufferSize)
			if !tt.wantErr(t, err, fmt.Sprintf("UnmarshalRestfulBody(%v, %v)", tt.args.body, tt.args.bufferSize)) {
				return
			}
			assert.Equalf(t, tt.want, got, "UnmarshalRestfulBody(%v, %v)", tt.args.body, tt.args.bufferSize)
		})
	}
}

func TestUnmarshalRestfulBodyWithTimezone(t *testing.T) {
	parisTimezone, err := time.LoadLocation("Europe/Paris")
	require.NoError(t, err)
	ts, err := time.Parse(time.RFC3339Nano, "2025-03-20T09:11:11.634Z")
	assert.NoError(t, err)
	ts2, err := time.Parse(time.RFC3339Nano, "2025-03-20T09:11:12.634Z")
	assert.NoError(t, err)
	ts = ts.In(parisTimezone)
	ts2 = ts2.In(parisTimezone)
	type args struct {
		body       io.Reader
		bufferSize int
		loc        *time.Location
	}
	tests := []struct {
		name    string
		args    args
		want    *TDEngineRestfulResp
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "all type",
			args: args{
				body:       strings.NewReader(`{"code":0,"column_meta":[["ts","TIMESTAMP",8],["c1","BOOL",1],["c2","TINYINT",1],["c3","SMALLINT",2],["c4","INT",4],["c5","BIGINT",8],["c6","TINYINT UNSIGNED",1],["c7","SMALLINT UNSIGNED",2],["c8","INT UNSIGNED",4],["c9","BIGINT UNSIGNED",8],["c10","FLOAT",4],["c11","DOUBLE",8],["c12","VARCHAR",20],["c13","NCHAR",20],["c14","VARBINARY",20],["c15","GEOMETRY",100],["c16","DECIMAL(20,4)",16],["c17","DECIMAL(10,4)",8],["c18","BLOB",4194304],["info","JSON",4095]],"data":[["2025-03-20T10:11:11.634+01:00",true,1,1,1,1,1,1,1,1,1,1,"test_binary","test_nchar","76617262696e617279","010100000000000000000059400000000000005940","-123.4000","1234.5600","626c6f62",{"a":1}],["2025-03-20T10:11:12.634+01:00",null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,{"a":1}]],"rows":2}`),
				bufferSize: 1024,
				loc:        parisTimezone,
			},
			want: &TDEngineRestfulResp{
				Code: 0,
				Rows: 2,
				Desc: "",
				ColNames: []string{
					"ts",
					"c1",
					"c2",
					"c3",
					"c4",
					"c5",
					"c6",
					"c7",
					"c8",
					"c9",
					"c10",
					"c11",
					"c12",
					"c13",
					"c14",
					"c15",
					"c16",
					"c17",
					"c18",
					"info",
				},
				ColTypes: []int{
					TSDB_DATA_TYPE_TIMESTAMP,
					TSDB_DATA_TYPE_BOOL,
					TSDB_DATA_TYPE_TINYINT,
					TSDB_DATA_TYPE_SMALLINT,
					TSDB_DATA_TYPE_INT,
					TSDB_DATA_TYPE_BIGINT,
					TSDB_DATA_TYPE_UTINYINT,
					TSDB_DATA_TYPE_USMALLINT,
					TSDB_DATA_TYPE_UINT,
					TSDB_DATA_TYPE_UBIGINT,
					TSDB_DATA_TYPE_FLOAT,
					TSDB_DATA_TYPE_DOUBLE,
					TSDB_DATA_TYPE_BINARY,
					TSDB_DATA_TYPE_NCHAR,
					TSDB_DATA_TYPE_VARBINARY,
					TSDB_DATA_TYPE_GEOMETRY,
					TSDB_DATA_TYPE_DECIMAL,
					TSDB_DATA_TYPE_DECIMAL64,
					TSDB_DATA_TYPE_BLOB,
					TSDB_DATA_TYPE_JSON,
				},
				ColLength: []int64{
					8,
					1,
					1,
					2,
					4,
					8,
					1,
					2,
					4,
					8,
					4,
					8,
					20,
					20,
					20,
					100,
					16,
					8,
					4194304,
					4095,
				},
				Precisions: []int64{
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					20,
					10,
					0,
					0,
				},
				Scales: []int64{
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					0,
					4,
					4,
					0,
					0,
				},
				Data: [][]driver.Value{
					{
						ts,
						true,
						int8(1),
						int16(1),
						int32(1),
						int64(1),
						uint8(1),
						uint16(1),
						uint32(1),
						uint64(1),
						float32(1),
						float64(1),
						"test_binary",
						"test_nchar",
						[]byte("varbinary"),
						[]byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40},
						"-123.4000",
						"1234.5600",
						[]byte("blob"),
						[]byte(`{"a":1}`),
					},
					{
						ts2,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						nil,
						[]byte(`{"a":1}`),
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "wrong timestamp",
			args: args{
				body:       strings.NewReader(`{"code":0,"column_meta":[["ts","TIMESTAMP",8]],"data":[["xxxx"],[null]],"rows":2}`),
				bufferSize: 1024,
			},
			wantErr: assert.Error,
		},
		{
			name: "wrong varbinary",
			args: args{
				body:       strings.NewReader(`{"code":0,"column_meta":[["c1","VARBINARY",8]],"data":[["02a"],[null]],"rows":2}`),
				bufferSize: 1024,
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := UnmarshalRestfulBodyWithTimezone(tt.args.body, tt.args.bufferSize, tt.args.loc)
			if !tt.wantErr(t, err, fmt.Sprintf("UnmarshalRestfulBodyWithTimezone(%v, %v, %v)", tt.args.body, tt.args.bufferSize, tt.args.loc)) {
				return
			}
			assert.Equalf(t, tt.want, got, "UnmarshalRestfulBodyWithTimezone(%v, %v, %v)", tt.args.body, tt.args.bufferSize, tt.args.loc)
		})
	}
}
