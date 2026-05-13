package inputjson

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/taosdata/taosadapter/v3/db/commonpool"
	"github.com/taosdata/taosadapter/v3/log"
	"github.com/taosdata/taosadapter/v3/tools/connectpool"
)

var testStartTime = time.Date(2025, 11, 3, 10, 0, 0, 0, time.UTC)
var testRecords = []*record{
	{stb: "`db2`.`stb1`", subTable: "B", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(time.Hour), values: "'B','2025-11-03T11:00:00Z',11,21"},
	{stb: "`db2`.`stb1`", subTable: "B", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(-time.Hour), values: "'B','2025-11-03T9:00:00Z',9,19"},
	{stb: "`db2`.`stb1`", subTable: "B", keys: "`ts`,`v1`,`v2`", ts: testStartTime, values: "'B','2025-11-03T10:00:00Z',10,20"},
	{stb: "`db2`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(time.Hour), values: "'A','2025-11-03T11:00:00Z',11,21"},
	{stb: "`db2`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(-time.Hour), values: "'A','2025-11-03T9:00:00Z',9,19"},
	{stb: "`db2`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime, values: "'A','2025-11-03T10:00:00Z',10,20"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(time.Hour), values: "'C','2025-11-03T11:00:00Z',11,21"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`", ts: testStartTime.Add(time.Hour), values: "'C','2025-11-03T11:00:00Z',11"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(-time.Hour), values: "'C','2025-11-03T9:00:00Z',9,19"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`", ts: testStartTime.Add(-time.Hour), values: "'C','2025-11-03T9:00:00Z',9"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`,`v2`", ts: testStartTime, values: "'C','2025-11-03T10:00:00Z',10,20"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`", ts: testStartTime, values: "'C','2025-11-03T10:00:00Z',10"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(time.Hour), values: "'A','2025-11-03T10:00:00Z',11,21"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`", ts: testStartTime.Add(time.Hour), values: "'A','2025-11-03T10:00:00Z',11"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(-time.Hour), values: "'A','2025-11-03T09:00:00Z',9,19"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`", ts: testStartTime.Add(-time.Hour), values: "'A','2025-11-03T09:00:00Z',9"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime, values: "'A','2025-11-03T10:00:00Z',10,20"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`", ts: testStartTime, values: "'A','2025-11-03T10:00:00Z',10"},
	{stb: "`db1`.`stb2`", subTable: "B", keys: "`ts`,`v1`,`v2`,`v3`", ts: testStartTime.Add(time.Hour), values: "'B','2025-11-03T11:00:00Z',11,21,31"},
	{stb: "`db1`.`stb2`", subTable: "C", keys: "`ts`,`v1`,`v3`", ts: testStartTime.Add(time.Hour), values: "'C','2025-11-03T11:00:00Z',11,31"},
	{stb: "`db1`.`stb2`", subTable: "D", keys: "`ts`,`v1`,`v2`,`v4`", ts: testStartTime.Add(-time.Hour), values: "'D','2025-11-03T09:00:00Z',9,19,41"},
	{stb: "`db1`.`stb2`", subTable: "E", keys: "`ts`,`v1`,`v5`", ts: testStartTime.Add(-time.Hour), values: "'E','2025-11-03T09:00:00Z',9,51"},
	{stb: "`db1`.`stb2`", subTable: "D", keys: "`ts`,`v1`,`v3`", ts: testStartTime, values: "'D','2025-11-03T10:00:00Z',10,31"},
	{stb: "`db1`.`stb2`", subTable: "B", keys: "`ts`,`v1`", ts: testStartTime, values: "'B','2025-11-03T10:00:00Z',10"},
}
var testRecordsExpectedOrder = []*record{
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`", ts: testStartTime.Add(-time.Hour), values: "'A','2025-11-03T09:00:00Z',9"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`", ts: testStartTime, values: "'A','2025-11-03T10:00:00Z',10"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`", ts: testStartTime.Add(time.Hour), values: "'A','2025-11-03T10:00:00Z',11"},

	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(-time.Hour), values: "'A','2025-11-03T09:00:00Z',9,19"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime, values: "'A','2025-11-03T10:00:00Z',10,20"},
	{stb: "`db1`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(time.Hour), values: "'A','2025-11-03T10:00:00Z',11,21"},

	{stb: "`db1`.`stb2`", subTable: "B", keys: "`ts`,`v1`", ts: testStartTime, values: "'B','2025-11-03T10:00:00Z',10"},

	{stb: "`db1`.`stb2`", subTable: "B", keys: "`ts`,`v1`,`v2`,`v3`", ts: testStartTime.Add(time.Hour), values: "'B','2025-11-03T11:00:00Z',11,21,31"},

	{stb: "`db1`.`stb2`", subTable: "C", keys: "`ts`,`v1`,`v3`", ts: testStartTime.Add(time.Hour), values: "'C','2025-11-03T11:00:00Z',11,31"},

	{stb: "`db1`.`stb2`", subTable: "D", keys: "`ts`,`v1`,`v2`,`v4`", ts: testStartTime.Add(-time.Hour), values: "'D','2025-11-03T09:00:00Z',9,19,41"},

	{stb: "`db1`.`stb2`", subTable: "D", keys: "`ts`,`v1`,`v3`", ts: testStartTime, values: "'D','2025-11-03T10:00:00Z',10,31"},

	{stb: "`db1`.`stb2`", subTable: "E", keys: "`ts`,`v1`,`v5`", ts: testStartTime.Add(-time.Hour), values: "'E','2025-11-03T09:00:00Z',9,51"},

	{stb: "`db2`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(-time.Hour), values: "'A','2025-11-03T9:00:00Z',9,19"},
	{stb: "`db2`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime, values: "'A','2025-11-03T10:00:00Z',10,20"},
	{stb: "`db2`.`stb1`", subTable: "A", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(time.Hour), values: "'A','2025-11-03T11:00:00Z',11,21"},

	{stb: "`db2`.`stb1`", subTable: "B", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(-time.Hour), values: "'B','2025-11-03T9:00:00Z',9,19"},
	{stb: "`db2`.`stb1`", subTable: "B", keys: "`ts`,`v1`,`v2`", ts: testStartTime, values: "'B','2025-11-03T10:00:00Z',10,20"},
	{stb: "`db2`.`stb1`", subTable: "B", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(time.Hour), values: "'B','2025-11-03T11:00:00Z',11,21"},

	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`", ts: testStartTime.Add(-time.Hour), values: "'C','2025-11-03T9:00:00Z',9"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`", ts: testStartTime, values: "'C','2025-11-03T10:00:00Z',10"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`", ts: testStartTime.Add(time.Hour), values: "'C','2025-11-03T11:00:00Z',11"},

	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(-time.Hour), values: "'C','2025-11-03T9:00:00Z',9,19"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`,`v2`", ts: testStartTime, values: "'C','2025-11-03T10:00:00Z',10,20"},
	{stb: "`db2`.`stb1`", subTable: "C", keys: "`ts`,`v1`,`v2`", ts: testStartTime.Add(time.Hour), values: "'C','2025-11-03T11:00:00Z',11,21"},
}

func Test_escapeIdentifier(t *testing.T) {
	type args struct {
		identifier string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "no special chars",
			args: args{identifier: "simpleIdentifier"},
			want: "simpleIdentifier",
		},
		{
			name: "with backtick",
			args: args{identifier: "identifier`with`backticks"},
			want: "identifier``with``backticks",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, escapeIdentifier(tt.args.identifier), "escapeIdentifier(%v)", tt.args.identifier)
		})
	}
}

func Test_writeSuperTableStatement(t *testing.T) {
	type args struct {
		builder    io.StringWriter
		superTable string
		keys       string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "basic case",
			args: args{
				builder:    &strings.Builder{},
				superTable: "`db`.`mySuperTable`",
				keys:       "v1,v2",
			},
			want: "`db`.`mySuperTable`(`tbname`,v1,v2)values",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeSuperTableStatement(tt.args.builder, tt.args.superTable, tt.args.keys)
			assert.Equalf(
				t,
				tt.want,
				tt.args.builder.(*strings.Builder).String(),
				"writeSuperTableStatement(%v, %v, %v)",
				tt.args.builder,
				tt.args.superTable,
				tt.args.keys,
			)
		})
	}
}

func Test_castToString(t *testing.T) {
	type args struct {
		value interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "string input",
			args:    args{value: "test string"},
			want:    "test string",
			wantErr: assert.NoError,
		},
		{
			name:    "int input",
			args:    args{value: json.Number("123")},
			want:    "123",
			wantErr: assert.NoError,
		},
		{
			name:    "float input",
			args:    args{value: json.Number("45.67")},
			want:    "45.67",
			wantErr: assert.NoError,
		},
		{
			name:    "bool input true",
			args:    args{value: true},
			want:    "true",
			wantErr: assert.NoError,
		},
		{
			name:    "bool input false",
			args:    args{value: false},
			want:    "false",
			wantErr: assert.NoError,
		},
		{
			name:    "unsupported type input",
			args:    args{value: []int{1, 2, 3}},
			want:    "",
			wantErr: assert.Error,
		},
		{
			name:    "nil input",
			args:    args{value: nil},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := castToString(tt.args.value)
			if !tt.wantErr(t, err, fmt.Sprintf("castToString(%v)", tt.args.value)) {
				return
			}
			assert.Equalf(t, tt.want, got, "castToString(%v)", tt.args.value)
		})
	}
}

func Test_writeValue(t *testing.T) {
	type args struct {
		builder *strings.Builder
		value   interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
		want    string
	}{
		{
			name: "string value",
			args: args{
				builder: &strings.Builder{},
				value:   "hello",
			},
			want:    "'hello'",
			wantErr: assert.NoError,
		},
		{
			name: "int value",
			args: args{
				builder: &strings.Builder{},
				value:   json.Number("42"),
			},
			want:    "42",
			wantErr: assert.NoError,
		},
		{
			name: "float value",
			args: args{
				builder: &strings.Builder{},
				value:   json.Number("3.14"),
			},
			want:    "3.14",
			wantErr: assert.NoError,
		},
		{
			name: "bool value true",
			args: args{
				builder: &strings.Builder{},
				value:   true,
			},
			want:    "true",
			wantErr: assert.NoError,
		},
		{
			name: "bool value false",
			args: args{
				builder: &strings.Builder{},
				value:   false,
			},
			want:    "false",
			wantErr: assert.NoError,
		},
		{
			name: "nil value",
			args: args{
				builder: &strings.Builder{},
				value:   nil,
			},
			want:    "null",
			wantErr: assert.NoError,
		},
		{
			name: "unsupported type value",
			args: args{
				builder: &strings.Builder{},
				value:   []int{1, 2, 3},
			},
			want:    "",
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := writeValue(tt.args.builder, tt.args.value)
			if !tt.wantErr(t, err, fmt.Sprintf("writeValue(%v, %v)", tt.args.builder, tt.args.value)) {
				return
			}
			assert.Equalf(t, tt.want, tt.args.builder.String(), "writeValue(%v, %v)", tt.args.builder, tt.args.value)
		})
	}
}

func Test_generateFieldKeyStatement(t *testing.T) {
	type args struct {
		builder *strings.Builder
		rule    *ParsedRule
		count   int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "basic case",
			args: args{
				builder: &strings.Builder{},
				rule: &ParsedRule{
					TimeFieldName: "_ts",
					FieldKeys:     []string{"field1", "field2", "field3"},
				},
				count: 3,
			},
			want: "`_ts`,`field1`,`field2`,`field3`",
		},
		{
			name: "not all keys",
			args: args{
				builder: &strings.Builder{},
				rule: &ParsedRule{
					TimeFieldName: "_time",
					FieldKeys:     []string{"f1", "f2", "f3", "f4"},
				},
				count: 2,
			},
			want: "`_time`,`f1`,`f2`",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			generateFieldKeyStatement(tt.args.builder, tt.args.rule, tt.args.count)
		})
	}
}

func Test_generateSql(t *testing.T) {
	type args struct {
		records      []*record
		maxSqlLength int
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "basic case",
			args: args{
				records: []*record{
					{
						stb:      "`test`.`stb1`",
						subTable: "ABC",
						ts:       time.Date(2025, 11, 3, 10, 0, 0, 0, time.UTC),
						keys:     "`ts`,`v`,`tg`",
						values:   "'ABC','2025-11-03T10:00:00Z',1,'tag1'",
					},
				},
				maxSqlLength: MAXSQLLength,
			},
			want:    []string{"insert into `test`.`stb1`(`tbname`,`ts`,`v`,`tg`)values('ABC','2025-11-03T10:00:00Z',1,'tag1')"},
			wantErr: assert.NoError,
		},
		{
			name: "multiple records within max length",
			args: args{
				records:      testRecords,
				maxSqlLength: MAXSQLLength,
			},
			want: []string{
				"insert into `db1`.`stb1`(`tbname`,`ts`,`v1`)values('A','2025-11-03T09:00:00Z',9)('A','2025-11-03T10:00:00Z',10)('A','2025-11-03T10:00:00Z',11)" +
					"`db1`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('A','2025-11-03T09:00:00Z',9,19)('A','2025-11-03T10:00:00Z',10,20)('A','2025-11-03T10:00:00Z',11,21)" +
					"`db1`.`stb2`(`tbname`,`ts`,`v1`)values('B','2025-11-03T10:00:00Z',10)" +
					"`db1`.`stb2`(`tbname`,`ts`,`v1`,`v2`,`v3`)values('B','2025-11-03T11:00:00Z',11,21,31)" +
					"`db1`.`stb2`(`tbname`,`ts`,`v1`,`v3`)values('C','2025-11-03T11:00:00Z',11,31)" +
					"`db1`.`stb2`(`tbname`,`ts`,`v1`,`v2`,`v4`)values('D','2025-11-03T09:00:00Z',9,19,41)" +
					"`db1`.`stb2`(`tbname`,`ts`,`v1`,`v3`)values('D','2025-11-03T10:00:00Z',10,31)" +
					"`db1`.`stb2`(`tbname`,`ts`,`v1`,`v5`)values('E','2025-11-03T09:00:00Z',9,51)" +
					"`db2`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('A','2025-11-03T9:00:00Z',9,19)('A','2025-11-03T10:00:00Z',10,20)('A','2025-11-03T11:00:00Z',11,21)('B','2025-11-03T9:00:00Z',9,19)('B','2025-11-03T10:00:00Z',10,20)('B','2025-11-03T11:00:00Z',11,21)" +
					"`db2`.`stb1`(`tbname`,`ts`,`v1`)values('C','2025-11-03T9:00:00Z',9)('C','2025-11-03T10:00:00Z',10)('C','2025-11-03T11:00:00Z',11)" +
					"`db2`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('C','2025-11-03T9:00:00Z',9,19)('C','2025-11-03T10:00:00Z',10,20)('C','2025-11-03T11:00:00Z',11,21)",
			},
			wantErr: assert.NoError,
		},
		{
			name: "multiple records cutting by max length",
			args: args{
				records:      testRecords,
				maxSqlLength: 150,
			},
			want: []string{
				"insert into `db1`.`stb1`(`tbname`,`ts`,`v1`)values('A','2025-11-03T09:00:00Z',9)('A','2025-11-03T10:00:00Z',10)('A','2025-11-03T10:00:00Z',11)",
				"insert into `db1`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('A','2025-11-03T09:00:00Z',9,19)('A','2025-11-03T10:00:00Z',10,20)",
				"insert into `db1`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('A','2025-11-03T10:00:00Z',11,21)",
				"insert into `db1`.`stb2`(`tbname`,`ts`,`v1`)values('B','2025-11-03T10:00:00Z',10)",
				"insert into `db1`.`stb2`(`tbname`,`ts`,`v1`,`v2`,`v3`)values('B','2025-11-03T11:00:00Z',11,21,31)",
				"insert into `db1`.`stb2`(`tbname`,`ts`,`v1`,`v3`)values('C','2025-11-03T11:00:00Z',11,31)",
				"insert into `db1`.`stb2`(`tbname`,`ts`,`v1`,`v2`,`v4`)values('D','2025-11-03T09:00:00Z',9,19,41)",
				"insert into `db1`.`stb2`(`tbname`,`ts`,`v1`,`v3`)values('D','2025-11-03T10:00:00Z',10,31)",
				"insert into `db1`.`stb2`(`tbname`,`ts`,`v1`,`v5`)values('E','2025-11-03T09:00:00Z',9,51)",
				"insert into `db2`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('A','2025-11-03T9:00:00Z',9,19)('A','2025-11-03T10:00:00Z',10,20)",
				"insert into `db2`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('A','2025-11-03T11:00:00Z',11,21)('B','2025-11-03T9:00:00Z',9,19)",
				"insert into `db2`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('B','2025-11-03T10:00:00Z',10,20)('B','2025-11-03T11:00:00Z',11,21)",
				"insert into `db2`.`stb1`(`tbname`,`ts`,`v1`)values('C','2025-11-03T9:00:00Z',9)('C','2025-11-03T10:00:00Z',10)('C','2025-11-03T11:00:00Z',11)",
				"insert into `db2`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('C','2025-11-03T9:00:00Z',9,19)('C','2025-11-03T10:00:00Z',10,20)",
				"insert into `db2`.`stb1`(`tbname`,`ts`,`v1`,`v2`)values('C','2025-11-03T11:00:00Z',11,21)",
			},
			wantErr: assert.NoError,
		},
		{
			name: "multiple records cutting by max length",
			args: args{
				records:      testRecords,
				maxSqlLength: 10,
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := generateSql(tt.args.records, tt.args.maxSqlLength)
			if !tt.wantErr(t, err, fmt.Sprintf("generateSql(%v, %v)", tt.args.records, tt.args.maxSqlLength)) {
				return
			}
			assert.Equalf(t, tt.want, got, "generateSql(%v, %v)", tt.args.records, tt.args.maxSqlLength)
		})
	}
}

func Test_sortRecords(t *testing.T) {
	type args struct {
		records []*record
	}
	tests := []struct {
		name string
		args args
		want []*record
	}{
		{
			name: "basic case",
			args: args{
				records: testRecords,
			},
			want: testRecordsExpectedOrder,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lastCP := tt.args.records
			for i := 0; i < 10000; i++ {
				cp := make([]*record, len(tt.args.records))
				copy(cp, tt.args.records)
				// shuffle records
				for {
					rand.Shuffle(len(tt.args.records), func(m, n int) {
						cp[m], cp[n] = cp[n], cp[m]
					})
					if !assert.ObjectsAreEqual(lastCP, cp) {
						break
					}
					lastCP = cp
				}
				sortRecords(cp)
				assert.Equal(t, tt.want, cp)
			}
		})
	}
}

func Test_writeEscapeStringValue(t *testing.T) {
	type args struct {
		builder *strings.Builder
		s       string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "basic case",
			args: args{
				builder: &strings.Builder{},
				s:       "simple string",
			},
			want: "simple string",
		},
		{
			name: "string with single quote",
			args: args{
				builder: &strings.Builder{},
				s:       "string with 'single' quotes",
			},
			want: "string with ''single'' quotes",
		},
		{
			name: "string with backslash",
			args: args{
				builder: &strings.Builder{},
				s:       "string with \\ backslash",
			},
			want: "string with \\\\ backslash",
		},
		{
			name: "string with both single quote and backslash",
			args: args{
				builder: &strings.Builder{},
				s:       "string with 'single' quotes and \\ backslash",
			},
			want: "string with ''single'' quotes and \\\\ backslash",
		},
		{
			name: "empty string",
			args: args{
				builder: &strings.Builder{},
				s:       "",
			},
			want: "",
		},
		{
			name: "string with \r",
			args: args{
				builder: &strings.Builder{},
				s:       "string with \r carriage return",
			},
			want: "string with \\r carriage return",
		},
		{
			name: "string with \n and \t",
			args: args{
				builder: &strings.Builder{},
				s:       "string with \n new line and \t tab",
			},
			want: "string with \\n new line and \\t tab",
		},
		{
			name: "string with double quotes",
			args: args{
				builder: &strings.Builder{},
				s:       "string with \"double\" quotes",
			},
			want: "string with \\\"double\\\" quotes",
		},
		{
			name: "string with \000",
			args: args{
				builder: &strings.Builder{},
				s:       "string with \000 null char",
			},
			want: "string with  null char",
		},
		{
			name: "string with mixed special characters",
			args: args{
				builder: &strings.Builder{},
				s:       "complex 'string' with \\ backslash, \"double\" quotes, \n new line, and \t tab",
			},
			want: "complex ''string'' with \\\\ backslash, \\\"double\\\" quotes, \\n new line, and \\t tab",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeEscapeStringValue(tt.args.builder, tt.args.s)
			assert.Equalf(t, tt.want, tt.args.builder.String(), "writeEscapeStringValue(%v, %v)", tt.args.builder, tt.args.s)
		})
	}
}

func Test_parseRecord(t *testing.T) {
	var logger = log.GetLogger("test").WithField("case", "Test_parseRecord")
	type args struct {
		rule        *ParsedRule
		jsonData    []byte
		missingTime bool
	}
	tests := []struct {
		name    string
		args    args
		want    []*record
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "basic case",
			args: args{
				rule: &ParsedRule{
					DB:             "db",
					SuperTable:     "stb1",
					SubTable:       "t1",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData: []byte(`[
					{"ts":"2025-11-03 10:00:00","v1":10,"v2":20},
					{"ts":"2025-11-03 11:00:00","v1":110,"v2":120}
				]`),
			},
			want: []*record{
				{
					stb:      "`db`.`stb1`",
					subTable: "t1",
					ts:       time.Date(2025, 11, 3, 10, 0, 0, 0, time.UTC),
					keys:     "`ts`,`v1`,`v2`",
					values:   "'t1','2025-11-03T10:00:00Z',10,20",
				},
				{
					stb:      "`db`.`stb1`",
					subTable: "t1",
					ts:       time.Date(2025, 11, 3, 11, 0, 0, 0, time.UTC),
					keys:     "`ts`,`v1`,`v2`",
					values:   "'t1','2025-11-03T11:00:00Z',110,120",
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "basic case with key",
			args: args{
				rule: &ParsedRule{
					DBKey:          "db_key",
					SuperTableKey:  "stb_key",
					SubTableKey:    "subtable_key",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData: []byte(`[
					{"db_key":"db","stb_key":"stb1","subtable_key":"t1","ts":"2025-11-03 10:00:00","v1":10,"v2":20},
					{"db_key":"db2","stb_key":"stb2","subtable_key":"t2","ts":"2025-11-03 11:00:00","v1":110,"v2":120}
				]`),
			},
			want: []*record{
				{
					stb:      "`db`.`stb1`",
					subTable: "t1",
					ts:       time.Date(2025, 11, 3, 10, 0, 0, 0, time.UTC),
					keys:     "`ts`,`v1`,`v2`",
					values:   "'t1','2025-11-03T10:00:00Z',10,20",
				},
				{
					stb:      "`db2`.`stb2`",
					subTable: "t2",
					ts:       time.Date(2025, 11, 3, 11, 0, 0, 0, time.UTC),
					keys:     "`ts`,`v1`,`v2`",
					values:   "'t2','2025-11-03T11:00:00Z',110,120",
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "basic case with missing time key",
			args: args{
				rule: &ParsedRule{
					DBKey:          "db_key",
					SuperTableKey:  "stb_key",
					SubTableKey:    "subtable_key",
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData:    []byte(`[{"db_key":"db","stb_key":"stb1","subtable_key":"t1","v1":10,"v2":20}]`),
				missingTime: true,
			},
			want: []*record{
				{
					stb:      "`db`.`stb1`",
					subTable: "t1",
					keys:     "`ts`,`v1`,`v2`",
					values:   "'t1','%s',10,20",
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "optional field missing",
			args: args{
				rule: &ParsedRule{
					DB:             "db",
					SuperTable:     "stb1",
					SubTable:       "t1",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2", "v3"},
					FieldOptionals: []bool{false, true, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`,`v3`",
				},
				jsonData: []byte(`[
					{"ts":"2025-11-03 10:00:00","v1":10,"v3":30},
					{"ts":"2025-11-03 11:00:00","v1":10,"v2":20,"v3":30},
					{"ts":"2025-11-03 12:00:00","v1":10,"v3":30}
				]`),
			},
			want: []*record{
				{
					stb:      "`db`.`stb1`",
					subTable: "t1",
					ts:       time.Date(2025, 11, 3, 10, 0, 0, 0, time.UTC),
					keys:     "`ts`,`v1`,`v3`",
					values:   "'t1','2025-11-03T10:00:00Z',10,30",
				},
				{
					stb:      "`db`.`stb1`",
					subTable: "t1",
					ts:       time.Date(2025, 11, 3, 11, 0, 0, 0, time.UTC),
					keys:     "`ts`,`v1`,`v2`,`v3`",
					values:   "'t1','2025-11-03T11:00:00Z',10,20,30",
				},
				{
					stb:      "`db`.`stb1`",
					subTable: "t1",
					ts:       time.Date(2025, 11, 3, 12, 0, 0, 0, time.UTC),
					keys:     "`ts`,`v1`,`v3`",
					values:   "'t1','2025-11-03T12:00:00Z',10,30",
				},
			},
			wantErr: assert.NoError,
		},
		// wrong cases
		{
			name: "invalid json",
			args: args{
				rule: &ParsedRule{
					DB:             "db",
					SuperTable:     "stb1",
					SubTable:       "t1",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData: []byte(`invalid json`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "db key not exists",
			args: args{
				rule: &ParsedRule{
					DBKey: "db_key",
				},
				jsonData: []byte(`[
					{"stb_key":"stb1","subtable_key":"t1","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "db key has null value",
			args: args{
				rule: &ParsedRule{
					DBKey: "db_key",
				},
				jsonData: []byte(`[
					{"db_key":null,"stb_key":"stb1","subtable_key":"t1","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "db key has empty string value",
			args: args{
				rule: &ParsedRule{
					DBKey: "db_key",
				},
				jsonData: []byte(`[
					{"db_key":"","stb_key":"stb1","subtable_key":"t1","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "db key has unsupported type value",
			args: args{
				rule: &ParsedRule{
					DBKey: "db_key",
				},
				jsonData: []byte(`[
					{"db_key":[1,2,3],"stb_key":"stb1","subtable_key":"t1","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "super table key not exists",
			args: args{
				rule: &ParsedRule{
					DBKey:         "db_key",
					SuperTableKey: "stb_key",
				},
				jsonData: []byte(`[
					{"db_key":"db","subtable_key":"t1","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "super table key has null value",
			args: args{
				rule: &ParsedRule{
					DBKey:         "db_key",
					SuperTableKey: "stb_key",
				},
				jsonData: []byte(`[
					{"db_key":"db","stb_key":null,"subtable_key":"t1","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "super table key has empty string value",
			args: args{
				rule: &ParsedRule{
					DBKey:         "db_key",
					SuperTableKey: "stb_key",
				},
				jsonData: []byte(`[
					{"db_key":"db","stb_key":"","subtable_key":"t1","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "super table key has unsupported type value",
			args: args{
				rule: &ParsedRule{
					DBKey:         "db_key",
					SuperTableKey: "stb_key",
				},
				jsonData: []byte(`[
					{"db_key":"db","stb_key":{"key":"value"},"subtable_key":"t1","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "sub table key not exists",
			args: args{
				rule: &ParsedRule{
					DBKey:         "db_key",
					SuperTableKey: "stb_key",
					SubTableKey:   "subtable_key",
				},
				jsonData: []byte(`[
					{"db_key":"db","stb_key":"stb1","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "sub table key has null value",
			args: args{
				rule: &ParsedRule{
					DBKey:         "db_key",
					SuperTableKey: "stb_key",
					SubTableKey:   "subtable_key",
				},
				jsonData: []byte(`[
					{"db_key":"db","stb_key":"stb1","subtable_key":null,"ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "sub table key has empty string value",
			args: args{
				rule: &ParsedRule{
					DBKey:         "db_key",
					SuperTableKey: "stb_key",
					SubTableKey:   "subtable_key",
				},
				jsonData: []byte(`[
					{"db_key":"db","stb_key":"stb1","subtable_key":"","ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "sub table key has unsupported type value",
			args: args{
				rule: &ParsedRule{
					DBKey:         "db_key",
					SuperTableKey: "stb_key",
					SubTableKey:   "subtable_key",
				},
				jsonData: []byte(`[
					{"db_key":"db","stb_key":"stb1","subtable_key":[1,2,3],"ts":"2025-11-03 10:00:00","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "time key not exists",
			args: args{
				rule: &ParsedRule{
					DB:             "db",
					SuperTable:     "stb1",
					SubTable:       "t1",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData: []byte(`[
					{"v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "time key has null value",
			args: args{
				rule: &ParsedRule{
					DB:             "db",
					SuperTable:     "stb1",
					SubTable:       "t1",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData: []byte(`[
					{"ts":null,"v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "time key has unsupported type value",
			args: args{
				rule: &ParsedRule{
					DB:             "db",
					SuperTable:     "stb1",
					SubTable:       "t1",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData: []byte(`[
					{"ts":{"time":"2025-11-03 10:00:00"},"v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "time key has invalid format value",
			args: args{
				rule: &ParsedRule{
					DB:             "db",
					SuperTable:     "stb1",
					SubTable:       "t1",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData: []byte(`[
					{"ts":"invalid time format","v1":10,"v2":20}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "non-optional field missing",
			args: args{
				rule: &ParsedRule{
					DB:             "db",
					SuperTable:     "stb1",
					SubTable:       "t1",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData: []byte(`[
					{"ts":"2025-11-03 10:00:00","v1":10}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name: "field has unsupported type value",
			args: args{
				rule: &ParsedRule{
					DB:             "db",
					SuperTable:     "stb1",
					SubTable:       "t1",
					TimeKey:        "ts",
					TimeFieldName:  "ts",
					TimeFormat:     "datetime",
					Timezone:       time.UTC,
					FieldKeys:      []string{"v1", "v2"},
					FieldOptionals: []bool{false, false},
					SqlAllColumns:  "`ts`,`v1`,`v2`",
				},
				jsonData: []byte(`[
					{"ts":"2025-11-03 10:00:00","v1":10,"v2":[1,2,3]}
				]`),
			},
			want:    nil,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var parseFuncs = []func(rule *ParsedRule, jsonData []byte, logger *logrus.Entry) ([]*record, error){
				parseRecord,
				parseRecordGJson,
				parseRecordJsoniter,
				parseRecordSonic,
				//parseRecordSimdJson,
			}
			for funcIndex, parseFunc := range parseFuncs {
				got, err := parseFunc(tt.args.rule, tt.args.jsonData, logger)
				if !tt.wantErr(t, err, fmt.Sprintf("parseRecord(%v, %v, %v)", tt.args.rule, tt.args.jsonData, logger)) {
					return
				}
				var values []string
				if tt.args.missingTime {
					// replace time placeholder
					for i := range got {
						values = append(values, tt.want[i].values)
						tt.want[i].ts = got[i].ts
						tt.want[i].values = fmt.Sprintf(tt.want[i].values, got[i].ts.Format(time.RFC3339Nano))
					}
				}
				assert.Equalf(t, tt.want, got, "parseRecord_%d(%v, %v, %v)", funcIndex, tt.args.rule, tt.args.jsonData, logger)
				if len(values) > 0 {
					for i := range got {
						tt.want[i].values = values[i]
					}
				}
			}
		})
	}
}

func TestPluginInterface(t *testing.T) {
	p := &Plugin{}
	assert.Nil(t, p.Start())
	assert.Nil(t, p.Stop())
	assert.Equal(t, "input_json", p.String())
	assert.Equal(t, "v1", p.Version())
}

func TestPlugin_ParseRulesFromConfig(t *testing.T) {
	type fields struct {
		conf  Config
		rules map[string]*ParsedRule
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "valid config",
			fields: fields{
				conf: Config{
					Rules: []*Rule{
						{
							DB:         "db1",
							SuperTable: "stb1",
							SubTable:   "t1",
							TimeKey:    "ts",
							TimeFormat: "datetime",
							Fields: []*Field{
								{Key: "v1", Optional: false},
								{Key: "v2", Optional: true},
							},
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid transformation",
			fields: fields{
				conf: Config{
					Rules: []*Rule{
						{
							DB:             "db1",
							SuperTable:     "stb1",
							SubTable:       "t1",
							TimeKey:        "ts",
							TimeFormat:     "datetime",
							Transformation: "[invalid_transformation",
							Fields: []*Field{
								{Key: "v1", Optional: false},
								{Key: "v2", Optional: true},
							},
						},
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid time format",
			fields: fields{
				conf: Config{
					Rules: []*Rule{
						{
							DB:         "db1",
							SuperTable: "stb1",
							SubTable:   "t1",
							TimeKey:    "ts",
							TimeFormat: "%9",
							Fields: []*Field{
								{Key: "v1", Optional: false},
								{Key: "v2", Optional: true},
							},
						},
					},
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid timezone",
			fields: fields{
				conf: Config{
					Rules: []*Rule{
						{
							DB:         "db1",
							SuperTable: "stb1",
							SubTable:   "t1",
							TimeKey:    "ts",
							TimeFormat: "datetime",
							Timezone:   "Invalid/Timezone",
							Fields: []*Field{
								{Key: "v1", Optional: false},
								{Key: "v2", Optional: true},
							},
						},
					},
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plugin{
				conf:  tt.fields.conf,
				rules: tt.fields.rules,
			}
			tt.wantErr(t, p.ParseRulesFromConfig(), "ParseRulesFromConfig()")
		})
	}
}

func TestPlugin_InitWithViper(t *testing.T) {
	tmpDir := t.TempDir()
	type args struct {
		r gin.IRouter
	}
	tests := []struct {
		name    string
		content string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "valid config",
			args: args{
				r: gin.New(),
			},
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
db = "test_db"
superTable = "test_stb"
subTable = "test_subtable"
fields = [
    {key = "k1", optional = false},
]
`,
			wantErr: assert.NoError,
		},
		{
			name: "disable",
			args: args{
				r: gin.New(),
			},
			content: `
[input_json]
enable = false
`,
			wantErr: assert.NoError,
		},
		{
			name: "invalid config",
			args: args{
				r: gin.New(),
			},
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
db = "test_db"
superTable = "test_stb"
subTable = "test_subtable"
timeKey = "ts"
# no time format
fields = [
    {key = "k1", optional = false},
]
`,
			wantErr: assert.Error,
		},
		{
			name: "invalid config setting",
			args: args{
				r: gin.New(),
			},
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
db = "test_db"
superTable = "test_stb"
subTable = "test_subtable"
# no fields
`,
			wantErr: assert.Error,
		},
		{
			name: "wrong config rule",
			args: args{
				r: gin.New(),
			},
			content: `
[input_json]
enable = true
[[input_json.rules]]
endpoint = "rule1"
db = "test_db"
superTable = "test_stb"
subTable = "test_subtable"
transformation = "[invalid_transformation"
fields = [
    {key = "k1", optional = false},
]
`,
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plugin{}
			configPath := filepath.Join(tmpDir, "config.toml")
			err := os.WriteFile(configPath, []byte(tt.content), 0644)
			require.NoError(t, err)
			v := viper.New()
			v.SetConfigType("toml")
			v.SetConfigFile(configPath)
			err = v.ReadInConfig()
			require.NoError(t, err)
			tt.wantErr(t, p.initWithViper(tt.args.r, v), fmt.Sprintf("Init(%v)", tt.args.r))
		})
	}
}

func TestPlugin_Init(t *testing.T) {
	r := gin.New()
	p := &Plugin{}
	err := p.Init(r)
	assert.NoError(t, err)
}

func Test_errorResponse(t *testing.T) {
	type args struct {
		httpCode int
		err      error
	}
	tests := []struct {
		name            string
		args            args
		expectHttpCode  int
		expectErrorCode int
		expectDesc      string
	}{
		{
			name: "ErrWhitelistForbidden",
			args: args{
				httpCode: http.StatusInternalServerError,
				err:      commonpool.ErrWhitelistForbidden,
			},
			expectHttpCode:  http.StatusForbidden,
			expectErrorCode: 0xffff,
			expectDesc:      "whitelist forbidden: whitelist prohibits current IP access",
		},
		{
			name: "ErrTimeout",
			args: args{
				httpCode: http.StatusInternalServerError,
				err:      connectpool.ErrTimeout,
			},
			expectHttpCode:  http.StatusGatewayTimeout,
			expectErrorCode: 0xffff,
			expectDesc:      "get connect from pool error: get connection timeout",
		},
		{
			name: "ErrMaxWait",
			args: args{
				httpCode: http.StatusInternalServerError,
				err:      connectpool.ErrMaxWait,
			},
			expectHttpCode:  http.StatusGatewayTimeout,
			expectErrorCode: 0xffff,
			expectDesc:      "get connect from pool error: exceeded connection pool max wait",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			errorResponse(c, tt.args.httpCode, tt.args.err)
			assert.Equalf(t, tt.expectHttpCode, w.Code, "errorResponse(%v, %v)", tt.args.httpCode, tt.args.err)
			var respBody message
			err := json.Unmarshal(w.Body.Bytes(), &respBody)
			assert.NoErrorf(t, err, "errorResponse(%v, %v)", tt.args.httpCode, tt.args.err)
			assert.Equalf(t, tt.expectErrorCode, respBody.Code, "errorResponse(%v, %v)", tt.args.httpCode, tt.args.err)
			assert.Equalf(t, tt.expectDesc, respBody.Desc, "errorResponse(%v, %v)", tt.args.httpCode, tt.args.err)
		})
	}
}
