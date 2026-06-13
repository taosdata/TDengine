package sqlparser

import (
	"reflect"
	"testing"
)

func TestParse_CreateDatabase(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name    string
		args    args
		want    Statement
		wantErr bool
	}{
		{
			name: "simple create database",
			args: args{
				sql: "CREATE DATABASE testdb;",
			},
			want: &CreateDatabaseStmt{
				DbName:       "testdb",
				IgnoreExists: false,
				Options:      &DatabaseOptions{},
			},
			wantErr: false,
		},
		{
			name: "simple create database with uppercase",
			args: args{
				sql: "CREATE DATABASE testDB",
			},
			want: &CreateDatabaseStmt{
				DbName:       "testdb",
				IgnoreExists: false,
				Options:      &DatabaseOptions{},
			},
			wantErr: false,
		},
		{
			name: "simple create database",
			args: args{
				sql: "CREATE DATABASE `testDB`",
			},
			want: &CreateDatabaseStmt{
				DbName:       "testDB",
				IgnoreExists: false,
				Options:      &DatabaseOptions{},
			},
			wantErr: false,
		},
		{
			name: "create database if not exists",
			args: args{
				sql: "CREATE DATABASE IF NOT EXISTS testdb;",
			},
			want: &CreateDatabaseStmt{
				DbName:       "testdb",
				IgnoreExists: true,
				Options:      &DatabaseOptions{},
			},
			wantErr: false,
		},
		{
			name: "create database if not exists with buffer",
			args: args{
				sql: "CREATE DATABASE IF NOT EXISTS testdb buffer 123;",
			},
			want: &CreateDatabaseStmt{
				DbName:       "testdb",
				IgnoreExists: true,
				Options: &DatabaseOptions{
					Buffer: 123,
				},
			},
			wantErr: false,
		},
		{
			name: "wrong buffer value",
			args: args{
				sql: "CREATE DATABASE testdb buffer 999999999999999 cachesize 123;",
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "with cache size",
			args: args{
				sql: "CREATE DATABASE testdb buffer 256 cachesize 2048;",
			},
			want: &CreateDatabaseStmt{
				DbName:       "testdb",
				IgnoreExists: false,
				Options: &DatabaseOptions{
					CacheLastSize: 2048,
					Buffer:        256,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.args.sql)
			if err != nil {
				t.Log(err)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParse_Insert(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name    string
		args    args
		want    Statement
		wantErr bool
	}{
		{
			name: "simple insert",
			args: args{
				sql: "insert into test.d_1 using test.meters (`Groupid`,`location`) tags (1,'location1') (`ts`, `current`, `voltage` , `status`) values (1627891234, 10.2, 219, 0.31);",
			},
			want: InsertStatement{
				{
					TableName: &TableName{
						Qualifier: NewTableIdent("test"),
						Name:      NewTableIdent("d_1"),
					},
					Using: &UsingClause{
						TableName: &TableName{
							Qualifier: NewTableIdent("test"),
							Name:      NewTableIdent("meters"),
						},
						TagKeys:   [][]byte{[]byte("Groupid"), []byte("location")},
						TagValues: []*SQLVal{NewIntVal([]byte("1")), NewStrVal([]byte("location1"))},
					},
					Fields: [][]byte{[]byte("ts"), []byte("current"), []byte("voltage"), []byte("status")},
					Values: [][]*SQLVal{
						{NewIntVal([]byte("1627891234")), NewFloatVal([]byte("10.2")), NewIntVal([]byte("219")), NewFloatVal([]byte("0.31"))},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "insert with multiple value sets",
			args: args{
				sql: "insert into test.d_1 using test.meters (`Groupid`,`location`) tags (1,'location1') (`ts`, `current`, `voltage` , `status`) values (1627891234, 10.2, 219, 0.31), (1627891294, 10.5, 220, 0.29) test.d_2 using test.meters2 (`location`,`Groupid`) tags('location2',2) values(1627891235, 11.2, 221, 0.32)(1627891236, 11.3, 223, 0.42);",
			},
			want: InsertStatement{
				{
					TableName: &TableName{
						Qualifier: NewTableIdent("test"),
						Name:      NewTableIdent("d_1"),
					},
					Using: &UsingClause{
						TableName: &TableName{
							Qualifier: NewTableIdent("test"),
							Name:      NewTableIdent("meters"),
						},
						TagKeys:   [][]byte{[]byte("Groupid"), []byte("location")},
						TagValues: []*SQLVal{NewIntVal([]byte("1")), NewStrVal([]byte("location1"))},
					},
					Fields: [][]byte{[]byte("ts"), []byte("current"), []byte("voltage"), []byte("status")},
					Values: [][]*SQLVal{
						{NewIntVal([]byte("1627891234")), NewFloatVal([]byte("10.2")), NewIntVal([]byte("219")), NewFloatVal([]byte("0.31"))},
						{NewIntVal([]byte("1627891294")), NewFloatVal([]byte("10.5")), NewIntVal([]byte("220")), NewFloatVal([]byte("0.29"))},
					},
				},
				{
					TableName: &TableName{
						Qualifier: NewTableIdent("test"),
						Name:      NewTableIdent("d_2"),
					},
					Using: &UsingClause{
						TableName: &TableName{
							Qualifier: NewTableIdent("test"),
							Name:      NewTableIdent("meters2"),
						},
						TagKeys:   [][]byte{[]byte("location"), []byte("Groupid")},
						TagValues: []*SQLVal{NewStrVal([]byte("location2")), NewIntVal([]byte("2"))},
					},
					Fields: nil,
					Values: [][]*SQLVal{
						{NewIntVal([]byte("1627891235")), NewFloatVal([]byte("11.2")), NewIntVal([]byte("221")), NewFloatVal([]byte("0.32"))},
						{NewIntVal([]byte("1627891236")), NewFloatVal([]byte("11.3")), NewIntVal([]byte("223")), NewFloatVal([]byte("0.42"))},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "simple subtable insert",
			args: args{
				sql: "insert into test.d_1  (`ts`, `current`, `voltage` , `status`) values (1627891234, 10.2, 219, 0.31) test.d_2 values(1627891235,10.3,220,0.33)(1627891236,11.3,221,0.31);",
			},
			want: InsertStatement{
				{
					TableName: &TableName{
						Qualifier: NewTableIdent("test"),
						Name:      NewTableIdent("d_1"),
					},
					Using:  nil,
					Fields: [][]byte{[]byte("ts"), []byte("current"), []byte("voltage"), []byte("status")},
					Values: [][]*SQLVal{
						{NewIntVal([]byte("1627891234")), NewFloatVal([]byte("10.2")), NewIntVal([]byte("219")), NewFloatVal([]byte("0.31"))},
					},
				},
				{
					TableName: &TableName{
						Qualifier: NewTableIdent("test"),
						Name:      NewTableIdent("d_2"),
					},
					Using:  nil,
					Fields: nil,
					Values: [][]*SQLVal{
						{NewIntVal([]byte("1627891235")), NewFloatVal([]byte("10.3")), NewIntVal([]byte("220")), NewFloatVal([]byte("0.33"))},
						{NewIntVal([]byte("1627891236")), NewFloatVal([]byte("11.3")), NewIntVal([]byte("221")), NewFloatVal([]byte("0.31"))},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "simple subtable insert without dbname",
			args: args{
				sql: "insert into test.d_1  (`ts`, `current`, `voltage` , `status`) values (1627891234, 10.2, 219, 0.31) test.d_2 values(1627891235,10.3,220,0.33)(1627891236,11.3,221,0.31);",
			},
			want: InsertStatement{
				{
					TableName: &TableName{
						Qualifier: NewTableIdent("test"),
						Name:      NewTableIdent("d_1"),
					},
					Using:  nil,
					Fields: [][]byte{[]byte("ts"), []byte("current"), []byte("voltage"), []byte("status")},
					Values: [][]*SQLVal{
						{NewIntVal([]byte("1627891234")), NewFloatVal([]byte("10.2")), NewIntVal([]byte("219")), NewFloatVal([]byte("0.31"))},
					},
				},
				{
					TableName: &TableName{
						Qualifier: NewTableIdent("test"),
						Name:      NewTableIdent("d_2"),
					},
					Using:  nil,
					Fields: nil,
					Values: [][]*SQLVal{
						{NewIntVal([]byte("1627891235")), NewFloatVal([]byte("10.3")), NewIntVal([]byte("220")), NewFloatVal([]byte("0.33"))},
						{NewIntVal([]byte("1627891236")), NewFloatVal([]byte("11.3")), NewIntVal([]byte("221")), NewFloatVal([]byte("0.31"))},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "simple subtable insert with now and today",
			args: args{
				sql: "insert into test.d_1  (`ts`, `current`, `voltage` , `status`) values (now, 10.2, 219, 0.31) (today, 10.2, 219, 0.31) test.d_2 values(now(),10.3,220,0.33)(today(),11.3,221,0.31);",
			},
			want: InsertStatement{
				{
					TableName: &TableName{
						Qualifier: NewTableIdent("test"),
						Name:      NewTableIdent("d_1"),
					},
					Using:  nil,
					Fields: [][]byte{[]byte("ts"), []byte("current"), []byte("voltage"), []byte("status")},
					Values: [][]*SQLVal{
						{NewTimeVal([]byte("now")), NewFloatVal([]byte("10.2")), NewIntVal([]byte("219")), NewFloatVal([]byte("0.31"))},
						{NewTimeVal([]byte("today")), NewFloatVal([]byte("10.2")), NewIntVal([]byte("219")), NewFloatVal([]byte("0.31"))},
					},
				},
				{
					TableName: &TableName{
						Qualifier: NewTableIdent("test"),
						Name:      NewTableIdent("d_2"),
					},
					Using:  nil,
					Fields: nil,
					Values: [][]*SQLVal{
						{NewTimeVal([]byte("now")), NewFloatVal([]byte("10.3")), NewIntVal([]byte("220")), NewFloatVal([]byte("0.33"))},
						{NewTimeVal([]byte("today")), NewFloatVal([]byte("11.3")), NewIntVal([]byte("221")), NewFloatVal([]byte("0.31"))},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.args.sql)
			if err != nil {
				t.Log(err)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestParse_Select(t *testing.T) {
	stmt, err := Parse("select /*+ batch_scan() */ v from t1;")
	if err != nil {
		t.Fatalf("parse hinted select failed: %v", err)
	}
	s, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if s.Hint == nil || s.Hint.HintType != HINT_BATCH_SCAN {
		t.Fatalf("unexpected hint parse result: %+v", s.Hint)
	}
}

func TestParse_CreateTable(t *testing.T) {
	type args struct {
		sql string
	}
	tests := []struct {
		name    string
		args    args
		want    Statement
		wantErr bool
	}{
		{
			name: "simple create database",
			args: args{
				sql: "CREATE table testdb.test_table(ts timestamp, v int)",
			},
			want: &CreateTableStmt{
				TableName: &TableName{
					Name:      NewTableIdent("test_table"),
					Qualifier: NewTableIdent("testdb"),
				},
				IgnoreExists: false,
				Columns: []*ColumnDef{
					{
						ColName: "ts",
						DataType: DataType{
							Type:  TSDB_DATA_TYPE_TIMESTAMP,
							Bytes: 8,
						},
						Options: nil,
						SMA:     false,
					},
					{
						ColName: "v",
						DataType: DataType{
							Type:  TSDB_DATA_TYPE_INT,
							Bytes: 4,
						},
						Options: nil,
						SMA:     false,
					},
				},
				Tags:    nil,
				Options: &TableOptions{},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Parse(tt.args.sql)
			if err != nil {
				t.Log(err)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
		})
	}
}
