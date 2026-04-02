package taosWS

import (
	"database/sql"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/types"
)

func generateCreatetablesql3360(db string, withJson bool) string {
	createSql := fmt.Sprintf("create table if not exists %s.alltype(ts timestamp,"+
		"c1 bool,"+
		"c2 tinyint,"+
		"c3 smallint,"+
		"c4 int,"+
		"c5 bigint,"+
		"c6 tinyint unsigned,"+
		"c7 smallint unsigned,"+
		"c8 int unsigned,"+
		"c9 bigint unsigned,"+
		"c10 float,"+
		"c11 double,"+
		"c12 binary(20),"+
		"c13 nchar(20),"+
		"c14 varbinary(100),"+
		"c15 geometry(100),"+
		"c16 decimal(8,4),"+
		"c17 decimal(20,4)"+
		")",
		db)
	if withJson {
		createSql += " tags(t json)"
	}
	return createSql
}

func generatevalues3360() (value []interface{}, scanValue []interface{}, insertSql string) {
	rand.Seed(time.Now().UnixNano())
	v1 := true
	v2 := int8(rand.Int())
	v3 := int16(rand.Int())
	v4 := rand.Int31()
	v5 := int64(rand.Int31())
	v6 := uint8(rand.Uint32())
	v7 := uint16(rand.Uint32())
	v8 := rand.Uint32()
	v9 := uint64(rand.Uint32())
	v10 := rand.Float32()
	v11 := rand.Float64()
	v12 := "test_binary"
	v13 := "test_nchar"
	v14 := []byte("test_varbinary")
	v15 := []byte{0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x59, 0x40}
	v16 := "123.4560"
	v17 := "-123456789.1234"
	ts := time.Now().Round(time.Millisecond)
	var (
		cts time.Time
		c1  bool
		c2  int8
		c3  int16
		c4  int32
		c5  int64
		c6  uint8
		c7  uint16
		c8  uint32
		c9  uint64
		c10 float32
		c11 float64
		c12 string
		c13 string
		c14 []byte
		c15 []byte
		c16 string
		c17 string
	)
	return []interface{}{
			ts, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16, v17,
		}, []interface{}{cts, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17},
		fmt.Sprintf(`values('%s',%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,'test_binary','test_nchar','test_varbinary','point(100 100)','123.456','-123456789.1234')`, ts.Format(time.RFC3339Nano), v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11)
}

// @author: xftan
// @date: 2023/10/13 11:22
// @description: test all type query
func TestAllTypeQuery_3360(t *testing.T) {
	database := "ws_test_3360"
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreatetablesql3360(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generatevalues3360()
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') %s`, database, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	var tt types.RawMessage
	dest := make([]interface{}, len(scanValues)+1)
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	dest[len(scanValues)] = &tt
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), tt)
}

// @author: xftan
// @date: 2023/10/13 11:22
// @description: test null value
func TestAllTypeQueryNull_3360(t *testing.T) {
	database := "ws_test_null_3360"
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreatetablesql3360(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, _, _ := generatevalues3360()
	builder := &strings.Builder{}
	for i := 1; i < len(colValues); i++ {
		builder.WriteString(",null")
	}
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') values('%s'%s)`, database, database, colValues[0].(time.Time).Format(time.RFC3339Nano), builder.String()))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	values := make([]interface{}, len(cTypes))
	values[0] = new(time.Time)
	for i := 1; i < len(colValues); i++ {
		var v interface{}
		values[i] = &v
	}
	var tt types.RawMessage
	values[len(colValues)] = &tt
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			t.Fatal(err)
		}
	}
	assert.Equal(t, *values[0].(*time.Time), colValues[0].(time.Time))
	for i := 1; i < len(values)-1; i++ {
		assert.Nil(t, *values[i].(*interface{}))
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), *(values[len(values)-1]).(*types.RawMessage))
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test compression
func TestAllTypeQueryCompression_3360(t *testing.T) {
	database := "ws_test_compression_3360"
	db, err := sql.Open("taosWS", dataSourceNameWithCompression)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreatetablesql3360(database, true))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generatevalues3360()
	_, err = exec(db, fmt.Sprintf(`insert into %s.t1 using %s.alltype tags('{"a":"b"}') %s`, database, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	var tt types.RawMessage
	dest := make([]interface{}, len(scanValues)+1)
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	dest[len(scanValues)] = &tt
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
	assert.Equal(t, types.RawMessage(`{"a":"b"}`), tt)
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test all type query without json
func TestAllTypeQueryWithoutJson_3360(t *testing.T) {
	database := "ws_test_without_json_3360"
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreatetablesql3360(database, false))
	if err != nil {
		t.Fatal(err)
	}
	colValues, scanValues, insertSql := generatevalues3360()
	_, err = exec(db, fmt.Sprintf(`insert into %s.alltype %s`, database, insertSql))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	dest := make([]interface{}, len(scanValues))
	for i := range scanValues {
		dest[i] = reflect.ValueOf(&scanValues[i]).Interface()
	}
	for rows.Next() {
		err := rows.Scan(dest...)
		assert.NoError(t, err)
	}
	for i, v := range colValues {
		assert.Equal(t, v, scanValues[i])
	}
}

// @author: xftan
// @date: 2023/10/13 11:24
// @description: test all type query with null without json
func TestAllTypeQueryNullWithoutJson_3360(t *testing.T) {
	database := "ws_test_without_json_null_3360"
	db, err := sql.Open("taosWS", dataSourceName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = db.Ping()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, err = exec(db, fmt.Sprintf("drop database if exists %s", database))
		if err != nil {
			t.Fatal(err)
		}
	}()
	_, err = exec(db, fmt.Sprintf("create database if not exists %s", database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec(db, generateCreatetablesql3360(database, false))
	if err != nil {
		t.Fatal(err)
	}
	colValues, _, _ := generatevalues3360()
	builder := &strings.Builder{}
	for i := 1; i < len(colValues); i++ {
		builder.WriteString(",null")
	}
	insertSql := fmt.Sprintf(`insert into %s.alltype values('%s'%s)`, database, colValues[0].(time.Time).Format(time.RFC3339Nano), builder.String())
	_, err = exec(db, insertSql)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(fmt.Sprintf("select * from %s.alltype where ts = '%s'", database, colValues[0].(time.Time).Format(time.RFC3339Nano)))
	assert.NoError(t, err)
	columns, err := rows.Columns()
	assert.NoError(t, err)
	t.Log(columns)
	cTypes, err := rows.ColumnTypes()
	assert.NoError(t, err)
	t.Log(cTypes)
	values := make([]interface{}, len(cTypes))
	values[0] = new(time.Time)
	for i := 1; i < len(colValues); i++ {
		var v interface{}
		values[i] = &v
	}
	for rows.Next() {
		err := rows.Scan(values...)
		if err != nil {
			t.Fatal(err)
		}
	}
	assert.Equal(t, *values[0].(*time.Time), colValues[0].(time.Time))
	for i := 1; i < len(values)-1; i++ {
		assert.Nil(t, *values[i].(*interface{}))
	}
}
