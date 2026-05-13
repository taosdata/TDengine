package executor

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"time"
)

type TDTest struct {
	DriverName string
	DSN        string
}

const (
	DefaultNativeDriverName  = "taosSql"
	DefaultNativeDSN         = "root:taosdata@tcp(:)/"
	DefaultRestfulDriverName = "taosRestful"
	DefaultRestfulDSN        = "root:taosdata@http(127.0.0.1:6041)/"
)

func NewTDTest(driverName string, DSN string) *TDTest {
	if driverName == "" {
		driverName = DefaultNativeDriverName
	}
	if DSN == "" {
		DSN = DefaultNativeDSN
	}
	return &TDTest{DriverName: driverName, DSN: DSN}
}

func (t *TDTest) PrepareWrite() {
	db, err := sql.Open(t.DriverName, t.DSN)
	if err != nil {
		log.Fatalf("error on:  sql.open %s", err.Error())
	}
	_, err = db.Exec("drop database if exists benchmark_go")
	if err != nil {
		log.Fatalf("drop database error: %s", err.Error())
	}
	_, err = db.Exec("create database benchmark_go")
	if err != nil {
		log.Fatalf("create database error: %s", err.Error())
	}
	_ = db.Close()
	db, err = sql.Open(t.DriverName, t.DSN+"benchmark_go")
	if err != nil {
		log.Fatalf("error on:  sql.open %s", err.Error())
	}
	_, err = db.Exec("create table write_single_common (ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)" +
		")tags(tts timestamp," +
		"t1 bool," +
		"t2 tinyint," +
		"t3 smallint," +
		"t4 int," +
		"t5 bigint," +
		"t6 tinyint unsigned," +
		"t7 smallint unsigned," +
		"t8 int unsigned," +
		"t9 bigint unsigned," +
		"t10 float," +
		"t11 double," +
		"t12 binary(20)," +
		"t13 nchar(20)" +
		")")
	if err != nil {
		log.Fatalf("create stable error %s", err.Error())
	}
	_, err = db.Exec("create table write_single_json (ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)" +
		")tags(info json)")
	if err != nil {
		log.Fatalf("create stable error %s", err.Error())
	}

	_, err = db.Exec("create table write_batch_common (ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)" +
		")tags(tts timestamp," +
		"t1 bool," +
		"t2 tinyint," +
		"t3 smallint," +
		"t4 int," +
		"t5 bigint," +
		"t6 tinyint unsigned," +
		"t7 smallint unsigned," +
		"t8 int unsigned," +
		"t9 bigint unsigned," +
		"t10 float," +
		"t11 double," +
		"t12 binary(20)," +
		"t13 nchar(20)" +
		")")
	if err != nil {
		log.Fatalf("create stable error %s", err.Error())
	}
	_, err = db.Exec("create table write_batch_json (ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)" +
		")tags(info json)")
	if err != nil {
		log.Fatalf("create stable error %s", err.Error())
	}
	_ = db.Close()
}

func (t *TDTest) Clean() {
	db, err := sql.Open(t.DriverName, t.DSN)
	if err != nil {
		log.Fatalf("error on:  sql.open %s", err.Error())
	}
	_, err = db.Exec("drop database if exists benchmark_go")
	if err != nil {
		log.Fatalf("drop database error: %s", err.Error())
	}
}

func (t *TDTest) BenchmarkWriteSingleCommon(count int) {
	prefix := t.DriverName + ": BenchmarkWriteSingleCommon"
	fmt.Printf("%s, count = %d", prefix, count)
	db, err := sql.Open(t.DriverName, t.DSN+"benchmark_go")
	if err != nil {
		panic(err)
	}
	defer func() {
		err = db.Close()
		if err != nil {
			panic(err)
		}
	}()
	now := time.Now().UnixNano() / 1e6
	s := time.Now()
	_, err = db.Exec(fmt.Sprintf("create table if not exists wsc using write_single_common tags(%d,true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')", now))
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s : create table cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	sqls := make([]string, count)
	for i := 0; i < count; i++ {
		sqls[i] = fmt.Sprintf("insert into wsc values(%d,true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')", now+int64(i))
	}
	fmt.Printf("%s : prepare sql cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	for i := 0; i < count; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			panic(err)
		}
	}
	cost := time.Since(s)
	fmt.Printf("%s :execute count: %d, execute cost: %d ns, average cost: %f ns\n", prefix, count, cost.Nanoseconds(), float64(cost.Nanoseconds())/float64(count))
}

func (t *TDTest) BenchmarkWriteSingleJson(count int) {
	prefix := t.DriverName + ": BenchmarkWriteSingleJson"
	fmt.Printf("%s, count = %d", prefix, count)
	db, err := sql.Open(t.DriverName, t.DSN+"benchmark_go")
	if err != nil {
		panic(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)
	now := time.Now().UnixNano() / 1e6
	s := time.Now()
	_, err = db.Exec("create table if not exists wsj using write_single_json tags('{\"a\":\"b\"}')")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s : create table cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	sqls := make([]string, count)
	for i := 0; i < count; i++ {
		sqls[i] = fmt.Sprintf("insert into wsj values(%d,true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')", now+int64(i))
	}
	fmt.Printf("%s : prepare sql cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	for i := 0; i < count; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			panic(err)
		}
	}
	cost := time.Since(s)
	fmt.Printf("%s :execute count: %d, execute cost: %d ns, average cost: %f ns\n", prefix, count, cost.Nanoseconds(), float64(cost.Nanoseconds())/float64(count))
}

func (t *TDTest) BenchmarkWriteBatchCommon(count, batch int) {
	prefix := t.DriverName + ": BenchmarkWriteBatchCommon"
	fmt.Printf("%s, count = %d", prefix, count)
	db, err := sql.Open(t.DriverName, t.DSN+"benchmark_go")
	if err != nil {
		panic(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)
	now := int(time.Now().UnixNano() / 1e6)
	s := time.Now()
	_, err = db.Exec(fmt.Sprintf("create table if not exists wbc using write_batch_common tags (%d,true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')", now))
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s : create table cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	sqls := make([]string, count)
	b := &bytes.Buffer{}
	for i := 0; i < count; i++ {
		b.WriteString("insert into wbc values")
		for j := 0; j < batch; j++ {
			b.WriteString(" (")
			b.WriteString(strconv.Itoa(now + i*batch + j))
			b.WriteString(",true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')")
		}
		sqls[i] = b.String()
		b.Reset()
	}
	fmt.Printf("%s : prepare sql cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	for i := 0; i < count; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			panic(err)
		}
	}
	cost := time.Since(s)
	fmt.Printf("%s :execute count: %d, batch: %d, total record: %d, execute cost: %d ns, average count cost: %f ns,average record cost %f\n",
		prefix,
		count,
		batch,
		count*batch,
		cost.Nanoseconds(),
		float64(cost.Nanoseconds())/float64(count),
		float64(cost.Nanoseconds())/float64(batch))
}

func (t *TDTest) BenchmarkWriteBatchJson(count, batch int) {
	prefix := t.DriverName + ": BenchmarkWriteBatchJson"
	fmt.Printf("%s, count = %d", prefix, count)
	db, err := sql.Open(t.DriverName, t.DSN+"benchmark_go")
	if err != nil {
		panic(err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)
	now := int(time.Now().UnixNano() / 1e6)
	s := time.Now()
	_, err = db.Exec("create table if not exists wbj using write_batch_json tags('{\"a\":\"b\"}')")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s : create table cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	sqls := make([]string, count)
	b := &bytes.Buffer{}
	for i := 0; i < count; i++ {
		b.WriteString("insert into wbj values")
		for j := 0; j < batch; j++ {
			b.WriteString(" (")
			b.WriteString(strconv.Itoa(now + i*batch + j))
			b.WriteString(",true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')")
		}
		sqls[i] = b.String()
		b.Reset()
	}
	fmt.Printf("%s : prepare sql cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	for i := 0; i < count; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			panic(err)
		}
	}
	cost := time.Since(s)
	fmt.Printf("%s :execute count: %d, batch: %d, total record: %d, execute cost: %d ns, average count cost: %f ns,average record cost %f\n",
		prefix,
		count,
		batch,
		count*batch,
		cost.Nanoseconds(),
		float64(cost.Nanoseconds())/float64(count),
		float64(cost.Nanoseconds())/float64(batch))
}

func (t *TDTest) PrepareRead(count, batch int) (tableName string) {
	db, err := sql.Open(t.DriverName, t.DSN)
	if err != nil {
		log.Fatalf("error on:  sql.open %s", err.Error())
	}
	_, err = db.Exec("drop database if exists benchmark_go")
	if err != nil {
		log.Fatalf("drop database error: %s", err.Error())
	}
	_, err = db.Exec("create database benchmark_go")
	if err != nil {
		log.Fatalf("create database error: %s", err.Error())
	}
	err = db.Close()
	if err != nil {
		log.Fatalf("close db error: %s", err.Error())
	}
	db, err = sql.Open(t.DriverName, t.DSN+"benchmark_go")
	if err != nil {
		log.Fatalf("error on:  sql.open %s", err.Error())
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)
	_, err = db.Exec("create table read_json (ts timestamp," +
		"c1 bool," +
		"c2 tinyint," +
		"c3 smallint," +
		"c4 int," +
		"c5 bigint," +
		"c6 tinyint unsigned," +
		"c7 smallint unsigned," +
		"c8 int unsigned," +
		"c9 bigint unsigned," +
		"c10 float," +
		"c11 double," +
		"c12 binary(20)," +
		"c13 nchar(20)" +
		")tags(info json)")
	if err != nil {
		panic(err)
	}

	prefix := t.DriverName + ": PrepareRead"
	s := time.Now()
	now := int(time.Now().UnixNano() / 1e6)
	_, err = db.Exec("create table if not exists rj using read_json tags('{\"a\":\"b\"}')")
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s : create table cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	sqls := make([]string, count)
	b := &bytes.Buffer{}
	for i := 0; i < count; i++ {
		b.WriteString("insert into rj values")
		for j := 0; j < batch; j++ {
			b.WriteString(" (")
			b.WriteString(strconv.Itoa(now + i*batch + j))
			b.WriteString(",true,2,3,4,5,6,7,8,9,10,11,'binary','nchar')")
		}
		sqls[i] = b.String()
		b.Reset()
	}
	fmt.Printf("%s : prepare sql cost: %d ns\n", prefix, time.Since(s).Nanoseconds())
	s = time.Now()
	for i := 0; i < count; i++ {
		_, err = db.Exec(sqls[i])
		if err != nil {
			panic(err)
		}
	}
	cost := time.Since(s)
	fmt.Printf("%s :execute count: %d, batch: %d, total record: %d, execute cost: %d ns, average count cost: %f ns,average record cost %f\n",
		prefix,
		count,
		batch,
		count*batch,
		cost.Nanoseconds(),
		float64(cost.Nanoseconds())/float64(count),
		float64(cost.Nanoseconds())/float64(batch))
	return "read_json"
}
func (t *TDTest) BenchmarkRead(sqlStr string) {
	db, err := sql.Open(t.DriverName, t.DSN+"benchmark_go")
	if err != nil {
		log.Fatalf("error on:  sql.open %s", err.Error())
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}(db)
	prefix := t.DriverName + ": BenchmarkRead"
	s := time.Now()
	rows, err := db.Query(sqlStr)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s : query: %d ns\n", prefix, time.Since(s))
	tt, err := rows.ColumnTypes()
	if err != nil {
		log.Fatalf("ColumnTypes: %v", err)
	}
	types := make([]reflect.Type, len(tt))
	for i, tp := range tt {
		st := tp.ScanType()
		if st == nil {
			log.Fatalf("scantype is null for column %q", tp.Name())
		}
		types[i] = st
	}
	values := make([]interface{}, len(tt))
	for i := range values {
		values[i] = reflect.New(types[i]).Interface()
	}
	count := 0
	s = time.Now()
	for rows.Next() {
		count += 1
		err = rows.Scan(values...)
		if err != nil {
			log.Fatalf("scan value error: %s", err.Error())
		}
	}
	cost := time.Since(s)
	fmt.Printf("%s : result count: %d, execute cost: %d ns, average count cost: %f ns\n",
		prefix,
		count,
		cost.Nanoseconds(),
		float64(cost.Nanoseconds())/float64(count))
}
