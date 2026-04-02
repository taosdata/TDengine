package bench

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

var (
	driverName     = "taosSql"
	user           = "root"
	password       = "taosdata"
	host           = ""
	port           = 6030
	dataSourceName = fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s?interpolateParams=true", user, password, host, port, "")
)

func BenchmarkInsert(b *testing.B) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		b.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	for i := 0; i < b.N; i++ {
		_, err = db.Exec("insert into bench_test.test_insert values (now,123.456)")
		if err != nil {
			b.Fatalf("insert data error %s", err)
		}
	}
}

func BenchmarkSelect(b *testing.B) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		b.Fatalf("error on:  sql.open %s", err.Error())
		return
	}
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("select * from bench_test.test_select")
		if err != nil {
			b.Fatalf("select data error %s", err.Error())
		}
		var t time.Time
		var s float64
		for rows.Next() {
			err := rows.Scan(&t, &s)
			if err != nil {
				b.Fatalf("scan error %s", err.Error())
			}
			if s != 123.456 {
				b.Fatalf("result error expect 123.456 got %f", s)
			}
		}
	}
}
