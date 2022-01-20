package main

import (
	"fmt"
	"log"
	"nano/connector"
	"time"

	"github.com/taosdata/go-utils/tdengine/config"
)

func main() {
	e, err := connector.NewExecutor(&config.TDengineGo{
		Address:     "root:taosdata@/tcp(127.0.0.1:6030)/",
		MaxIdle:     20,
		MaxOpen:     30,
		MaxLifetime: 30,
	}, "db", false)
	if err != nil {
		panic(err)
	}
	prepareData(e)
	data, err := e.Query("select * from tb")
	if err != nil {
		panic(err)
	}

	layout := "2006-01-02 15:04:05.999999999"
	t0, _ := time.Parse(layout, "2021-06-10 00:00:00.100000001")
	t1, _ := time.Parse(layout, "2021-06-10 00:00:00.150000000")
	t2, _ := time.Parse(layout, "2021-06-10 00:00:00.299999999")
	t3, _ := time.Parse(layout, "2021-06-10 00:00:00.300000000")
	t4, _ := time.Parse(layout, "2021-06-10 00:00:00.300000001")
	t5, _ := time.Parse(layout, "2021-06-10 00:00:00.999999999")

	e.CheckData2(0, 0, t0, data)
	e.CheckData2(1, 0, t1, data)
	e.CheckData2(2, 0, t2, data)
	e.CheckData2(3, 0, t3, data)
	e.CheckData2(4, 0, t4, data)
	e.CheckData2(5, 0, t5, data)
	e.CheckData2(3, 1, int32(3), data)
	e.CheckData2(4, 1, int32(5), data)
	e.CheckData2(5, 1, int32(7), data)

	fmt.Println(" start check nano support!")

	data, _ = e.Query("select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400100000002;")
	e.CheckData2(0, 0, int64(1), data)

	data, _ = e.Query("select count(*) from tb where ts > \"2021-06-10 0:00:00.100000001\" and ts < \"2021-06-10 0:00:00.160000000\";")
	e.CheckData2(0, 0, int64(1), data)

	data, _ = e.Query("select count(*) from tb where ts > 1623254400100000000 and ts < 1623254400150000000;")
	e.CheckData2(0, 0, int64(1), data)
	data, _ = e.Query("select count(*) from tb where ts > \"2021-06-10 0:00:00.100000000\" and ts < \"2021-06-10 0:00:00.150000000\";")
	e.CheckData2(0, 0, int64(1), data)

	data, _ = e.Query("select count(*) from tb where ts > 1623254400400000000;")
	e.CheckData2(0, 0, int64(1), data)
	data, _ = e.Query("select count(*) from tb where ts < \"2021-06-10 00:00:00.400000000\";")
	e.CheckData2(0, 0, int64(5), data)

	data, _ = e.Query("select count(*) from tb where ts < now + 400000000b;")
	e.CheckData2(0, 0, int64(6), data)

	data, _ = e.Query("select count(*) from tb where ts >= \"2021-06-10 0:00:00.100000001\";")
	e.CheckData2(0, 0, int64(6), data)

	data, _ = e.Query("select count(*) from tb where ts <= 1623254400300000000;")
	e.CheckData2(0, 0, int64(4), data)

	data, _ = e.Query("select count(*) from tb where ts = \"2021-06-10 0:00:00.000000000\";")

	data, _ = e.Query("select count(*) from tb where ts = 1623254400150000000;")
	e.CheckData2(0, 0, int64(1), data)

	data, _ = e.Query("select count(*) from tb where ts = \"2021-06-10 0:00:00.100000001\";")
	e.CheckData2(0, 0, int64(1), data)

	data, _ = e.Query("select count(*) from tb where ts between 1623254400000000000 and 1623254400400000000;")
	e.CheckData2(0, 0, int64(5), data)

	data, _ = e.Query("select count(*) from tb where ts between \"2021-06-10 0:00:00.299999999\" and \"2021-06-10 0:00:00.300000001\";")
	e.CheckData2(0, 0, int64(3), data)

	data, _ = e.Query("select avg(speed) from tb interval(5000000000b);")
	e.CheckRow(1, data)

	data, _ = e.Query("select avg(speed) from tb interval(100000000b)")
	e.CheckRow(4, data)

	data, _ = e.Query("select avg(speed) from tb interval(1000b);")
	e.CheckRow(5, data)

	data, _ = e.Query("select avg(speed) from tb interval(1u);")
	e.CheckRow(5, data)

	data, _ = e.Query("select avg(speed) from tb interval(100000000b) sliding (100000000b);")
	e.CheckRow(4, data)

	data, _ = e.Query("select last(*) from tb")
	tt, _ := time.Parse(layout, "2021-06-10 0:00:00.999999999")
	e.CheckData2(0, 0, tt, data)

	data, _ = e.Query("select first(*) from tb")
	tt1, _ := time.Parse(layout, "2021-06-10 0:00:00.100000001")
	e.CheckData2(0, 0, tt1, data)

	e.Execute("insert into tb values(now + 500000000b, 6);")
	data, _ = e.Query("select * from tb;")
	e.CheckRow(7, data)

	e.Execute("create table tb2 (ts timestamp, speed int, ts2 timestamp);")
	e.Execute("insert into tb2 values(\"2021-06-10 0:00:00.100000001\", 1, \"2021-06-11 0:00:00.100000001\");")
	e.Execute("insert into tb2 values(1623254400150000000, 2, 1623340800150000000);")
	e.Execute("import into tb2 values(1623254400300000000, 3, 1623340800300000000);")
	e.Execute("import into tb2 values(1623254400299999999, 4, 1623340800299999999);")
	e.Execute("insert into tb2 values(1623254400300000001, 5, 1623340800300000001);")
	e.Execute("insert into tb2 values(1623254400999999999, 7, 1623513600999999999);")

	data, _ = e.Query("select * from tb2;")
	tt2, _ := time.Parse(layout, "2021-06-10 0:00:00.100000001")
	tt3, _ := time.Parse(layout, "2021-06-10 0:00:00.150000000")

	e.CheckData2(0, 0, tt2, data)
	e.CheckData2(1, 0, tt3, data)
	e.CheckData2(2, 1, int32(4), data)
	e.CheckData2(3, 1, int32(3), data)
	tt4, _ := time.Parse(layout, "2021-06-11 00:00:00.300000001")
	e.CheckData2(4, 2, tt4, data)
	e.CheckRow(6, data)

	data, _ = e.Query("select count(*) from tb2 where ts2 > 1623340800000000000 and ts2 < 1623340800150000000;")
	e.CheckData2(0, 0, int64(1), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 > \"2021-06-11 0:00:00.100000000\" and ts2 < \"2021-06-11 0:00:00.100000002\";")
	e.CheckData2(0, 0, int64(1), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 > 1623340800500000000;")
	e.CheckData2(0, 0, int64(1), data)
	data, _ = e.Query("select count(*) from tb2 where ts2 < \"2021-06-11 0:00:00.400000000\";")
	e.CheckData2(0, 0, int64(5), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 < now + 400000000b;")
	e.CheckData2(0, 0, int64(6), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 >= \"2021-06-11 0:00:00.100000001\";")
	e.CheckData2(0, 0, int64(6), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 <= 1623340800400000000;")
	e.CheckData2(0, 0, int64(5), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 = \"2021-06-11 0:00:00.000000000\";")

	data, _ = e.Query("select count(*) from tb2 where ts2 = \"2021-06-11 0:00:00.300000001\";")
	e.CheckData2(0, 0, int64(1), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 = 1623340800300000001;")
	e.CheckData2(0, 0, int64(1), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 between 1623340800000000000 and 1623340800450000000;")
	e.CheckData2(0, 0, int64(5), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 between \"2021-06-11 0:00:00.299999999\" and \"2021-06-11 0:00:00.300000001\";")
	e.CheckData2(0, 0, int64(3), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 <> 1623513600999999999;")
	e.CheckData2(0, 0, int64(5), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 <> \"2021-06-11 0:00:00.100000001\";")
	e.CheckData2(0, 0, int64(5), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 <> \"2021-06-11 0:00:00.100000000\";")
	e.CheckData2(0, 0, int64(6), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 != 1623513600999999999;")
	e.CheckData2(0, 0, int64(5), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 != \"2021-06-11 0:00:00.100000001\";")
	e.CheckData2(0, 0, int64(5), data)

	data, _ = e.Query("select count(*) from tb2 where ts2 != \"2021-06-11 0:00:00.100000000\";")
	e.CheckData2(0, 0, int64(6), data)

	e.Execute("insert into tb2 values(now + 500000000b, 6, now +2d);")
	data, _ = e.Query("select * from tb2;")
	e.CheckRow(7, data)

	e.Execute("create table tb3 (ts timestamp, speed int);")
	_, err = e.Execute("insert into tb3 values(16232544001500000, 2);")
	if err != nil {
		fmt.Println("check pass! ")
	}

	e.Execute("insert into tb3 values(\"2021-06-10 0:00:00.123456\", 2);")
	data, _ = e.Query("select * from tb3 where ts = \"2021-06-10 0:00:00.123456000\";")
	e.CheckRow(1, data)

	e.Execute("insert into tb3 values(\"2021-06-10 0:00:00.123456789000\", 2);")
	data, _ = e.Query("select * from tb3 where ts = \"2021-06-10 0:00:00.123456789\";")
	e.CheckRow(1, data)

	// check timezone support

	e.Execute("drop database if exists nsdb;")
	e.Execute("create database nsdb precision 'ns';")
	e.Execute("use nsdb;")
	e.Execute("create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);")
	e.Execute("insert into tb1 using st tags('2021-06-10 0:00:00.123456789' , 1 ) values('2021-06-10T0:00:00.123456789+07:00' , 1.0);")
	data, _ = e.Query("select first(*) from tb1;")

	ttt, _ := time.Parse(layout, "2021-06-10 01:00:00.123456789")
	e.CheckData2(0, 0, ttt, data)

	e.Execute("create database usdb precision 'us';")
	e.Execute("use usdb;")
	e.Execute("create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);")
	e.Execute("insert into tb1 using st tags('2021-06-10 0:00:00.123456' , 1 ) values('2021-06-10T0:00:00.123456+07:00' , 1.0);")
	data, _ = e.Query("select first(*) from tb1;")
	ttt2, _ := time.Parse(layout, "2021-06-10 01:00:00.123456")
	e.CheckData2(0, 0, ttt2, data)

	e.Execute("drop database if exists msdb;")
	e.Execute("create database msdb precision 'ms';")
	e.Execute("use msdb;")
	e.Execute("create stable st (ts timestamp ,speed float ) tags(time timestamp ,id int);")
	e.Execute("insert into tb1 using st tags('2021-06-10 0:00:00.123' , 1 ) values('2021-06-10T0:00:00.123+07:00' , 1.0);")
	data, _ = e.Query("select first(*) from tb1;")
	ttt3, _ := time.Parse(layout, "2021-06-10 01:00:00.123")
	e.CheckData2(0, 0, ttt3, data)
	fmt.Println("all test done!")

}

func prepareData(e *connector.Executor) {
	sqlList := []string{
		"reset query cache;",
		"drop database if exists db;",
		"create database db;",
		"use db;",
		"reset query cache;",
		"drop database if exists db;",
		"create database db precision 'ns';",
		"show databases;",
		"use db;",
		"create table tb (ts timestamp, speed int);",
		"insert into tb values('2021-06-10 0:00:00.100000001', 1);",
		"insert into tb values(1623254400150000000, 2);",
		"import into tb values(1623254400300000000, 3);",
		"import into tb values(1623254400299999999, 4);",
		"insert into tb values(1623254400300000001, 5);",
		"insert into tb values(1623254400999999999, 7);",
	}
	for _, sql := range sqlList {
		err := executeSql(e, sql)
		if err != nil {
			log.Fatalf("prepare data error:%v, sql:%s", err, sql)
		}
	}
}

func executeSql(e *connector.Executor, sql string) error {
	_, err := e.Execute(sql)
	if err != nil {
		return err
	}
	return nil
}
