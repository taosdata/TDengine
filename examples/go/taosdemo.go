/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

const (
	maxLocationSize = 32
	//maxSqlBufSize   = 65480
)

var locations = [maxLocationSize]string{
	"Beijing", "Shanghai", "Guangzhou", "Shenzhen",
	"HangZhou", "Tianjin", "Wuhan", "Changsha",
	"Nanjing", "Xian"}

type config struct {
	hostName             string
	serverPort           int
	user                 string
	password             string
	dbName               string
	supTblName           string
	tablePrefix          string
	mode                 string
	numOftables          int
	numOfRecordsPerTable int
	numOfRecordsPerReq   int
	numOfThreads         int
	startTimestamp       string
	startTs              int64

	keep int
	days int
}

var configPara config
var taosDriverName = "taosSql"
var url string

func init() {
	flag.StringVar(&configPara.hostName, "h", "127.0.0.1", "The host to connect to TDengine server.")
	flag.IntVar(&configPara.serverPort, "p", 6030, "The TCP/IP port number to use for the connection to TDengine server.")
	flag.StringVar(&configPara.user, "u", "root", "The TDengine user name to use when connecting to the server.")
	flag.StringVar(&configPara.password, "P", "taosdata", "The password to use when connecting to the server.")
	flag.StringVar(&configPara.dbName, "d", "test", "Destination database.")
	flag.StringVar(&configPara.tablePrefix, "m", "d", "Table prefix name.")
	flag.StringVar(&configPara.mode, "M", "r", "mode,r:raw,s:stmt")
	flag.IntVar(&configPara.numOftables, "t", 2, "The number of tables.")
	flag.IntVar(&configPara.numOfRecordsPerTable, "n", 10, "The number of records per table.")
	flag.IntVar(&configPara.numOfRecordsPerReq, "r", 3, "The number of records per request.")
	flag.IntVar(&configPara.numOfThreads, "T", 1, "The number of threads.")
	flag.StringVar(&configPara.startTimestamp, "s", "2020-10-01 08:00:00", "The start timestamp for one table.")
	flag.Parse()

	configPara.keep = 365 * 20
	configPara.days = 30
	configPara.supTblName = "meters"

	startTs, err := time.ParseInLocation("2006-01-02 15:04:05", configPara.startTimestamp, time.Local)
	if err == nil {
		configPara.startTs = startTs.UnixNano() / 1e6
	}
}

func printAllArgs() {
	fmt.Printf("\n============= args parse result: =============\n")
	fmt.Printf("hostName:             %v\n", configPara.hostName)
	fmt.Printf("serverPort:           %v\n", configPara.serverPort)
	fmt.Printf("usr:                  %v\n", configPara.user)
	fmt.Printf("password:             %v\n", configPara.password)
	fmt.Printf("dbName:               %v\n", configPara.dbName)
	fmt.Printf("mode:                 %v\n", configPara.mode)
	fmt.Printf("tablePrefix:          %v\n", configPara.tablePrefix)
	fmt.Printf("numOftables:          %v\n", configPara.numOftables)
	fmt.Printf("numOfRecordsPerTable: %v\n", configPara.numOfRecordsPerTable)
	fmt.Printf("numOfRecordsPerReq:   %v\n", configPara.numOfRecordsPerReq)
	fmt.Printf("numOfThreads:         %v\n", configPara.numOfThreads)
	fmt.Printf("startTimestamp:       %v[%v]\n", configPara.startTimestamp, configPara.startTs)
	fmt.Printf("================================================\n")
}

func main() {
	printAllArgs()
	fmt.Printf("Please press enter key to continue....\n")
	_, _ = fmt.Scanln()

	url = "root:taosdata@/tcp(" + configPara.hostName + ":" + strconv.Itoa(configPara.serverPort) + ")/"
	//url = fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s?interpolateParams=true", configPara.user, configPara.password, configPara.hostName, configPara.serverPort, configPara.dbName)
	// open connect to taos server
	//db, err := sql.Open(taosDriverName, url)
	//if err != nil {
	//  fmt.Println("Open database error: %s\n", err)
	//  os.Exit(1)
	//}
	//defer db.Close()
	rand.Seed(time.Now().Unix())

	if configPara.mode == "s" {
		fmt.Printf("\n======== start stmt mode test ========\n")
		db, err := sql.Open("taosSql", url)
		if err != nil {
			log.Fatalf("Open database error: %s\n", err)
		}
		defer db.Close()
		demodbStmt := configPara.dbName
		demotStmt := "demotStmt"
		drop_database_stmt(db, demodbStmt)
		create_database_stmt(db, demodbStmt)
		use_database_stmt(db, demodbStmt)
		create_table_stmt(db, demotStmt)
		insert_data_stmt(db, demotStmt)
		select_data_stmt(db, demotStmt)
		return
	}

	createDatabase(configPara.dbName, configPara.supTblName)
	fmt.Printf("======== create database success! ========\n\n")

	//create_table(db, stblName)
	multiThreadCreateTable(configPara.numOfThreads, configPara.numOftables, configPara.dbName, configPara.tablePrefix)
	fmt.Printf("======== create super table and child tables success! ========\n\n")

	//insert_data(db, demot)
	multiThreadInsertData(configPara.numOfThreads, configPara.numOftables, configPara.dbName, configPara.tablePrefix)
	fmt.Printf("======== insert data into child tables success! ========\n\n")

	//select_data(db, demot)
	selectTest(configPara.dbName, configPara.tablePrefix, configPara.supTblName)
	fmt.Printf("======== select data success!  ========\n\n")

	fmt.Printf("======== end demo ========\n")
}

func createDatabase(dbName string, supTblName string) {
	db, err := sql.Open(taosDriverName, url)
	if err != nil {
		fmt.Printf("Open database error: %s\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// drop database if exists
	sqlStr := "drop database if exists " + dbName
	_, err = db.Exec(sqlStr)
	checkErr(err, sqlStr)

	time.Sleep(time.Second)

	// create database
	sqlStr = "create database " + dbName
	_, err = db.Exec(sqlStr)
	checkErr(err, sqlStr)

	// use database
	//sqlStr = "use " + dbName
	//_, err = db.Exec(sqlStr)
	//checkErr(err, sqlStr)

	sqlStr = "create table if not exists " + dbName + "." + supTblName + " (ts timestamp, current float, voltage int, phase float) tags(location binary(64), groupId int);"
	_, err = db.Exec(sqlStr)
	checkErr(err, sqlStr)
}

func multiThreadCreateTable(threads int, nTables int, dbName string, tablePrefix string) {
	st := time.Now().UnixNano()

	if threads < 1 {
		threads = 1
	}

	a := nTables / threads
	if a < 1 {
		threads = nTables
		a = 1
	}

	b := nTables % threads

	last := 0
	endTblId := 0
	wg := sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		startTblId := last
		if i < b {
			endTblId = last + a
		} else {
			endTblId = last + a - 1
		}
		last = endTblId + 1
		wg.Add(1)
		go createTable(dbName, tablePrefix, startTblId, endTblId, &wg)
	}
	wg.Wait()

	et := time.Now().UnixNano()
	fmt.Printf("create tables spent duration: %6.6fs\n", (float32(et-st))/1e9)
}

func createTable(dbName string, childTblPrefix string, startTblId int, endTblId int, wg *sync.WaitGroup) {
	//fmt.Printf("subThread[%d]: create table from %d to %d \n", unix.Gettid(), startTblId, endTblId)
	// windows.GetCurrentThreadId()

	db, err := sql.Open(taosDriverName, url)
	if err != nil {
		fmt.Printf("Open database error: %s\n", err)
		os.Exit(1)
	}
	defer db.Close()

	for i := startTblId; i <= endTblId; i++ {
		sqlStr := "create table if not exists " + dbName + "." + childTblPrefix + strconv.Itoa(i) + " using " + dbName + ".meters tags('" + locations[i%maxLocationSize] + "', " + strconv.Itoa(i) + ");"
		//fmt.Printf("sqlStr:               %v\n", sqlStr)
		_, err = db.Exec(sqlStr)
		checkErr(err, sqlStr)
	}
	wg.Done()
	runtime.Goexit()
}

func generateRowData(ts int64) string {
	voltage := rand.Int() % 1000
	current := 200 + rand.Float32()
	phase := rand.Float32()
	values := "( " + strconv.FormatInt(ts, 10) + ", " + strconv.FormatFloat(float64(current), 'f', 6, 64) + ", " + strconv.Itoa(voltage) + ", " + strconv.FormatFloat(float64(phase), 'f', 6, 64) + " ) "
	return values
}

func insertData(dbName string, childTblPrefix string, startTblId int, endTblId int, wg *sync.WaitGroup) {
	//fmt.Printf("subThread[%d]: insert data to table from %d to %d \n", unix.Gettid(), startTblId, endTblId)
	// windows.GetCurrentThreadId()

	db, err := sql.Open(taosDriverName, url)
	if err != nil {
		fmt.Printf("Open database error: %s\n", err)
		os.Exit(1)
	}
	defer db.Close()

	tmpTs := configPara.startTs
	//rand.New(rand.NewSource(time.Now().UnixNano()))
	for tID := startTblId; tID <= endTblId; tID++ {
		totalNum := 0
		for {
			sqlStr := "insert into " + dbName + "." + childTblPrefix + strconv.Itoa(tID) + " values "
			currRowNum := 0
			for {
				tmpTs += 1000
				valuesOfRow := generateRowData(tmpTs)
				currRowNum += 1
				totalNum += 1

				sqlStr = fmt.Sprintf("%s %s", sqlStr, valuesOfRow)

				if currRowNum >= configPara.numOfRecordsPerReq || totalNum >= configPara.numOfRecordsPerTable {
					break
				}
			}

			res, err := db.Exec(sqlStr)
			checkErr(err, sqlStr)

			count, err := res.RowsAffected()
			checkErr(err, "rows affected")

			if count != int64(currRowNum) {
				fmt.Printf("insert data, expect affected:%d, actual:%d\n", currRowNum, count)
				os.Exit(1)
			}

			if totalNum >= configPara.numOfRecordsPerTable {
				break
			}
		}
	}

	wg.Done()
	runtime.Goexit()
}

func multiThreadInsertData(threads int, nTables int, dbName string, tablePrefix string) {
	st := time.Now().UnixNano()

	if threads < 1 {
		threads = 1
	}

	a := nTables / threads
	if a < 1 {
		threads = nTables
		a = 1
	}

	b := nTables % threads

	last := 0
	endTblId := 0
	wg := sync.WaitGroup{}
	for i := 0; i < threads; i++ {
		startTblId := last
		if i < b {
			endTblId = last + a
		} else {
			endTblId = last + a - 1
		}
		last = endTblId + 1
		wg.Add(1)
		go insertData(dbName, tablePrefix, startTblId, endTblId, &wg)
	}
	wg.Wait()

	et := time.Now().UnixNano()
	fmt.Printf("insert data spent duration: %6.6fs\n", (float32(et-st))/1e9)
}

func selectTest(dbName string, tbPrefix string, supTblName string) {
	db, err := sql.Open(taosDriverName, url)
	if err != nil {
		fmt.Printf("Open database error: %s\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// select sql 1
	limit := 3
	offset := 0
	sqlStr := "select * from " + dbName + "." + supTblName + " limit " + strconv.Itoa(limit) + " offset " + strconv.Itoa(offset)
	rows, err := db.Query(sqlStr)
	checkErr(err, sqlStr)

	defer rows.Close()
	fmt.Printf("query sql: %s\n", sqlStr)
	for rows.Next() {
		var (
			ts       string
			current  float32
			voltage  int
			phase    float32
			location string
			groupid  int
		)
		err := rows.Scan(&ts, &current, &voltage, &phase, &location, &groupid)
		if err != nil {
			checkErr(err, "rows scan fail")
		}

		fmt.Printf("ts:%s\t current:%f\t voltage:%d\t phase:%f\t location:%s\t groupid:%d\n", ts, current, voltage, phase, location, groupid)
	}
	// check iteration error
	if rows.Err() != nil {
		checkErr(err, "rows next iteration error")
	}

	// select sql 2
	sqlStr = "select avg(voltage), min(voltage), max(voltage) from " + dbName + "." + tbPrefix + strconv.Itoa(rand.Int()%configPara.numOftables)
	rows, err = db.Query(sqlStr)
	checkErr(err, sqlStr)

	defer rows.Close()
	fmt.Printf("\nquery sql: %s\n", sqlStr)
	for rows.Next() {
		var (
			voltageAvg float32
			voltageMin int
			voltageMax int
		)
		err := rows.Scan(&voltageAvg, &voltageMin, &voltageMax)
		if err != nil {
			checkErr(err, "rows scan fail")
		}

		fmt.Printf("avg(voltage):%f\t min(voltage):%d\t max(voltage):%d\n", voltageAvg, voltageMin, voltageMax)
	}
	// check iteration error
	if rows.Err() != nil {
		checkErr(err, "rows next iteration error")
	}

	// select sql 3
	sqlStr = "select last(*) from " + dbName + "." + supTblName
	rows, err = db.Query(sqlStr)
	checkErr(err, sqlStr)

	defer rows.Close()
	fmt.Printf("\nquery sql: %s\n", sqlStr)
	for rows.Next() {
		var (
			lastTs      string
			lastCurrent float32
			lastVoltage int
			lastPhase   float32
		)
		err := rows.Scan(&lastTs, &lastCurrent, &lastVoltage, &lastPhase)
		if err != nil {
			checkErr(err, "rows scan fail")
		}

		fmt.Printf("last(ts):%s\t last(current):%f\t last(voltage):%d\t last(phase):%f\n", lastTs, lastCurrent, lastVoltage, lastPhase)
	}
	// check iteration error
	if rows.Err() != nil {
		checkErr(err, "rows next iteration error")
	}
}
func drop_database_stmt(db *sql.DB, demodb string) {
	st := time.Now().Nanosecond()
	// drop test db
	res, err := db.Exec("drop database if exists " + demodb)
	checkErr(err, "drop database "+demodb)

	affectd, err := res.RowsAffected()
	checkErr(err, "drop db, res.RowsAffected")

	et := time.Now().Nanosecond()
	fmt.Printf("drop database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1e9)
}

func create_database_stmt(db *sql.DB, demodb string) {
	st := time.Now().Nanosecond()
	// create database
	//var stmt interface{}
	stmt, err := db.Prepare("create database ?")
	checkErr(err, "create db, db.Prepare")

	//var res driver.Result
	res, err := stmt.Exec(demodb)
	checkErr(err, "create db, stmt.Exec")

	//fmt.Printf("Query OK, %d row(s) affected()", res.RowsAffected())
	affectd, err := res.RowsAffected()
	checkErr(err, "create db, res.RowsAffected")

	et := time.Now().Nanosecond()
	fmt.Printf("create database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1e9)
}

func use_database_stmt(db *sql.DB, demodb string) {
	st := time.Now().Nanosecond()
	// create database
	//var stmt interface{}
	stmt, err := db.Prepare("use " + demodb)
	checkErr(err, "use db, db.Prepare")

	res, err := stmt.Exec()
	checkErr(err, "use db, stmt.Exec")

	affectd, err := res.RowsAffected()
	checkErr(err, "use db, res.RowsAffected")

	et := time.Now().Nanosecond()
	fmt.Printf("use database result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1e9)
}

func create_table_stmt(db *sql.DB, demot string) {
	st := time.Now().Nanosecond()
	// create table
	// (ts timestamp, id int, name binary(8), len tinyint, flag bool, notes binary(8), fv float, dv double)
	stmt, err := db.Prepare("create table ? (? timestamp, ? int, ? binary(10), ? tinyint, ? bool, ? binary(8), ? float, ? double)")
	checkErr(err, "create table db.Prepare")

	res, err := stmt.Exec(demot, "ts", "id", "name", "len", "flag", "notes", "fv", "dv")
	checkErr(err, "create table stmt.Exec")

	affectd, err := res.RowsAffected()
	checkErr(err, "create table res.RowsAffected")

	et := time.Now().Nanosecond()
	fmt.Printf("create table result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1e9)
}

func insert_data_stmt(db *sql.DB, demot string) {
	st := time.Now().Nanosecond()
	// insert data into table
	stmt, err := db.Prepare("insert into ? values(?, ?, ?, ?, ?, ?, ?, ?) (?, ?, ?, ?, ?, ?, ?, ?) (?, ?, ?, ?, ?, ?, ?, ?)")
	checkErr(err, "insert db.Prepare")

	res, err := stmt.Exec(demot, "now", 1000, "'haidian'", 6, true, "'AI world'", 6987.654, 321.987,
		"now+1s", 1001, "'changyang'", 7, false, "'DeepMode'", 12356.456, 128634.456,
		"now+2s", 1002, "'chuangping'", 8, true, "'database'", 3879.456, 65433478.456)
	checkErr(err, "insert data, stmt.Exec")

	affectd, err := res.RowsAffected()
	checkErr(err, "res.RowsAffected")

	et := time.Now().Nanosecond()
	fmt.Printf("insert data result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1e9)
}

func select_data_stmt(db *sql.DB, demot string) {
	st := time.Now().Nanosecond()

	stmt, err := db.Prepare("select ?, ?, ?, ?, ?, ?, ?, ? from ?") // go binary mode
	checkErr(err, "db.Prepare")

	rows, err := stmt.Query("ts", "id", "name", "len", "flag", "notes", "fv", "dv", demot)
	checkErr(err, "stmt.Query")

	fmt.Printf("%10s%s%8s %5s %8s%s %s %10s%s %7s%s %8s%s %11s%s %14s%s\n", " ", "ts", " ", "id", " ", "name", " ", "len", " ", "flag", " ", "notes", " ", "fv", " ", " ", "dv")
	var affectd int
	for rows.Next() {
		var ts string
		var name string
		var id int
		var len int8
		var flag bool
		var notes string
		var fv float32
		var dv float64

		err = rows.Scan(&ts, &id, &name, &len, &flag, &notes, &fv, &dv)
		//fmt.Println("start scan fields from row.rs, &fv:", &fv)
		//err = rows.Scan(&fv)
		checkErr(err, "rows.Scan")

		fmt.Printf("%s\t", ts)
		fmt.Printf("%d\t", id)
		fmt.Printf("%10s\t", name)
		fmt.Printf("%d\t", len)
		fmt.Printf("%t\t", flag)
		fmt.Printf("%s\t", notes)
		fmt.Printf("%06.3f\t", fv)
		fmt.Printf("%09.6f\n", dv)

		affectd++

	}

	et := time.Now().Nanosecond()
	fmt.Printf("insert data result:\n %d row(s) affectd (%6.6fs)\n\n", affectd, (float32(et-st))/1e9)
}
func checkErr(err error, prompt string) {
	if err != nil {
		fmt.Printf("%s\n", prompt)
		panic(err)
	}
}
