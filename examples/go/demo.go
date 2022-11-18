package main

import (
	"container/heap"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/taosdata/driver-go/v2/taosSql"
)

type heapElem struct {
	timeout int64
	colName string
}

type MinHeap []heapElem

type Column struct {
}

func (h MinHeap) Len() int {
	return len(h)
}

func (h MinHeap) Less(i, j int) bool {
	res := h[i].timeout - h[j].timeout
	if res < 0 {
		return true
	} else if res > 0 {
		return false
	}

	cmp := strings.Compare(h[i].colName, h[j].colName)
	if cmp <= 0 {
		return true
	} else {
		return false
	}
}

func (h *MinHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(heapElem))
}

func (h *MinHeap) Empty() bool {
	if len(*h) == 0 {
		return true
	}
	return false
}

func (h *MinHeap) Top() heapElem {
	return (*h)[0]
}

func (h *MinHeap) Pop() interface{} {
	res := (*h)[len(*h)-1]
	*h = (*h)[:len(*h)-1]
	return res
}

type config struct {
	hostName   string
	serverPort int
	user       string
	password   string
	dbName     string
	srcdbName  string
	supTblName string
}

var configPara config
var taosDriverName = "taosSql"
var url string

func init() {
	flag.StringVar(&configPara.hostName, "h", "127.0.0.1", "The host to connect to TDengine server.")
	flag.IntVar(&configPara.serverPort, "p", 6030, "The TCP/IP port number to use for the connection to TDengine server.")
	flag.StringVar(&configPara.user, "u", "root", "The TDengine user name to use when connecting to the server.")
	flag.StringVar(&configPara.password, "P", "taosdata", "The password to use when connecting to the server.")
	flag.StringVar(&configPara.dbName, "d", "test1", "check  database.")
	flag.StringVar(&configPara.srcdbName, "s", "test", "Destination database.")
	flag.Parse()

}

func checkErr(err error, prompt string) {
	if err != nil {
		fmt.Printf("%s\n", prompt)
		panic(err)
	}
}

type schema struct {
	idx        int
	numOfField int
	timestamp  time.Time
	colName    string
	interval   int32
	threshold  int32
}
type demo struct {
	db *sql.DB

	dbname      string
	srcdbname   string
	metaTable   string
	exceptTable string

	dStartTs   int64
	dInterval  int32
	dThreshold int32

	suptabname string

	metaDict map[string]*schema
	heap     MinHeap
	timer    *time.Timer

	wg *sync.WaitGroup
}

/***

|ts   |colName     |interval |threshold|
|now  |stbx.tx.colx|2        |5        |
|now+1|stbx.tx.colx|2        |5        |
|now+2|stbx.tx.colx|2        |5        |

***/

type taskInfo struct {
	wg      *sync.WaitGroup
	subtask map[string]*demo
}

type tableInfo struct {
	tbname     string
	createTime string
	columns    int
	stbname    string
	uid        int64
	tid        int64
	vgId       int32
}

func taskInit(db *sql.DB, dbname string, srcdbname string, metatable string, exptable string, tskinfo *taskInfo) {
	{
		sql := fmt.Sprintf("create database if not exists %s update 2", dbname)
		_, err := db.Exec(sql)
		checkErr(err, sql)
	}
	{
		sql := fmt.Sprintf("create stable if not exists %s.%s (ts timestamp, dbname binary(64), tabname binary(64), colname binary(64), lastTime timestamp,  offline int) tags(tablename binary(128))", dbname, exptable)
		_, err := db.Exec(sql)
		checkErr(err, sql)
	}

	{
		sql := fmt.Sprintf("create table if not exists %s.%s (ts timestamp,  dbname binary(64), tablename binary(64), colName binary(128),  checkInterval int, threshold  int)", dbname, metatable)
		_, err := db.Exec(sql)
		checkErr(err, sql)
	}

	sql := "show " + srcdbname + ".tables"
	tbs := make([]tableInfo, 0, 512)
	rows, _ := db.Query(sql)
	for rows.Next() {
		var (
			tbname     string
			createTime string
			columns    int
			stbname    string = ""
			uid        int64
			tid        int64
			vgId       int32
		)
		err := rows.Scan(&tbname, &createTime, &columns, &stbname, &uid, &tid, &vgId)
		if err != nil {
			checkErr(err, sql)
		}

		// ignore exceptable name
		if strings.Compare(strings.ToLower(tbname), strings.ToLower(metatable)) == 0 || strings.Compare(tbname, exptable) == 0 || strings.HasPrefix(strings.ToLower(stbname), strings.ToLower(exptable)) == true {
			continue
		}

		// ignore normal table
		if len(stbname) == 0 || strings.Compare(stbname, "") == 0 {
			continue
		}

		tbs = append(tbs, tableInfo{tbname: tbname, createTime: createTime, columns: columns, stbname: stbname, uid: uid, tid: tid, vgId: vgId})
	}
	rows.Close()

	fieldTs := time.Now().Add(time.Hour * -1000)

	for _, e := range tbs {
		tbname := e.tbname
		columns := e.columns
		stbname := e.stbname
		fields := make([]string, 0, columns)
		{
			subsql := "describe " + srcdbname + "." + stbname

			subRows, err := db.Query(subsql)
			if err != nil {
				checkErr(err, subsql)
			}

			count := 0
			for subRows.Next() {
				var field string
				var ty string
				var len int32
				var note string
				subRows.Scan(&field, &ty, &len, &note)

				// ignore time and tag col
				if count != 0 && strings.Compare(note, "TAG") != 0 {
					// skip first and skip tag col
					fields = append(fields, field)
				}
				count = count + 1

			}
			defer subRows.Close()
		}
		for _, f := range fields {
			count := 0
			{
				checkSql := fmt.Sprintf("select * from %s.%s where dbname = \"%s\" and tablename = \"%s\" and colname = \"%s\"", dbname, metatable, srcdbname, tbname, f)
				checkRow, err := db.Query(checkSql)
				if err != nil {
					checkErr(err, checkSql)
				}

				for checkRow.Next() {
					count = count + 1
					break
				}
				if count != 0 {
					continue
				}
				defer checkRow.Close()
			}

			if count == 0 {
				sql := fmt.Sprintf("insert into %s.%s values(%v, \"%s\", \"%s\", \"%s\", %d, %d)", dbname, metatable, fieldTs.UnixMilli(), srcdbname, tbname, f, 2, 2)
				_, err := db.Exec(sql)
				if err != nil {
					checkErr(err, sql)
				}
			}
			fieldTs = fieldTs.Add(time.Millisecond * 2)
		}

		key := fmt.Sprintf("%s_%s", srcdbname, stbname)
		_, ok := tskinfo.subtask[key]
		if !ok {
			tskinfo.subtask[key] = &demo{db: db, dbname: dbname, srcdbname: srcdbname, suptabname: stbname, metaTable: metatable, exceptTable: exptable, wg: tskinfo.wg}
		}
	}
}

func subTaskStart(d *demo) {

	d.Init()
	for {
		select {
		case <-d.timer.C:
			timeout := d.NextTimout()
			fmt.Printf("stbname %s, timeout %d\n", d.suptabname, timeout)
			d.timer.Reset(time.Second * time.Duration(timeout))
		}
	}
	d.wg.Done()

}
func (d *demo) Init() {
	d.heap = make(MinHeap, 0, 200)
	heap.Init(&d.heap)
	d.metaDict = make(map[string]*schema)

	fieldTs := time.Now().Add(time.Hour * -1000)

	sql := "show " + d.srcdbname + ".tables"
	tbs := make([]tableInfo, 0, 512)
	rows, _ := d.db.Query(sql)
	for rows.Next() {
		var (
			tbname     string
			createTime string
			columns    int
			stbname    string = ""
			uid        int64
			tid        int64
			vgId       int32
		)
		err := rows.Scan(&tbname, &createTime, &columns, &stbname, &uid, &tid, &vgId)
		if err != nil {
			checkErr(err, sql)
		}
		if strings.Compare(stbname, d.suptabname) == 0 {
			tbs = append(tbs, tableInfo{tbname: tbname, createTime: createTime, columns: columns, stbname: stbname, uid: uid, tid: tid, vgId: vgId})
		}

	}
	rows.Close()

	for _, e := range tbs {
		tbname := e.tbname
		columns := e.columns
		stbname := e.stbname

		fields := make([]string, 0, columns)
		// sub sql
		{
			subsql := "describe " + d.srcdbname + "." + stbname

			subRows, err := d.db.Query(subsql)
			if err != nil {
				checkErr(err, subsql)
			}

			count := 0
			for subRows.Next() {
				var field string
				var ty string
				var len int32
				var note string
				subRows.Scan(&field, &ty, &len, &note)

				// ignore time and tag col
				if count != 0 && strings.Compare(note, "TAG") != 0 {
					// skip first and skip tag col
					fields = append(fields, field)
				}
				count = count + 1

			}
			defer subRows.Close()
		}

		lastTime := time.Now()
		{
			subsql := fmt.Sprintf("select last_row(ts) from %s.%s group by tbname", d.srcdbname, stbname)
			subRows, err := d.db.Query(subsql)
			if err != nil {
				checkErr(err, subsql)
			}
			for subRows.Next() {
				var tbname string
				subRows.Scan(&lastTime, &tbname)
			}
			subRows.Close()
		}

		for i, f := range fields {
			col := fmt.Sprintf("%s %s %s", stbname, tbname, f)
			count := 0
			{

				var (
					ts            time.Time
					dbname        string
					tablename     string
					colname       string
					checkinterval int
					threshold     int
				)

				checkSql := fmt.Sprintf("select * from %s.%s where dbname = \"%s\" and tablename = \"%s\" and colname = \"%s\"", d.dbname, d.metaTable, d.srcdbname, tbname, f)
				checkRow, err := d.db.Query(checkSql)
				if err != nil {
					checkErr(err, checkSql)
				}

				for checkRow.Next() {
					_ = checkRow.Scan(&ts, &dbname, &tablename, &colname, &checkinterval, &threshold)
					d.metaDict[col] = &schema{idx: i, numOfField: len(fields), timestamp: lastTime, colName: col, interval: int32(checkinterval), threshold: int32(threshold)}

					count = count + 1
				}
				if count != 0 {
					continue
				}
				defer checkRow.Close()
			}

			if count == 0 {
				sql := fmt.Sprintf("insert into %s.%s values(%v, \"%s\", \"%s\", \"%s\", %d, %d)", d.dbname, d.metaTable, fieldTs.UnixMilli(), d.dbname, tbname, f, d.dInterval, d.dThreshold)
				_, err := d.db.Exec(sql)
				if err != nil {
					checkErr(err, sql)
				}

				d.metaDict[col] = &schema{idx: i, numOfField: len(fields), timestamp: lastTime, colName: col, interval: d.dInterval, threshold: d.dThreshold}

			}
			fieldTs = fieldTs.Add(time.Millisecond * 2)

		}
	}
	now := time.Now()
	for k, v := range d.metaDict {
		durtion := fmt.Sprintf("%ds", v.interval)
		s, _ := time.ParseDuration(durtion)
		now.Add(s)
		heap.Push(&d.heap, heapElem{timeout: now.Unix(), colName: k})
	}

	d.timer = time.NewTimer(time.Second * 1)
}

type ValueRows struct {
	rows []interface{}
	ts   time.Time
}

func (d *demo) Update(stbname string, tbname string, col string, interval int32, threshold int32) {
	key := fmt.Sprintf("%s %s %s", stbname, tbname, col)
	sql := fmt.Sprintf("select * from %s.%s where dbname = \"%s\" and tablename = \"%s\" and colName = \"%s\"", d.dbname, d.metaTable, d.dbname, tbname, col)
	rows, _ := d.db.Query(sql)
	fmt.Printf("check metatable %s, SQL: %s\n", d.metaTable, sql)
	for rows.Next() {
		var (
			ts     time.Time
			dbname string
			tbname string
			col    string
			inter  int32
			thresh int32
		)

		err := rows.Scan(&ts, &dbname, &tbname, &col, &inter, &thresh)
		if interval != inter || threshold != thresh {
			sql := fmt.Sprintf("insert into %s.%s values(%v, \"%s\", \"%s\", \"%s\", %d, %d)", d.dbname, d.metaTable, ts.UnixMilli(), d.dbname, tbname, col, interval, threshold)
			_, err = d.db.Exec(sql)
			if err != nil {
				checkErr(err, sql)
			}
		}

	}

	schemadata := d.metaDict[key]
	if schemadata != nil {
		schemadata.interval = interval
		schemadata.threshold = threshold
	}

	defer rows.Close()
}

func (d *demo) NextTimout() int32 {
	now := time.Now().Unix()
	colArray := make([]string, 0, 10)

	for !d.heap.Empty() {
		elem := d.heap.Top()
		if elem.timeout <= now {
			colArray = append(colArray, elem.colName)
			heap.Pop(&d.heap)
		} else {
			break
		}
	}

	cacheTs := make(map[string]*ValueRows)
	ts := time.Now()

	for _, e := range colArray {
		//fmt.Println("key : ", e)
		elem := d.metaDict[e]
		var stbName string
		var colName string
		var tabName string
		fmt.Sscanf(e, "%s %s %s", &stbName, &tabName, &colName)

		cacheKey := fmt.Sprintf("%s__%s", d.dbname, stbName)

		v, ok := cacheTs[cacheKey]
		if ok {
			ts = v.ts
			v, err := v.rows[elem.idx].(driver.Valuer).Value()
			if err != nil || v == nil {
			}
		} else {
			sql := fmt.Sprintf("select last_row(*) from %s.%s group by tbname", d.srcdbname, stbName)
			rows, err := d.db.Query(sql)
			if err != nil {
				checkErr(err, sql)
			}

			tt, err := rows.ColumnTypes()
			types := make([]reflect.Type, len(tt))
			for i, tp := range tt {
				st := tp.ScanType()
				types[i] = st
			}
			values := make([]interface{}, len(tt))
			for i := range values {
				values[i] = reflect.New(types[i]).Interface()
			}

			for rows.Next() {
				rows.Scan(values...)
			}

			v, err := values[0].(driver.Valuer).Value()
			if err != nil {
				checkErr(err, "invalid timestamp")
			}

			cvttime, is := v.(time.Time)

			if is {
				cacheTs[cacheKey] = &ValueRows{rows: values, ts: cvttime}
				ts = cvttime
			} else {
				cacheTs[cacheKey] = &ValueRows{rows: values, ts: ts}
				ts = ts
			}

			rows.Close()
		}

		exceptTableName := fmt.Sprintf("%s_%s_%s", stbName, tabName, colName)

		var dura time.Duration = ts.Sub(elem.timestamp)
		cost := int32(dura.Seconds())
		if cost == 0 {
			elem.timestamp = ts
			sql := fmt.Sprintf("insert into %s.%s using %s.%s tags(\"%s\") values(%v, \"%s\", \"%s\", \"%s\", %v, %d)", d.dbname, exceptTableName, d.dbname, d.exceptTable, exceptTableName, time.Now().UnixMilli(), d.dbname, tabName, colName, ts.UnixMilli(), int(time.Now().Sub(elem.timestamp).Seconds()))
			fmt.Printf("INSERT SQL: %s\n", sql)
			_, err := d.db.Exec(sql)
			if err != nil {
				checkErr(err, sql)
			}
		} else {
			elem.timestamp = ts
			if cost > elem.threshold {
				sql := fmt.Sprintf("insert into %s.%s using %s.%s tags(\"%s\") values(%v, \"%s\", \"%s\", \"%s\", %v, %d)", d.dbname, exceptTableName, d.dbname, d.exceptTable, exceptTableName, time.Now().UnixMilli(), d.dbname, tabName, colName, ts.UnixMilli(), int(time.Now().Sub(elem.timestamp).Seconds()))
				fmt.Printf("INSERT SQL: %s\n", sql)

				_, err := d.db.Exec(sql)
				if err != nil {
					checkErr(err, sql)
				}
			} else {
				//fmt.Printf("C dura %d, threshold %d not insert \n", cost, elem.threshold)
			}
		}
		heap.Push(&d.heap, heapElem{timeout: int64(elem.interval) + now, colName: e})
	}

	if !d.heap.Empty() {
		elem := d.heap.Top()
		timeout := elem.timeout - now
		if timeout < 1 {
			timeout = 1
		}
		return int32(timeout)
	}
	return 1

}

func printAllArgs() {
	fmt.Printf("\n============= args parse result: =============\n")
	fmt.Printf("hostName:             %v\n", configPara.hostName)
	fmt.Printf("serverPort:           %v\n", configPara.serverPort)
	fmt.Printf("usr:                  %v\n", configPara.user)
	fmt.Printf("password:             %v\n", configPara.password)
	fmt.Printf("dbName:               %v\n", configPara.dbName)
	fmt.Printf("srcDbName:            %v\n", configPara.srcdbName)
	fmt.Printf("stbNme:               %v\n", configPara.supTblName)
	fmt.Printf("================================================\n")
}

func main() {

	printAllArgs()
	url = "root:taosdata@/tcp(" + configPara.hostName + ":" + strconv.Itoa(configPara.serverPort) + ")/"

	db, err := sql.Open(taosDriverName, url)
	if err != nil {
		checkErr(err, "failed to connect db")
	}
	wg := sync.WaitGroup{}
	info := &taskInfo{subtask: make(map[string]*demo), wg: &wg}

	taskInit(db, configPara.dbName, configPara.srcdbName, "metatable", "exptable", info)
	for _, v := range info.subtask {
		wg.Add(1)
		go subTaskStart(v)
	}
	wg.Wait()
}
