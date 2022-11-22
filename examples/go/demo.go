package main

import (
	"container/heap"
	"database/sql"
	"database/sql/driver"
	"flag"
	"fmt"
	"reflect"
	"runtime"
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

func GetSubTableInfo(db *sql.DB, dbname, stbname string) []tableInfo {
	tbs := make([]tableInfo, 0, 512)
	sql := "show " + dbname + ".tables"

	row, err := db.Query(sql)
	if err != nil {
		checkErr(err, sql)
	}

	for row.Next() {
		var (
			tbname     string
			createTime string
			columns    int
			stb        string
			uid        int64
			tid        int64
			vgId       int32
		)
		err := row.Scan(&tbname, &createTime, &columns, &stb, &uid, &tid, &vgId)
		if err != nil {
			checkErr(err, sql)
		}

		if len(stbname) == 0 {
			// skip normal table
			if len(stb) == 0 || strings.Compare(stb, "") == 0 {
				continue
			}
			tbs = append(tbs, tableInfo{tbname: tbname, createTime: createTime, columns: columns, stbname: stb, uid: uid, tid: tid, vgId: vgId})
			continue
		}

		if strings.Compare(stb, stbname) == 0 {
			tbs = append(tbs, tableInfo{tbname: tbname, createTime: createTime, columns: columns, stbname: stbname, uid: uid, tid: tid, vgId: vgId})
		}
	}
	row.Close()
	return tbs

}
func GetStableField(db *sql.DB, dbname, stbname string) []string {
	result := make([]string, 0, 10)
	sql := "describe " + dbname + "." + stbname
	row, err := db.Query(sql)
	if err != nil {
		checkErr(err, sql)
	}
	count := 0
	for row.Next() {
		var field string
		var ty string
		var tlen int32
		var note string
		row.Scan(&field, &ty, &tlen, &note)

		// ignore time and tag col
		if count != 0 && strings.Compare(note, "TAG") != 0 {
			// skip first and skip tag col
			result = append(result, field)
		}
		count = count + 1
	}
	row.Close()
	return result
}

func taskInit(db *sql.DB, dbname string, srcdbname string, metatable string, exptable string, tskinfo *taskInfo) {
	{
		sql := fmt.Sprintf("drop database if exists %s", dbname)
		_, err := db.Exec(sql)
		checkErr(err, sql)
	}
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

	tbs := GetSubTableInfo(db, srcdbname, "")
	fmt.Printf("tbs size %d\n", len(tbs))

	fieldDict := make(map[string][]string)

	fieldTs := time.Now().Add(time.Hour * -1000)
	for _, e := range tbs {
		tbname := e.tbname
		stbname := e.stbname

		field, ok := fieldDict[stbname]
		if !ok {
			field = GetStableField(db, srcdbname, stbname)
			fieldDict[stbname] = field
		}

		for _, f := range field {
			insertTableInfoIntoMetatable := func(db *sql.DB, metaDB string, metaTable string, srcDB string, srcTable string, srcCol string, ts time.Time, interval, threshold int) {
				sql := fmt.Sprintf("insert into %s.%s values(%v, \"%s\", \"%s\", \"%s\", %d, %d)", metaDB, metaTable, ts.UnixMilli(), srcDB, srcTable, srcCol, interval, threshold)
				_, err := db.Exec(sql)
				if err != nil {
					checkErr(err, sql)
				}
			}
			insertTableInfoIntoMetatable(db, dbname, metatable, srcdbname, tbname, f, fieldTs, 2, 2*3)
			fieldTs = fieldTs.Add(time.Millisecond * 2)
		}

		key := fmt.Sprintf("%s_%s", srcdbname, stbname)
		_, ok = tskinfo.subtask[key]
		if !ok {
			tskinfo.subtask[key] = &demo{db: db, dbname: dbname, srcdbname: srcdbname, suptabname: stbname, metaTable: metatable, exceptTable: exptable, wg: tskinfo.wg, dInterval: 2, dThreshold: 2 * 3}
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

	tbs := GetSubTableInfo(d.db, d.srcdbname, d.suptabname)
	fields := GetStableField(d.db, d.srcdbname, d.suptabname)

	lastRowDict := func(db *sql.DB, srcDB, stbname string) map[string]time.Time {
		result := make(map[string]time.Time)
		sql := fmt.Sprintf("select last_row(ts) from %s.%s group by tbname", srcDB, stbname)
		row, err := d.db.Query(sql)
		if err != nil {
			checkErr(err, sql)
		}
		for row.Next() {
			var ts time.Time
			var tbname string
			row.Scan(&ts, &tbname)
			result[tbname] = ts
		}
		row.Close()
		return result
	}(d.db, d.srcdbname, d.suptabname)

	for _, e := range tbs {
		tbname := e.tbname
		lastTime, ok := lastRowDict[tbname]
		if !ok {
			lastTime = time.Now()
		}

		for i, f := range fields {
			col := fmt.Sprintf("%s %s", tbname, f)
			d.metaDict[col] = &schema{idx: i, numOfField: len(fields), timestamp: lastTime, colName: col, interval: int32(d.dInterval), threshold: d.dThreshold}
			{
				expRecordTab := fmt.Sprintf("%s_%s_%s", d.suptabname, tbname, f)
				sql := fmt.Sprintf("create table %s.%s using %s.%s tags(\"%s\")", d.dbname, expRecordTab, d.dbname, d.exceptTable, expRecordTab)
				{
					fmt.Printf("create TAB: %s\n", sql)
					_, err := d.db.Exec(sql)
					checkErr(err, sql)
				}
			}
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
	column []interface{}
	ts     time.Time
	tbname string
}

func (d *demo) Update(stbname, tbname, col string, interval int32, threshold int32) {
	key := fmt.Sprintf("%s %s", tbname, col)
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

type ExceptSQL struct {
	lastTime    time.Time
	exceptTable string
	cost        int32
	col         string
	tab         string
	elem        *schema

	key string
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

	lastRowGroup, colIdx := func(db *sql.DB, srcDB, stbname string) (map[string]*ValueRows, map[string]int) {
		result := make(map[string]*ValueRows)
		colIdx := make(map[string]int)

		sql := fmt.Sprintf("select last_row(*) from %s.%s group by tbname", srcDB, stbname)
		row, err := db.Query(sql)
		if err != nil {
			checkErr(err, sql)
		}
		tt, err := row.ColumnTypes()
		types := make([]reflect.Type, len(tt))
		for i, tp := range tt {
			st := tp.ScanType()
			types[i] = st
		}
		columns, _ := row.Columns()

		for row.Next() {
			values := make([]interface{}, len(tt))
			for i := range values {
				values[i] = reflect.New(types[i]).Interface()
			}
			row.Scan(values...)

			ts, _ := values[0].(driver.Valuer).Value()
			tts, _ := ts.(time.Time)

			tbname, _ := values[len(tt)-1].(driver.Valuer).Value()
			ttbname, _ := tbname.(string)

			result[ttbname] = &ValueRows{column: values, ts: tts, tbname: ttbname}
		}

		row.Close()

		for i, v := range columns {
			colIdx[v] = i
		}
		return result, colIdx
	}(d.db, d.srcdbname, d.suptabname)

	expSql := make([]*ExceptSQL, 0, len(colArray))
	for _, e := range colArray {
		elem := d.metaDict[e]
		var colName string
		var tabName string
		fmt.Sscanf(e, "%s %s", &tabName, &colName)

		ts, update := func(rowGroup map[string]*ValueRows, colIdx map[string]int, tabName, colName string) (time.Time, bool) {
			var ts time.Time
			update := false

			field := fmt.Sprintf("last_row(%s)", colName)
			idx, ok1 := colIdx[field]
			row, ok2 := rowGroup[tabName]
			if ok1 && ok2 {
				if row != nil {
					v, _ := row.column[idx].(driver.Valuer).Value()
					if v != nil {
						ts = row.ts
						update = true
					}
				}
			}
			return ts, update
		}(lastRowGroup, colIdx, tabName, colName)

		if !update {
			ts = elem.timestamp
		}
		exceptTableName := fmt.Sprintf("%s_%s_%s", d.suptabname, tabName, colName)

		var dura time.Duration = ts.Sub(elem.timestamp)
		cost := int32(dura.Seconds())
		if cost == 0 {
			elem.timestamp = ts

			expSql = append(expSql, &ExceptSQL{exceptTable: exceptTableName, cost: cost, lastTime: ts, col: colName, tab: tabName, elem: elem, key: e})
		} else {
			elem.timestamp = ts
			if cost > elem.threshold {
				expSql = append(expSql, &ExceptSQL{exceptTable: exceptTableName, cost: cost, lastTime: ts, col: colName, tab: tabName, elem: elem, key: e})
			}
		}
		heap.Push(&d.heap, heapElem{timeout: int64(elem.interval) + now, colName: e})
	}

	var info strings.Builder
	info.Grow(64 * len(expSql))

	for i, v := range expSql {
		if i == 0 {
			s := fmt.Sprintf("insert into %s.%s values(%v, \"%s\", \"%s\",\"%s\", %v, %d )", d.dbname, v.exceptTable, time.Now().UnixMilli(), d.srcdbname, v.tab, v.col, v.lastTime.UnixMilli(), int(time.Now().Sub(v.elem.timestamp).Seconds()))
			info.WriteString(s)
			info.WriteString(" ")
		} else {
			s := fmt.Sprintf("%s.%s values(%v, \"%s\", \"%s\",\"%s\", %v, %d )", d.dbname, v.exceptTable, time.Now().UnixMilli(), d.srcdbname, v.tab, v.col, v.lastTime.UnixMilli(), int(time.Now().Sub(v.elem.timestamp).Seconds()))
			info.WriteString(s)
			info.WriteString(" ")
		}
	}
	if len(expSql) != 0 {
		sql := info.String()
		fmt.Printf("INSERT SQL: %s\n", sql)
		_, err := d.db.Exec(sql)
		checkErr(err, sql)
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

	cpuNum := runtime.NumCPU()
	fmt.Println("cpu核心数:", cpuNum)
	runtime.GOMAXPROCS(cpuNum)

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
