package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/taosdata/driver-go/taosSql"
)

var (
	token      string
	url        string
	rowsDumped uint64
)

/*$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
 * Tools for 1.6 dump usage
 *$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$*/

// Table column information
type TableColInfo struct {
	Name   string
	Type   string
	Length int32
	Note   string
}

// Talbe information
type TableInfo struct {
	STable string
	Table  string
	SCols  []TableColInfo
	Cols   []TableColInfo
}

func taosGetTabInfo(db *sql.DB, table string, getMetaFromServer bool) (*TableInfo, error) {
	var info TableInfo
	info.Table = table

	if !getMetaFromServer {
		return &info, nil
	}

	// Get super table information if has
	_, rows, err := taosProcessQuery(db, fmt.Sprintf("show tables like '%s'", table))
	if err != nil {
		return nil, err
	}

	if len(rows) == 0 {
		log.Panicf("table %s not exists, skip", table)
		return nil, nil
	}

	info.STable = strings.Trim((*rows[0][3].(*interface{})).(string), string(0))

	// Get super table information
	if info.STable != "" {
		_, rows, err = taosProcessQuery(db, fmt.Sprintf("describe '%s'", info.STable))
		for _, row := range rows {
			var colInfo TableColInfo
			colInfo.Name = strings.Trim((*row[0].(*interface{})).(string), string(0))
			colInfo.Type = strings.Trim((*row[1].(*interface{})).(string), string(0))
			colInfo.Length = (*row[2].(*interface{})).(int32)
			colInfo.Note = strings.Trim((*row[3].(*interface{})).(string), string(0))

			info.SCols = append(info.SCols, colInfo)
		}
	}

	// Get columns information
	_, rows, err = taosProcessQuery(db, fmt.Sprintf("describe %s", table))
	if err != nil {
		return nil, err
	}

	for _, val := range rows {
		var colInfo TableColInfo
		// fmt.Println(*val[0].(*interface{}))
		colInfo.Name = strings.Trim((*val[0].(*interface{})).(string), string(0))
		colInfo.Type = strings.Trim((*val[1].(*interface{})).(string), string(0))
		colInfo.Length = (*val[2].(*interface{})).(int32)
		colInfo.Note = strings.Trim((*val[3].(*interface{})).(string), string(0))

		info.Cols = append(info.Cols, colInfo)
	}

	return &info, nil
}

func taosProcessQuery(db *sql.DB, sql string) ([]*sql.ColumnType, [][]interface{}, error) {
	rows, err := db.Query(sql)
	if err != nil {
		return nil, nil, err
	}

	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	vals := make([][]interface{}, 0)
	i := 0
	for rows.Next() {
		vals = append(vals, make([]interface{}, 0, len(cols)))
		for range cols {
			var v interface{}
			vals[i] = append(vals[i], &v)
		}

		rows.Scan(vals[i]...)
		i++
	}

	return cols, vals, nil
}

func taosDumpTableBatchData(db *sql.DB, dbname string, tbname string, tsname string, stime int64, etime int64, batch int) (string, int64, int, error) {
	var sql string
	if etime == 0 {
		sql = fmt.Sprintf("select * from %s.%s where %s > %d limit %d;", dbname, tbname, tsname, stime, batch)
	} else {
		sql = fmt.Sprintf("select * from %s.%s where %s > %d and %s <= %d limit %d;", dbname, tbname, tsname, stime, tsname, etime, batch)
	}

	_, rows, err := taosProcessQuery(db, sql)
	if err != nil {
		return "", 0, 0, err
	}

	var command bytes.Buffer
	var newStime int64
	command.Reset()

	command.WriteString("insert into ")
	command.WriteString(fmt.Sprintf("%s.%s", dbname, tbname))
	command.WriteString(" values ")
	for rowCount, row := range rows {
		command.WriteString("(")
		for i, val := range row {
			val = *val.(*interface{})

			if val == nil {
				command.WriteString("NULL")
			} else {
				switch val.(type) {
				case string:
					command.WriteString(fmt.Sprintf("'%s'", strings.Trim(val.(string), string(0))))
				default:
					command.WriteString(fmt.Sprintf("%v", val))
				}
			}

			if i != len(row)-1 {
				command.WriteString(",")
			}

			if (rowCount == len(rows)-1) && (i == 0) {
				newStime = val.(int64)
			}
		}
		command.WriteString(")")
	}

	return command.String(), newStime, len(rows), nil
}

func taosGetSchemaString(tableCols []TableColInfo) string {
	schemaString := "("

	for i, colInfo := range tableCols {
		if i != 0 {
			schemaString += ", "
		}

		if colInfo.Type == "BINARY" || colInfo.Type == "NCHAR" {
			schemaString = fmt.Sprintf("%s%s %s(%d)", schemaString, colInfo.Name, strings.ToLower(colInfo.Type), colInfo.Length)
		} else {
			schemaString = fmt.Sprintf("%s%s %s", schemaString, colInfo.Name, strings.ToLower(colInfo.Type))
		}

	}

	schemaString += ")"

	return schemaString
}

func taosDumpCreateTable(db *sql.DB, client *http.Client, dbname string, tbname string, logger *log.Logger, createTable *bool) (tInfo *TableInfo, err error) {
	tInfo, err = taosGetTabInfo(db, tbname, *createTable)
	if err != nil {
		return nil, err
	}

	if !(*createTable) {
		return tInfo, nil
	}

	if tInfo.STable != "" {
		// Create super table at first
		tagColIdx := 0
		for _, colInfo := range tInfo.SCols {
			if colInfo.Note != "" {
				break
			}
			tagColIdx++
		}

		cmd := fmt.Sprintf("create table if not exists %s.%s %s tags %s", dbname, tInfo.STable, taosGetSchemaString(tInfo.SCols[:tagColIdx]), taosGetSchemaString(tInfo.SCols[tagColIdx:]))
		err = taosSendSQLWithRest(client, cmd, logger)
		if err != nil {
			return nil, err
		}

		// Create normal table then
		cmd = fmt.Sprintf("create table if not exists %s.%s using %s.%s tags (", dbname, tbname, dbname, tInfo.STable)
		for j, colInfo := range tInfo.Cols {
			if colInfo.Note != "" {

				if colInfo.Note == "NULL" {
					cmd = fmt.Sprintf("%sNULL", cmd)
				} else {
					if colInfo.Type == "BINARY" || colInfo.Type == "NCHAR" {
						cmd = fmt.Sprintf("%s'%s'", cmd, colInfo.Note)
					} else {
						cmd = fmt.Sprintf("%s%s", cmd, colInfo.Note)
					}
				}
				if j != len(tInfo.Cols)-1 {
					cmd = fmt.Sprintf("%s, ", cmd)
				}
			}
		}
		cmd = cmd + ")"
		err = taosSendSQLWithRest(client, cmd, logger)
		if err != nil {
			return nil, err
		}
	} else { // try to create normal tables
		cmd := fmt.Sprintf("create table if not exists %s.%s %s", dbname, tbname, taosGetSchemaString(tInfo.Cols))
		err = taosSendSQLWithRest(client, cmd, logger)
		if err != nil {
			return nil, err
		}
	}

	return tInfo, nil
}

func taosDumpOneTableData(db *sql.DB, client *http.Client, tInfo *TableInfo, cfg *DumpCfg, logger *log.Logger) (int, error) {
	totalRows := 0

	stime := *cfg.stime
	etime := *cfg.etime
	for {
		cmd, nstime, fetchRows, err := taosDumpTableBatchData(db, *cfg.dbname, tInfo.Table, "_c0", stime, etime, *cfg.batch)
		if err != nil {
			return totalRows, err
		}

		if fetchRows == 0 {
			break
		}

		totalRows += fetchRows
		stime = nstime

		taosSendSQLWithRest(client, cmd, logger)
	}

	return totalRows, nil
}

/*$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
 * Tools for 2.0 dump usage
 *$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$*/
type JsonResult struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
}

type TokenResult struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
	Desc   string `json:"desc"`
}

func taosSendSQLWithRest(client *http.Client, sql string, logger *log.Logger) error {
	var times int
	var jsonResult JsonResult
	maxTryTime := 20
	for times = 0; times < maxTryTime; times++ {
		req, err := http.NewRequest("POST", url, bytes.NewReader([]byte(sql)))
		if err != nil {
			continue
		}

		req.Header.Add("Authorization", "Taosd "+token)

		resp, err := client.Do(req)
		if err != nil {
			continue
		}

		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			resp.Body.Close()
			continue
		}

		err = json.Unmarshal(data, &jsonResult)
		if err != nil {
			resp.Body.Close()
			continue
		}

		if jsonResult.Status != "succ" {
			resp.Body.Close()
			continue
		}

		return nil
	}

	if times >= maxTryTime {
		logger.Printf("ERROR Failed to run command, code : %d SQL: %s\n", jsonResult.Code, sql)
		return fmt.Errorf("Failed to run sql %s", sql)
	}

	return nil
}

func getToken(host string, port int, user string, pass string) (string, error) {
	url = fmt.Sprintf("http://%s:%d/rest/login/%s/%s", host, port, user, pass)
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}

	defer resp.Body.Close()

	var tokenResult TokenResult

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	err = json.Unmarshal(data, &tokenResult)
	if err != nil {
		return "", err
	}

	if tokenResult.Status != "succ" {
		fmt.Println("get http token failed")
		fmt.Println(tokenResult)
		return "", err
	}

	return tokenResult.Desc, nil

}

type DumpCfg struct {
	// Connection configuration
	srcHost  *string
	srcPort  *int
	srcUser  *string
	srcPass  *string
	destHost *string
	destPort *int
	destUser *string
	destPass *string
	// Dump configuration
	dbname     *string
	superTable *string
	stime      *int64
	etime      *int64
	// Performance configuration
	threads *int
	batch   *int
	// Other options
	logOnConsole *bool
	schemaOnly   *bool
	createSchema *bool
}

func taosGetTableNamesOfDB(db *sql.DB, superTable *string) (*[]string, error) {
	var sqlCmd string

	if *superTable == "" {
		sqlCmd = "show tables"
	} else {
		sqlCmd = fmt.Sprintf("select tbname from %s", *superTable)
	}

	_, rows, err := taosProcessQuery(db, sqlCmd)
	if err != nil {
		return nil, err
	}

	tableList := []string{}

	for _, row := range rows {
		tbname := strings.Trim((*row[0].(*interface{})).(string), string(0))
		tableList = append(tableList, tbname)
	}

	return &tableList, nil
}

func taosDumpWorker(cfg *DumpCfg, tables []string, wg *sync.WaitGroup, logger *log.Logger) {
	tRows := uint64(0)
	logger.Printf("Start to dump %d tables:%s\n", len(tables), tables)
	defer func() {
		atomic.AddUint64(&rowsDumped, tRows)
		wg.Done()
	}()

	// Connect to source engine
	db, err := sql.Open("taosSql", fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s", *cfg.srcUser, *cfg.srcPass, *cfg.srcHost, *cfg.srcPort, *cfg.dbname))
	if err != nil {
		logger.Println("ERROR Failed to connect to source engine ", err)
		return
	}

	defer db.Close()

	// Create an HTTP client
	client := &http.Client{}

	stime := time.Now()

	// Dump create-table commands
	var tInfos []*TableInfo
	for _, table := range tables {
		tInfo, err := taosDumpCreateTable(db, client, *cfg.dbname, table, logger, cfg.createSchema)
		if err != nil {
			tInfos = append(tInfos, nil)
		} else {
			tInfos = append(tInfos, tInfo)
		}
	}

	if !*cfg.schemaOnly {
		for i, tInfo := range tInfos {
			if tInfo == nil {
				logger.Printf("Skip to dump #%d table %s data...\n", i, tables[i])
				continue
			}

			logger.Printf("Start to dump #%d table %s data...\n", i, tInfo.Table)
			start := time.Now()
			totalRows, err := taosDumpOneTableData(db, client, tInfo, cfg, logger)
			tRows += uint64(totalRows)
			if err != nil {
				logger.Printf("ERROR Failed while dumping #%d table %s data\n", i, tInfo.Table)
				continue
			}
			seconds := (time.Now().Sub(start)).Seconds()
			logger.Printf("End to dump #%d table %s data, total rows: %d spent time: %f second speed: %f rows/second", i, tInfo.Table, totalRows, seconds, float64(totalRows)/seconds)
		}
	}

	logger.Printf("Finished to dump %d rows from %d tables, use %f seconds\n", tRows, len(tables), (time.Now().Sub(stime)).Seconds())
}

func taosDumpData(cfg *DumpCfg, tables []string) {
	// Connect to source engine
	db, err := sql.Open("taosSql", fmt.Sprintf("%s:%s@/tcp(%s:%d)/%s", *cfg.srcUser, *cfg.srcPass, *cfg.srcHost, *cfg.srcPort, *cfg.dbname))
	if err != nil {
		log.Fatal("Failed to connect to source engine ", err)
	}

	defer db.Close()

	// Get token and url of dest engine
	token, err = getToken(*cfg.destHost, *cfg.destPort, *cfg.destUser, *cfg.destPass)
	if err != nil {
		log.Fatal("Failed to get dest engine token ", err)
	}

	url = fmt.Sprintf("http://%s:%d/rest/sql", *cfg.destHost, *cfg.destPort)

	// Get the list of table names to dump
	var tableList *[]string
	if len(tables) == 0 {
		tableList, err = taosGetTableNamesOfDB(db, cfg.superTable)
		if err != nil {
			log.Fatal("Failed to get the table names from database ", err)
		}
	} else {
		tableList = &tables
	}

	if len(*tableList) == 0 {
		log.Println("No table data to dump, just exit!")
	} else {
		sort.Strings(*tableList)
		var threads int
		if *cfg.threads < len(*tableList) {
			threads = *cfg.threads
		} else {
			threads = len(*tableList)
		}

		meanTables := len(*tableList) / threads
		remainTables := len(*tableList) % threads

		logFName := "taosDumpTunnel.log"

		f, err := os.OpenFile(logFName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println("Failed to open file ", logFName, err)
			return
		}

		defer f.Close()

		var mw io.Writer
		if *cfg.logOnConsole {
			mw = io.MultiWriter(os.Stdout, f)
		} else {
			mw = io.MultiWriter(f)
		}

		// Launch workers to dump data
		var wg sync.WaitGroup
		sindex := 0

		log.Println("Start to dump data")
		start := time.Now()

		for i := 0; i < threads; i++ {
			wg.Add(1)

			var ntables int
			if i < remainTables {
				ntables = meanTables + 1
			} else {
				ntables = meanTables
			}
			tbs := (*tableList)[sindex : sindex+ntables]
			sindex = sindex + ntables

			logger := log.New(mw, fmt.Sprintf("routine #%d ", i), log.LstdFlags)

			go taosDumpWorker(cfg, tbs, &wg, logger)
		}

		wg.Wait()

		seconds := (time.Now().Sub(start)).Seconds()

		log.Printf("Spent %f seconds to dump %d rows of data\n", seconds, rowsDumped)
	}
}

//  Main function
func main() {
	var dumpCfg DumpCfg

	dumpCfg.srcHost = flag.String("src-host", "127.0.0.1", "data source TDengine host")
	dumpCfg.srcPort = flag.Int("src-port", 0, "data source TDengine port")
	dumpCfg.srcUser = flag.String("src-user", "root", "data source TDengine user")
	dumpCfg.srcPass = flag.String("src-pass", "taosdata", "data source TDengine password")

	dumpCfg.destHost = flag.String("dest-host", "127.0.0.1", "data dest TDengine host")
	dumpCfg.destPort = flag.Int("dest-port", 7011, "data dest TDengine port")
	dumpCfg.destUser = flag.String("dest-user", "root", "data dest TDengine user")
	dumpCfg.destPass = flag.String("dest-pass", "taosdata", "data dest TDengine password")

	dumpCfg.dbname = flag.String("db", "", "database name to dump")
	dumpCfg.superTable = flag.String("super-table", "", "super table name to dump")
	dumpCfg.stime = flag.Int64("stime", 1, "start time to dump (not included)")
	dumpCfg.etime = flag.Int64("etime", 0, "end time to dump (included)")

	dumpCfg.threads = flag.Int("threads", 5, "threads to do dump job")
	dumpCfg.batch = flag.Int("batch", 100, "batch size per dump insert")

	dumpCfg.logOnConsole = flag.Bool("log-on-console", true, "if print log on console")
	dumpCfg.schemaOnly = flag.Bool("schema-only", false, "only dump schema")
	dumpCfg.createSchema = flag.Bool("create-schema", true, "create schema before dumping data")

	flag.Parse()

	if *dumpCfg.dbname == "" {
		log.Fatal("Please assign the database name you want to dump")
	}

	args := flag.Args()

	taosDumpData(&dumpCfg, args)
}
