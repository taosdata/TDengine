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
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	dataImport "github.com/taosdata/TDengine/importSampleData/import"

	_ "github.com/taosdata/driver-go/taosSql"
)

const (
	// 主键类型必须为 timestamp
	TIMESTAMP = "timestamp"

	// 样例数据中主键时间字段是 millisecond 还是 dateTime 格式
	DATETIME    = "datetime"
	MILLISECOND = "millisecond"

	DefaultStartTime int64 = -1
	DefaultInterval  int64 = 1 * 1000 // 导入的记录时间间隔，该设置只会在指定 auto=1 之后生效，否则会根据样例数据自动计算间隔时间。单位为毫秒，默认 1000。
	DefaultDelay     int64 = -1       //

	// 当 save 为 1 时保存统计信息的表名， 默认 statistic。
	DefaultStatisticTable = "statistic"

	// 样例数据文件格式，可以是 json 或 csv
	JsonFormat = "json"
	CsvFormat  = "csv"

	SuperTablePrefix = "s_" // 超级表前缀
	SubTablePrefix   = "t_" // 子表前缀

	DriverName      = "taosSql"
	StartTimeLayout = "2006-01-02 15:04:05.000"
	InsertPrefix    = "insert into "
)

var (
	cfg          string // 导入配置文件路径，包含样例数据文件相关描述及对应 TDengine 配置信息。默认使用 config/cfg.toml
	cases        string // 需要导入的场景名称，该名称可从 -cfg 指定的配置文件中 [usecase] 查看，可同时导入多个场景，中间使用逗号分隔，如：sensor_info,camera_detection，默认为 sensor_info
	hnum         int    // 需要将样例数据进行横向扩展的倍数，假设原有样例数据包含 1 张子表 t_0 数据，指定 hnum 为 2 时会根据原有表名创建 t、t_1 两张子表。默认为 100。
	vnum         int    // 需要将样例数据进行纵向扩展的次数，如果设置为 0 代表将历史数据导入至当前时间后持续按照指定间隔导入。默认为 1000，表示将样例数据在时间轴上纵向复制1000 次
	thread       int    // 执行导入数据的线程数目，默认为 10
	batch        int    // 执行导入数据时的批量大小，默认为 100。批量是指一次写操作时，包含多少条记录
	auto         int    // 是否自动生成样例数据中的主键时间戳，1 是，0 否， 默认 0
	startTimeStr string // 导入的记录开始时间，格式为 "yyyy-MM-dd HH:mm:ss.SSS"，不设置会使用样例数据中最小时间，设置后会忽略样例数据中的主键时间，会按照指定的 start 进行导入。如果 auto 为 1，则必须设置 start，默认为空
	interval     int64  // 导入的记录时间间隔，该设置只会在指定 auto=1 之后生效，否则会根据样例数据自动计算间隔时间。单位为毫秒，默认 1000
	host         string // 导入的 TDengine 服务器 IP，默认为 127.0.0.1
	port         int    // 导入的 TDengine 服务器端口，默认为 6030
	user         string // 导入的 TDengine 用户名，默认为 root
	password     string // 导入的 TDengine 用户密码，默认为 taosdata
	dropdb       int    // 导入数据之前是否删除数据库，1 是，0 否， 默认 0
	db           string // 导入的 TDengine 数据库名称，默认为 test_yyyyMMdd
	dbparam      string // 当指定的数据库不存在时，自动创建数据库时可选项配置参数，如 days 10 cache 16000 ablocks 4，默认为空

	dataSourceName string
	startTime      int64

	superTableConfigMap = make(map[string]*superTableConfig)
	subTableMap         = make(map[string]*dataRows)
	scaleTableNames     []string

	scaleTableMap = make(map[string]*scaleTableInfo)

	successRows    []int64
	lastStaticTime time.Time
	lastTotalRows  int64
	timeTicker     *time.Ticker
	delay          int64  // 当 vnum 设置为 0 时持续导入的时间间隔，默认为所有场景中最小记录间隔时间的一半，单位 ms。
	tick           int64  // 打印统计信息的时间间隔，默认 2000 ms。
	save           int    // 是否保存统计信息到 tdengine 的 statistic 表中，1 是，0 否， 默认 0。
	saveTable      string // 当 save 为 1 时保存统计信息的表名， 默认 statistic。
)

type superTableConfig struct {
	startTime   int64
	endTime     int64
	cycleTime   int64
	avgInterval int64
	config      dataImport.CaseConfig
}

type scaleTableInfo struct {
	scaleTableName string
	subTableName   string
	insertRows     int64
}

//type tableRows struct {
//	tableName string // tableName
//	value     string // values(...)
//}

type dataRows struct {
	rows   []map[string]interface{}
	config dataImport.CaseConfig
}

func (rows dataRows) Len() int {
	return len(rows.rows)
}

func (rows dataRows) Less(i, j int) bool {
	iTime := getPrimaryKey(rows.rows[i][rows.config.Timestamp])
	jTime := getPrimaryKey(rows.rows[j][rows.config.Timestamp])
	return iTime < jTime
}

func (rows dataRows) Swap(i, j int) {
	rows.rows[i], rows.rows[j] = rows.rows[j], rows.rows[i]
}

func getPrimaryKey(value interface{}) int64 {
	val, _ := value.(int64)
	//time, _ := strconv.ParseInt(str, 10, 64)
	return val
}

func init() {
	parseArg() // parse argument

	if db == "" {
		// 导入的 TDengine 数据库名称，默认为 test_yyyyMMdd
		db = fmt.Sprintf("test_%s", time.Now().Format("20060102"))
	}

	if auto == 1 && len(startTimeStr) == 0 {
		log.Fatalf("startTime must be set when auto is 1, the format is \"yyyy-MM-dd HH:mm:ss.SSS\" ")
	}

	if len(startTimeStr) != 0 {
		t, err := time.ParseInLocation(StartTimeLayout, strings.TrimSpace(startTimeStr), time.Local)
		if err != nil {
			log.Fatalf("param startTime %s error, %s\n", startTimeStr, err)
		}

		startTime = t.UnixNano() / 1e6 // as millisecond
	} else {
		startTime = DefaultStartTime
	}

	dataSourceName = fmt.Sprintf("%s:%s@/tcp(%s:%d)/", user, password, host, port)

	printArg()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func main() {

	importConfig := dataImport.LoadConfig(cfg)

	var caseMinInterval int64 = -1

	for _, userCase := range strings.Split(cases, ",") {
		caseConfig, ok := importConfig.UserCases[userCase]

		if !ok {
			log.Println("not exist case: ", userCase)
			continue
		}

		checkUserCaseConfig(userCase, &caseConfig)

		// read file as map array
		fileRows := readFile(caseConfig)
		log.Printf("case [%s] sample data file contains %d rows.\n", userCase, len(fileRows.rows))

		if len(fileRows.rows) == 0 {
			log.Printf("there is no valid line in file %s\n", caseConfig.FilePath)
			continue
		}

		_, exists := superTableConfigMap[caseConfig.StName]
		if !exists {
			superTableConfigMap[caseConfig.StName] = &superTableConfig{config: caseConfig}
		} else {
			log.Fatalf("the stname of case %s already exist.\n", caseConfig.StName)
		}

		var start, cycleTime, avgInterval int64 = getSuperTableTimeConfig(fileRows)

		// set super table's startTime, cycleTime and avgInterval
		superTableConfigMap[caseConfig.StName].startTime = start
		superTableConfigMap[caseConfig.StName].cycleTime = cycleTime
		superTableConfigMap[caseConfig.StName].avgInterval = avgInterval

		if caseMinInterval == -1 || caseMinInterval > avgInterval {
			caseMinInterval = avgInterval
		}

		startStr := time.Unix(0, start*int64(time.Millisecond)).Format(StartTimeLayout)
		log.Printf("case [%s] startTime %s(%d), average dataInterval %d ms, cycleTime %d ms.\n", userCase, startStr, start, avgInterval, cycleTime)
	}

	if DefaultDelay == delay {
		// default delay
		delay = caseMinInterval / 2
		if delay < 1 {
			delay = 1
		}
		log.Printf("actual delay is %d ms.", delay)
	}

	superTableNum := len(superTableConfigMap)
	if superTableNum == 0 {
		log.Fatalln("no valid file, exited")
	}

	start := time.Now()
	// create super table
	createSuperTable(superTableConfigMap)
	log.Printf("create %d superTable ,used %d ms.\n", superTableNum, time.Since(start)/1e6)

	// create sub table
	start = time.Now()
	createSubTable(subTableMap)
	log.Printf("create %d times of %d subtable ,all %d tables, used %d ms.\n", hnum, len(subTableMap), len(scaleTableMap), time.Since(start)/1e6)

	subTableNum := len(scaleTableMap)

	if subTableNum < thread {
		thread = subTableNum
	}

	filePerThread := subTableNum / thread
	leftFileNum := subTableNum % thread

	var wg sync.WaitGroup

	start = time.Now()

	successRows = make([]int64, thread)

	startIndex, endIndex := 0, filePerThread
	for i := 0; i < thread; i++ {
		// start thread
		if i < leftFileNum {
			endIndex++
		}
		wg.Add(1)

		go insertData(i, startIndex, endIndex, &wg, successRows)
		startIndex, endIndex = endIndex, endIndex+filePerThread
	}

	lastStaticTime = time.Now()
	timeTicker = time.NewTicker(time.Millisecond * time.Duration(tick))
	go staticSpeed()
	wg.Wait()

	usedTime := time.Since(start)

	total := getTotalRows(successRows)

	log.Printf("finished insert %d rows, used %d ms, speed %d rows/s", total, usedTime/1e6, total*1e3/usedTime.Milliseconds())

	if vnum == 0 {
		// continue waiting for insert data
		wait := make(chan string)
		v := <-wait
		log.Printf("program receive %s, exited.\n", v)
	} else {
		timeTicker.Stop()
	}

}

func staticSpeed() {

	connection := getConnection()
	defer connection.Close()

	if save == 1 {
		_, _ = connection.Exec("use " + db)
		_, err := connection.Exec("create table if not exists " + saveTable + "(ts timestamp, speed int)")
		if err != nil {
			log.Fatalf("create %s Table error: %s\n", saveTable, err)
		}
	}

	for {
		<-timeTicker.C

		currentTime := time.Now()
		usedTime := currentTime.UnixNano() - lastStaticTime.UnixNano()

		total := getTotalRows(successRows)
		currentSuccessRows := total - lastTotalRows

		speed := currentSuccessRows * 1e9 / usedTime
		log.Printf("insert %d rows, used %d ms, speed %d rows/s", currentSuccessRows, usedTime/1e6, speed)

		if save == 1 {
			insertSql := fmt.Sprintf("insert into %s values(%d, %d)", saveTable, currentTime.UnixNano()/1e6, speed)
			_, _ = connection.Exec(insertSql)
		}

		lastStaticTime = currentTime
		lastTotalRows = total
	}

}

func getTotalRows(successRows []int64) int64 {
	var total int64 = 0
	for j := 0; j < len(successRows); j++ {
		total += successRows[j]
	}
	return total
}

func getSuperTableTimeConfig(fileRows dataRows) (start, cycleTime, avgInterval int64) {
	if auto == 1 {
		// use auto generate data time
		start = startTime
		avgInterval = interval
		maxTableRows := normalizationDataWithSameInterval(fileRows, avgInterval)
		cycleTime = maxTableRows*avgInterval + avgInterval

	} else {

		// use the sample data primary timestamp
		sort.Sort(fileRows) // sort the file data by the primaryKey
		minTime := getPrimaryKey(fileRows.rows[0][fileRows.config.Timestamp])
		maxTime := getPrimaryKey(fileRows.rows[len(fileRows.rows)-1][fileRows.config.Timestamp])

		start = minTime // default startTime use the minTime
		// 设置了start时间的话 按照start来
		if DefaultStartTime != startTime {
			start = startTime
		}

		tableNum := normalizationData(fileRows, minTime)

		if minTime == maxTime {
			avgInterval = interval
			cycleTime = tableNum*avgInterval + avgInterval
		} else {
			avgInterval = (maxTime - minTime) / int64(len(fileRows.rows)) * tableNum
			cycleTime = maxTime - minTime + avgInterval
		}

	}
	return
}

func createSubTable(subTableMaps map[string]*dataRows) {

	connection := getConnection()
	defer connection.Close()

	_, _ = connection.Exec("use " + db)

	createTablePrefix := "create table if not exists "
	var buffer bytes.Buffer
	for subTableName := range subTableMaps {

		superTableName := getSuperTableName(subTableMaps[subTableName].config.StName)
		firstRowValues := subTableMaps[subTableName].rows[0] // the first rows values as tags

		// create table t using superTable tags(...);
		for i := 0; i < hnum; i++ {
			tableName := getScaleSubTableName(subTableName, i)

			scaleTableMap[tableName] = &scaleTableInfo{
				subTableName: subTableName,
				insertRows:   0,
			}
			scaleTableNames = append(scaleTableNames, tableName)

			buffer.WriteString(createTablePrefix)
			buffer.WriteString(tableName)
			buffer.WriteString(" using ")
			buffer.WriteString(superTableName)
			buffer.WriteString(" tags(")
			for _, tag := range subTableMaps[subTableName].config.Tags {
				tagValue := fmt.Sprintf("%v", firstRowValues[strings.ToLower(tag.Name)])
				buffer.WriteString("'" + tagValue + "'")
				buffer.WriteString(",")
			}
			buffer.Truncate(buffer.Len() - 1)
			buffer.WriteString(")")

			createTableSql := buffer.String()
			buffer.Reset()

			//log.Printf("create table: %s\n", createTableSql)
			_, err := connection.Exec(createTableSql)
			if err != nil {
				log.Fatalf("create table error: %s\n", err)
			}
		}
	}
}

func createSuperTable(superTableConfigMap map[string]*superTableConfig) {

	connection := getConnection()
	defer connection.Close()

	if dropdb == 1 {
		dropDbSql := "drop database if exists " + db
		_, err := connection.Exec(dropDbSql) // drop database if exists
		if err != nil {
			log.Fatalf("drop database error: %s\n", err)
		}
		log.Printf("dropdb: %s\n", dropDbSql)
	}

	createDbSql := "create database if not exists " + db + " " + dbparam

	_, err := connection.Exec(createDbSql) // create database if not exists
	if err != nil {
		log.Fatalf("create database error: %s\n", err)
	}
	log.Printf("createDb: %s\n", createDbSql)

	_, _ = connection.Exec("use " + db)

	prefix := "create table if not exists "
	var buffer bytes.Buffer
	//CREATE TABLE <stable_name> (<field_name> TIMESTAMP, field_name1 field_type,…) TAGS(tag_name tag_type, …)
	for key := range superTableConfigMap {

		buffer.WriteString(prefix)
		buffer.WriteString(getSuperTableName(key))
		buffer.WriteString("(")

		superTableConf := superTableConfigMap[key]

		buffer.WriteString(superTableConf.config.Timestamp)
		buffer.WriteString(" timestamp, ")

		for _, field := range superTableConf.config.Fields {
			buffer.WriteString(field.Name + " " + field.Type + ",")
		}

		buffer.Truncate(buffer.Len() - 1)
		buffer.WriteString(") tags( ")

		for _, tag := range superTableConf.config.Tags {
			buffer.WriteString(tag.Name + " " + tag.Type + ",")
		}

		buffer.Truncate(buffer.Len() - 1)
		buffer.WriteString(")")

		createSql := buffer.String()
		buffer.Reset()

		//log.Printf("superTable: %s\n", createSql)
		_, err = connection.Exec(createSql)
		if err != nil {
			log.Fatalf("create supertable error: %s\n", err)
		}
	}

}

func getScaleSubTableName(subTableName string, hNum int) string {
	if hNum == 0 {
		return subTableName
	}
	return fmt.Sprintf("%s_%d", subTableName, hNum)
}

func getSuperTableName(stName string) string {
	return SuperTablePrefix + stName
}

/**
* normalizationData , and return the num of subTables
 */
func normalizationData(fileRows dataRows, minTime int64) int64 {

	var tableNum int64 = 0
	for _, row := range fileRows.rows {
		// get subTableName
		tableValue := getSubTableNameValue(row[fileRows.config.SubTableName])
		if len(tableValue) == 0 {
			continue
		}

		row[fileRows.config.Timestamp] = getPrimaryKey(row[fileRows.config.Timestamp]) - minTime

		subTableName := getSubTableName(tableValue, fileRows.config.StName)

		value, ok := subTableMap[subTableName]
		if !ok {
			subTableMap[subTableName] = &dataRows{
				rows:   []map[string]interface{}{row},
				config: fileRows.config,
			}

			tableNum++
		} else {
			value.rows = append(value.rows, row)
		}
	}
	return tableNum
}

// return the maximum table rows
func normalizationDataWithSameInterval(fileRows dataRows, avgInterval int64) int64 {
	// subTableMap
	currSubTableMap := make(map[string]*dataRows)
	for _, row := range fileRows.rows {
		// get subTableName
		tableValue := getSubTableNameValue(row[fileRows.config.SubTableName])
		if len(tableValue) == 0 {
			continue
		}

		subTableName := getSubTableName(tableValue, fileRows.config.StName)

		value, ok := currSubTableMap[subTableName]
		if !ok {
			row[fileRows.config.Timestamp] = 0
			currSubTableMap[subTableName] = &dataRows{
				rows:   []map[string]interface{}{row},
				config: fileRows.config,
			}
		} else {
			row[fileRows.config.Timestamp] = int64(len(value.rows)) * avgInterval
			value.rows = append(value.rows, row)
		}

	}

	var maxRows, tableRows = 0, 0
	for tableName := range currSubTableMap {
		tableRows = len(currSubTableMap[tableName].rows)
		subTableMap[tableName] = currSubTableMap[tableName] // add to global subTableMap
		if tableRows > maxRows {
			maxRows = tableRows
		}
	}

	return int64(maxRows)
}

func getSubTableName(subTableValue string, superTableName string) string {
	return SubTablePrefix + subTableValue + "_" + superTableName
}

func insertData(threadIndex, start, end int, wg *sync.WaitGroup, successRows []int64) {
	connection := getConnection()
	defer connection.Close()
	defer wg.Done()

	_, _ = connection.Exec("use " + db) // use db

	log.Printf("thread-%d start insert into [%d, %d) subtables.\n", threadIndex, start, end)

	num := 0
	subTables := scaleTableNames[start:end]
	var buffer bytes.Buffer
	for {
		var currSuccessRows int64
		var appendRows int
		var lastTableName string

		buffer.WriteString(InsertPrefix)

		for _, tableName := range subTables {

			subTableInfo := subTableMap[scaleTableMap[tableName].subTableName]
			subTableRows := int64(len(subTableInfo.rows))
			superTableConf := superTableConfigMap[subTableInfo.config.StName]

			tableStartTime := superTableConf.startTime
			var tableEndTime int64
			if vnum == 0 {
				// need continue generate data
				tableEndTime = time.Now().UnixNano() / 1e6
			} else {
				tableEndTime = tableStartTime + superTableConf.cycleTime*int64(vnum) - superTableConf.avgInterval
			}

			insertRows := scaleTableMap[tableName].insertRows

			for {
				loopNum := insertRows / subTableRows
				rowIndex := insertRows % subTableRows
				currentRow := subTableInfo.rows[rowIndex]

				currentTime := getPrimaryKey(currentRow[subTableInfo.config.Timestamp]) + loopNum*superTableConf.cycleTime + tableStartTime
				if currentTime <= tableEndTime {
					// append

					if lastTableName != tableName {
						buffer.WriteString(tableName)
						buffer.WriteString(" values")
					}
					lastTableName = tableName

					buffer.WriteString("(")
					buffer.WriteString(fmt.Sprintf("%v", currentTime))
					buffer.WriteString(",")

					for _, field := range subTableInfo.config.Fields {
						buffer.WriteString(getFieldValue(currentRow[strings.ToLower(field.Name)]))
						buffer.WriteString(",")
					}

					buffer.Truncate(buffer.Len() - 1)
					buffer.WriteString(") ")

					appendRows++
					insertRows++
					if appendRows == batch {
						// executeBatch
						insertSql := buffer.String()
						affectedRows := executeBatchInsert(insertSql, connection)

						successRows[threadIndex] += affectedRows
						currSuccessRows += affectedRows

						buffer.Reset()
						buffer.WriteString(InsertPrefix)
						lastTableName = ""
						appendRows = 0
					}
				} else {
					// finished insert current table
					break
				}
			}

			scaleTableMap[tableName].insertRows = insertRows

		}

		// left := len(rows)
		if appendRows > 0 {
			// executeBatch
			insertSql := buffer.String()
			affectedRows := executeBatchInsert(insertSql, connection)

			successRows[threadIndex] += affectedRows
			currSuccessRows += affectedRows

			buffer.Reset()
		}

		// log.Printf("thread-%d finished insert %d rows, used %d ms.", threadIndex, currSuccessRows, time.Since(threadStartTime)/1e6)

		if vnum != 0 {
			// thread finished insert data
			// log.Printf("thread-%d exit\n", threadIndex)
			break
		}

		if num == 0 {
			wg.Done() //finished insert history data
			num++
		}

		if currSuccessRows == 0 {
			// log.Printf("thread-%d start to sleep %d ms.", threadIndex, delay)
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}

		// need continue insert data
	}

}

func executeBatchInsert(insertSql string, connection *sql.DB) int64 {
	result, err := connection.Exec(insertSql)
	if err != nil {
		log.Printf("execute insertSql %s error, %s\n", insertSql, err)
		return 0
	}
	affected, _ := result.RowsAffected()
	if affected < 0 {
		affected = 0
	}
	return affected
}

func getFieldValue(fieldValue interface{}) string {
	return fmt.Sprintf("'%v'", fieldValue)
}

func getConnection() *sql.DB {
	db, err := sql.Open(DriverName, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}

func getSubTableNameValue(suffix interface{}) string {
	return fmt.Sprintf("%v", suffix)
}

func readFile(config dataImport.CaseConfig) dataRows {
	fileFormat := strings.ToLower(config.Format)
	if fileFormat == JsonFormat {
		return readJSONFile(config)
	} else if fileFormat == CsvFormat {
		return readCSVFile(config)
	}

	log.Printf("the file %s is not supported yet\n", config.FilePath)
	return dataRows{}
}

func readCSVFile(config dataImport.CaseConfig) dataRows {
	var rows dataRows
	f, err := os.Open(config.FilePath)
	if err != nil {
		log.Printf("Error: %s, %s\n", config.FilePath, err)
		return rows
	}
	defer f.Close()

	r := bufio.NewReader(f)

	//read the first line as title
	lineBytes, _, err := r.ReadLine()
	if err == io.EOF {
		log.Printf("the file %s is empty\n", config.FilePath)
		return rows
	}
	line := strings.ToLower(string(lineBytes))
	titles := strings.Split(line, config.Separator)
	if len(titles) < 3 {
		// need suffix、 primaryKey and at least one other field
		log.Printf("the first line of file %s should be title row, and at least 3 field.\n", config.FilePath)
		return rows
	}

	rows.config = config

	var lineNum = 0
	for {
		// read data row
		lineBytes, _, err = r.ReadLine()
		lineNum++
		if err == io.EOF {
			break
		}
		// fmt.Println(line)
		rowData := strings.Split(string(lineBytes), config.Separator)

		dataMap := make(map[string]interface{})
		for i, title := range titles {
			title = strings.TrimSpace(title)
			if i < len(rowData) {
				dataMap[title] = strings.TrimSpace(rowData[i])
			} else {
				dataMap[title] = ""
			}
		}

		// if the suffix valid
		if !existMapKeyAndNotEmpty(config.Timestamp, dataMap) {
			log.Printf("the Timestamp[%s] of line %d is empty, will filtered.\n", config.Timestamp, lineNum)
			continue
		}

		// if the primary key valid
		primaryKeyValue := getPrimaryKeyMilliSec(config.Timestamp, config.TimestampType, config.TimestampTypeFormat, dataMap)
		if primaryKeyValue == -1 {
			log.Printf("the Timestamp[%s] of line %d is not valid, will filtered.\n", config.Timestamp, lineNum)
			continue
		}

		dataMap[config.Timestamp] = primaryKeyValue

		rows.rows = append(rows.rows, dataMap)
	}
	return rows
}

func readJSONFile(config dataImport.CaseConfig) dataRows {

	var rows dataRows
	f, err := os.Open(config.FilePath)
	if err != nil {
		log.Printf("Error: %s, %s\n", config.FilePath, err)
		return rows
	}
	defer f.Close()

	r := bufio.NewReader(f)
	//log.Printf("file size %d\n", r.Size())

	rows.config = config
	var lineNum = 0
	for {
		lineBytes, _, err := r.ReadLine()
		lineNum++
		if err == io.EOF {
			break
		}

		line := make(map[string]interface{})
		err = json.Unmarshal(lineBytes, &line)

		if err != nil {
			log.Printf("line [%d] of file %s parse error, reason:  %s\n", lineNum, config.FilePath, err)
			continue
		}

		// transfer the key to lowercase
		lowerMapKey(line)

		if !existMapKeyAndNotEmpty(config.SubTableName, line) {
			log.Printf("the SubTableName[%s] of line %d is empty, will filtered.\n", config.SubTableName, lineNum)
			continue
		}

		primaryKeyValue := getPrimaryKeyMilliSec(config.Timestamp, config.TimestampType, config.TimestampTypeFormat, line)
		if primaryKeyValue == -1 {
			log.Printf("the Timestamp[%s] of line %d is not valid, will filtered.\n", config.Timestamp, lineNum)
			continue
		}

		line[config.Timestamp] = primaryKeyValue

		rows.rows = append(rows.rows, line)
	}

	return rows
}

/**
* get primary key as millisecond , otherwise return -1
 */
func getPrimaryKeyMilliSec(key string, valueType string, valueFormat string, line map[string]interface{}) int64 {
	if !existMapKeyAndNotEmpty(key, line) {
		return -1
	}
	if DATETIME == valueType {
		// transfer the datetime to milliseconds
		return parseMillisecond(line[key], valueFormat)
	}

	value, err := strconv.ParseInt(fmt.Sprintf("%v", line[key]), 10, 64)
	// as millisecond num
	if err != nil {
		return -1
	}
	return value
}

// parseMillisecond parse the dateStr to millisecond, return -1 if failed
func parseMillisecond(str interface{}, layout string) int64 {
	value, ok := str.(string)
	if !ok {
		return -1
	}

	t, err := time.ParseInLocation(layout, strings.TrimSpace(value), time.Local)

	if err != nil {
		log.Println(err)
		return -1
	}
	return t.UnixNano() / 1e6
}

// lowerMapKey transfer all the map key to lowercase
func lowerMapKey(maps map[string]interface{}) {
	for key := range maps {
		value := maps[key]
		delete(maps, key)
		maps[strings.ToLower(key)] = value
	}
}

func existMapKeyAndNotEmpty(key string, maps map[string]interface{}) bool {
	value, ok := maps[key]
	if !ok {
		return false
	}

	str, err := value.(string)
	if err && len(str) == 0 {
		return false
	}
	return true
}

func checkUserCaseConfig(caseName string, caseConfig *dataImport.CaseConfig) {

	if len(caseConfig.StName) == 0 {
		log.Fatalf("the stname of case %s can't be empty\n", caseName)
	}

	caseConfig.StName = strings.ToLower(caseConfig.StName)

	if len(caseConfig.Tags) == 0 {
		log.Fatalf("the tags of case %s can't be empty\n", caseName)
	}

	if len(caseConfig.Fields) == 0 {
		log.Fatalf("the fields of case %s can't be empty\n", caseName)
	}

	if len(caseConfig.SubTableName) == 0 {
		log.Fatalf("the suffix of case %s can't be empty\n", caseName)
	}

	caseConfig.SubTableName = strings.ToLower(caseConfig.SubTableName)

	caseConfig.Timestamp = strings.ToLower(caseConfig.Timestamp)

	var timestampExist = false
	for i, field := range caseConfig.Fields {
		if strings.EqualFold(field.Name, caseConfig.Timestamp) {
			if strings.ToLower(field.Type) != TIMESTAMP {
				log.Fatalf("case %s's primaryKey %s field type is %s, it must be timestamp\n", caseName, caseConfig.Timestamp, field.Type)
			}
			timestampExist = true
			if i < len(caseConfig.Fields)-1 {
				// delete middle item,  a = a[:i+copy(a[i:], a[i+1:])]
				caseConfig.Fields = caseConfig.Fields[:i+copy(caseConfig.Fields[i:], caseConfig.Fields[i+1:])]
			} else {
				// delete the last item
				caseConfig.Fields = caseConfig.Fields[:len(caseConfig.Fields)-1]
			}
			break
		}
	}

	if !timestampExist {
		log.Fatalf("case %s primaryKey %s is not exist in fields\n", caseName, caseConfig.Timestamp)
	}

	caseConfig.TimestampType = strings.ToLower(caseConfig.TimestampType)
	if caseConfig.TimestampType != MILLISECOND && caseConfig.TimestampType != DATETIME {
		log.Fatalf("case %s's timestampType %s error, only can be timestamp or datetime\n", caseName, caseConfig.TimestampType)
	}

	if caseConfig.TimestampType == DATETIME && len(caseConfig.TimestampTypeFormat) == 0 {
		log.Fatalf("case %s's timestampTypeFormat %s can't be empty when timestampType is datetime\n", caseName, caseConfig.TimestampTypeFormat)
	}

}

func parseArg() {
	flag.StringVar(&cfg, "cfg", "config/cfg.toml", "configuration file which describes useCase and data format.")
	flag.StringVar(&cases, "cases", "sensor_info", "useCase for dataset to be imported. Multiple choices can be separated by comma, for example, -cases sensor_info,camera_detection.")
	flag.IntVar(&hnum, "hnum", 100, "magnification factor of the sample tables. For example, if hnum is 100 and in the sample data there are 10 tables, then 10x100=1000 tables will be created in the database.")
	flag.IntVar(&vnum, "vnum", 1000, "copies of the sample records in each table. If set to 0，this program will never stop simulating and importing data even if the timestamp has passed current time.")
	flag.Int64Var(&delay, "delay", DefaultDelay, "the delay time interval(millisecond) to continue generating data when vnum set 0.")
	flag.Int64Var(&tick, "tick", 2000, "the tick time interval(millisecond) to print statistic info.")
	flag.IntVar(&save, "save", 0, "whether to save the statistical info into 'statistic' table. 0 is disabled and 1 is enabled.")
	flag.StringVar(&saveTable, "savetb", DefaultStatisticTable, "the table to save 'statistic' info when save set 1.")
	flag.IntVar(&thread, "thread", 10, "number of threads to import data.")
	flag.IntVar(&batch, "batch", 100, "rows of records in one import batch.")
	flag.IntVar(&auto, "auto", 0, "whether to use the startTime and interval specified by users when simulating the data. 0 is disabled and 1 is enabled.")
	flag.StringVar(&startTimeStr, "start", "", "the starting timestamp of simulated data, in the format of yyyy-MM-dd HH:mm:ss.SSS. If not specified, the earliest timestamp in the sample data will be set as the startTime.")
	flag.Int64Var(&interval, "interval", DefaultInterval, "time interval between two consecutive records, in the unit of millisecond. Only valid when auto is 1.")
	flag.StringVar(&host, "host", "127.0.0.1", "tdengine server ip.")
	flag.IntVar(&port, "port", 6030, "tdengine server port.")
	flag.StringVar(&user, "user", "root", "user name to login into the database.")
	flag.StringVar(&password, "password", "taosdata", "the import tdengine user password")
	flag.IntVar(&dropdb, "dropdb", 0, "whether to drop the existing database. 1 is yes and 0 otherwise.")
	flag.StringVar(&db, "db", "", "name of the database to store data.")
	flag.StringVar(&dbparam, "dbparam", "", "database configurations when it is created.")

	flag.Parse()
}

func printArg() {
	fmt.Println("used param: ")
	fmt.Println("-cfg: ", cfg)
	fmt.Println("-cases:", cases)
	fmt.Println("-hnum:", hnum)
	fmt.Println("-vnum:", vnum)
	fmt.Println("-delay:", delay)
	fmt.Println("-tick:", tick)
	fmt.Println("-save:", save)
	fmt.Println("-savetb:", saveTable)
	fmt.Println("-thread:", thread)
	fmt.Println("-batch:", batch)
	fmt.Println("-auto:", auto)
	fmt.Println("-start:", startTimeStr)
	fmt.Println("-interval:", interval)
	fmt.Println("-host:", host)
	fmt.Println("-port", port)
	fmt.Println("-user", user)
	fmt.Println("-password", password)
	fmt.Println("-dropdb", dropdb)
	fmt.Println("-db", db)
	fmt.Println("-dbparam", dbparam)
}
