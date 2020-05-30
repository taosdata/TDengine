package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/taosdata/TDengine/importSampleData/import"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/taosdata/TDengine/src/connector/go/taosSql"
)

const (
	TIMESTAMP  = "timestamp"
	DATETIME  = "datetime"
	MILLISECOND  = "millisecond"
	DEFAULT_STARTTIME int64 = -1
	DEFAULT_INTERVAL int64 = 1*1000
	DEFAULT_DELAY int64 = -1
	DEFAULT_STATISTIC_TABLE = "statistic"

	JSON_FORMAT = "json"
	CSV_FORMAT = "csv"
	SUPERTABLE_PREFIX = "s_"
	SUBTABLE_PREFIX = "t_"

	DRIVER_NAME = "taosSql"
	STARTTIME_LAYOUT = "2006-01-02 15:04:05.000"
	INSERT_PREFIX = "insert into "
)

var (

	cfg string
	cases   string
	hnum    int
	vnum  int
	thread  int
	batch  int
	auto		int
	starttimestr  string
	interval	int64
	host       string
	port       int
	user       string
	password   string
	dropdb   int
	db         string
	dbparam		string

	dataSourceName string
	startTime  int64

	superTableConfigMap  = make(map[string]*superTableConfig)
	subTableMap	= make(map[string]*dataRows)
	scaleTableNames []string

	scaleTableMap = make(map[string]*scaleTableInfo)

	successRows []int64
	lastStaticTime time.Time
	lastTotalRows int64
	timeTicker *time.Ticker
	delay int64  // default 10 milliseconds
	tick int64
	save int
	saveTable string
)

type superTableConfig struct {
	startTime int64
	endTime int64
	cycleTime int64
	avgInterval int64
	config dataimport.CaseConfig
}

type scaleTableInfo struct {
	scaleTableName string
	subTableName string
	insertRows int64
}

type tableRows struct {
	tableName string  // tableName
	value string      // values(...)
}

type dataRows struct {
	rows         []map[string]interface{}
	config       dataimport.CaseConfig
}

func (rows dataRows) Len() int {
	return len(rows.rows)
}

func (rows dataRows) Less(i, j int) bool {
	itime := getPrimaryKey(rows.rows[i][rows.config.Timestamp])
	jtime := getPrimaryKey(rows.rows[j][rows.config.Timestamp])
	return itime < jtime
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
	parseArg() //parse argument

	if db == "" {
		//db = "go"
		db = fmt.Sprintf("test_%s",time.Now().Format("20060102"))
	}

	if auto == 1 && len(starttimestr) == 0  {
		log.Fatalf("startTime must be set when auto is 1, the format is \"yyyy-MM-dd HH:mm:ss.SSS\" ")
	}

	if len(starttimestr) != 0 {
		t, err := time.ParseInLocation(STARTTIME_LAYOUT, strings.TrimSpace(starttimestr), time.Local)
		if err != nil {
			log.Fatalf("param startTime %s error, %s\n", starttimestr, err)
		}

		startTime = t.UnixNano() / 1e6 // as millisecond
	}else{
		startTime = DEFAULT_STARTTIME
	}

	dataSourceName = fmt.Sprintf("%s:%s@/tcp(%s:%d)/", user, password, host, port)

	printArg()

	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}

func main() {

	importConfig := dataimport.LoadConfig(cfg)

	var caseMinumInterval int64 = -1

	for _, userCase := range strings.Split(cases, ",") {
		caseConfig, ok := importConfig.UserCases[userCase]

		if !ok {
			log.Println("not exist case: ", userCase)
			continue
		}

		checkUserCaseConfig(userCase, &caseConfig)

		//read file as map array
		fileRows := readFile(caseConfig)
		log.Printf("case [%s] sample data file contains %d rows.\n", userCase, len(fileRows.rows))

		if len(fileRows.rows) == 0 {
			log.Printf("there is no valid line in file %s\n", caseConfig.FilePath)
			continue
		}

		_, exists := superTableConfigMap[caseConfig.Stname]
		if !exists {
			superTableConfigMap[caseConfig.Stname] = &superTableConfig{config:caseConfig}
		} else {
			log.Fatalf("the stname of case %s already exist.\n", caseConfig.Stname)
		}

		var start, cycleTime, avgInterval int64 = getSuperTableTimeConfig(fileRows)

		// set super table's startTime, cycleTime and avgInterval
		superTableConfigMap[caseConfig.Stname].startTime = start
		superTableConfigMap[caseConfig.Stname].avgInterval = avgInterval
		superTableConfigMap[caseConfig.Stname].cycleTime = cycleTime

		if caseMinumInterval == -1 || caseMinumInterval > avgInterval {
			caseMinumInterval = avgInterval
		}

		startStr := time.Unix(0, start*int64(time.Millisecond)).Format(STARTTIME_LAYOUT)
		log.Printf("case [%s] startTime %s(%d), average dataInterval %d ms, cycleTime %d ms.\n", userCase, startStr, start, avgInterval, cycleTime)
	}

	if DEFAULT_DELAY == delay {
		// default delay
		delay =  caseMinumInterval / 2
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

	//create sub table
	start = time.Now()
	createSubTable(subTableMap)
	log.Printf("create %d times of %d subtable ,all %d tables, used %d ms.\n", hnum, len(subTableMap), len(scaleTableMap), time.Since(start)/1e6)

	subTableNum := len(scaleTableMap)

	if subTableNum < thread {
		thread = subTableNum
	}

	filePerThread := subTableNum / thread
	leftFileNum := subTableNum % thread

	var wg  sync.WaitGroup

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

	usedTime :=  time.Since(start)

	total := getTotalRows(successRows)

	log.Printf("finished insert %d rows, used %d ms, speed %d rows/s", total, usedTime/1e6, total * 1e9 / int64(usedTime))

	if vnum == 0 {
		// continue waiting for insert data
		wait :=  make(chan string)
		v := <- wait
		log.Printf("program receive %s, exited.\n", v)
	}else{
		timeTicker.Stop()
	}

}

func staticSpeed(){

	connection := getConnection()
	defer connection.Close()

	if save == 1 {
		connection.Exec("use " + db)
		_, err := connection.Exec("create table if not exists " + saveTable +"(ts timestamp, speed int)")
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
		
		speed := currentSuccessRows * 1e9 / int64(usedTime)
		log.Printf("insert %d rows, used %d ms, speed %d rows/s", currentSuccessRows, usedTime/1e6, speed)

		if save == 1 {
			insertSql := fmt.Sprintf("insert into %s values(%d, %d)", saveTable, currentTime.UnixNano()/1e6, speed)
			connection.Exec(insertSql)
		}
		
		lastStaticTime = currentTime
		lastTotalRows = total
	}

}

func getTotalRows(successRows []int64) int64{
	var total int64 = 0
	for j := 0; j < len(successRows); j++ {
		total += successRows[j]
	}
	return total
}

func getSuperTableTimeConfig(fileRows dataRows) (start, cycleTime, avgInterval int64){
	if auto == 1 {
		// use auto generate data time
		start = startTime
		avgInterval = interval
		maxTableRows := normalizationDataWithSameInterval(fileRows, avgInterval)
		cycleTime = maxTableRows * avgInterval + avgInterval

	} else {

		// use the sample data primary timestamp
		sort.Sort(fileRows)// sort the file data by the primarykey
		minTime := getPrimaryKey(fileRows.rows[0][fileRows.config.Timestamp])
		maxTime := getPrimaryKey(fileRows.rows[len(fileRows.rows)-1][fileRows.config.Timestamp])

		start = minTime // default startTime use the minTime
		if DEFAULT_STARTTIME != startTime {
			start = startTime
		}

		tableNum := normalizationData(fileRows, minTime)

		if minTime == maxTime {
			avgInterval = interval
			cycleTime = tableNum * avgInterval + avgInterval
		}else{
			avgInterval = (maxTime - minTime) / int64(len(fileRows.rows)) * tableNum
			cycleTime = maxTime - minTime + avgInterval
		}
	
	}
	return
}

func createStatisticTable(){
	connection := getConnection()
	defer connection.Close()

	_, err := connection.Exec("create table if not exist " + db + "."+ saveTable +"(ts timestamp, speed int)")
	if err != nil {
		log.Fatalf("createStatisticTable error: %s\n", err)
	}
}

func createSubTable(subTableMaps map[string]*dataRows) {

	connection := getConnection()
	defer connection.Close()

	connection.Exec("use " + db)

	createTablePrefix := "create table if not exists "
	for subTableName := range subTableMaps {

		superTableName := getSuperTableName(subTableMaps[subTableName].config.Stname)
		tagValues := subTableMaps[subTableName].rows[0] // the first rows values as tags

		buffers := bytes.Buffer{}
		// create table t using supertTable tags(...);
		for i := 0; i < hnum; i++ {
			tableName := getScaleSubTableName(subTableName, i)

			scaleTableMap[tableName] = &scaleTableInfo{
				subTableName:   subTableName,
				insertRows: 0,
			}
			scaleTableNames = append(scaleTableNames, tableName)

			buffers.WriteString(createTablePrefix)
			buffers.WriteString(tableName)
			buffers.WriteString(" using ")
			buffers.WriteString(superTableName)
			buffers.WriteString(" tags(")
			for _, tag := range subTableMaps[subTableName].config.Tags{
				tagValue := fmt.Sprintf("%v", tagValues[strings.ToLower(tag.Name)])
				buffers.WriteString("'" + tagValue + "'")
				buffers.WriteString(",")
			}
			buffers.Truncate(buffers.Len()-1)
			buffers.WriteString(")")

			createTableSql := buffers.String()
			buffers.Reset()

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
		log.Printf("dropDb: %s\n", dropDbSql)
	}

	createDbSql := "create database if not exists " + db + " " + dbparam

	_, err := connection.Exec(createDbSql) // create database if not exists
	if err != nil {
		log.Fatalf("create database error: %s\n", err)
	}
	log.Printf("createDb: %s\n", createDbSql)

	connection.Exec("use " + db)

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

		buffer.Truncate(buffer.Len()-1)
		buffer.WriteString(") tags( ")

		for _, tag := range superTableConf.config.Tags {
			buffer.WriteString(tag.Name + " " + tag.Type + ",")
		}

		buffer.Truncate(buffer.Len()-1)
		buffer.WriteString(")")

		createSql := buffer.String()
		buffer.Reset()

		//log.Printf("supertable: %s\n", createSql)
		_, err = connection.Exec(createSql)
		if err != nil {
			log.Fatalf("create supertable error: %s\n", err)
		}
	}

}

func getScaleSubTableName(subTableName string, hnum int) string {
	if hnum == 0 {
		 return subTableName
	}
	return fmt.Sprintf(  "%s_%d", subTableName, hnum)
}

func getSuperTableName(stname string) string {
	return SUPERTABLE_PREFIX + stname
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

		subTableName := getSubTableName(tableValue, fileRows.config.Stname)

		value, ok := subTableMap[subTableName]
		if !ok {
			subTableMap[subTableName] = &dataRows{
				rows:         []map[string]interface{}{row},
				config:       fileRows.config,
			}

			tableNum++
		}else{
			value.rows = append(value.rows, row)
		}
	}
	return tableNum
}

// return the maximum table rows
func normalizationDataWithSameInterval(fileRows dataRows, avgInterval int64) int64{
	// subTableMap
	currSubTableMap	:= make(map[string]*dataRows)
	for _, row := range fileRows.rows {
		// get subTableName
		tableValue := getSubTableNameValue(row[fileRows.config.SubTableName])
		if len(tableValue) == 0 {
			continue
		}

		subTableName := getSubTableName(tableValue, fileRows.config.Stname)

		value, ok := currSubTableMap[subTableName]
		if !ok {
			row[fileRows.config.Timestamp] = 0
			currSubTableMap[subTableName] = &dataRows{
				rows:         []map[string]interface{}{row},
				config:       fileRows.config,
			}
		}else{
			row[fileRows.config.Timestamp] = int64(len(value.rows)) * avgInterval
			value.rows = append(value.rows, row)
		}

	}

	var maxRows, tableRows int = 0, 0
	for tableName := range currSubTableMap{
		tableRows = len(currSubTableMap[tableName].rows)
		subTableMap[tableName] = currSubTableMap[tableName] // add to global subTableMap
		if tableRows > maxRows {
			maxRows = tableRows
		}
	}

	return int64(maxRows)
}


func getSubTableName(subTableValue string, superTableName string)  string {
	return SUBTABLE_PREFIX + subTableValue + "_" + superTableName
}


func insertData(threadIndex, start, end int, wg  *sync.WaitGroup, successRows []int64) {
	connection := getConnection()
	defer connection.Close()
	defer wg.Done()

	connection.Exec("use " + db) // use db

	log.Printf("thread-%d start insert into [%d, %d) subtables.\n", threadIndex, start, end)

	num := 0
	subTables := scaleTableNames[start:end]
	for {
		var currSuccessRows int64
		var appendRows int
		var lastTableName string

		buffers := bytes.Buffer{}
		buffers.WriteString(INSERT_PREFIX)

		for _, tableName := range subTables {

			subTableInfo := subTableMap[scaleTableMap[tableName].subTableName]
			subTableRows := int64(len(subTableInfo.rows))
			superTableConf := superTableConfigMap[subTableInfo.config.Stname]

			tableStartTime := superTableConf.startTime
			var tableEndTime int64
			if vnum == 0 {
				// need continue generate data
				tableEndTime = time.Now().UnixNano()/1e6
			}else {	
				tableEndTime = tableStartTime + superTableConf.cycleTime * int64(vnum) - superTableConf.avgInterval
			}

			insertRows := scaleTableMap[tableName].insertRows

			for {
				loopNum := insertRows / subTableRows
				rowIndex := insertRows % subTableRows
				currentRow := subTableInfo.rows[rowIndex]

				currentTime := getPrimaryKey(currentRow[subTableInfo.config.Timestamp]) + loopNum * superTableConf.cycleTime + tableStartTime
				if currentTime <= tableEndTime {
					// append
					
					if lastTableName != tableName {
						buffers.WriteString(tableName)
						buffers.WriteString(" values")
					}
					lastTableName = tableName

					buffers.WriteString("(")
					buffers.WriteString(fmt.Sprintf("%v", currentTime))
					buffers.WriteString(",")
					
					// fieldNum := len(subTableInfo.config.Fields)
					for _,field := range subTableInfo.config.Fields {
						buffers.WriteString(getFieldValue(currentRow[strings.ToLower(field.Name)]))
						buffers.WriteString(",")
						// if( i != fieldNum -1){
							
						// }
					}

					buffers.Truncate(buffers.Len()-1)
					buffers.WriteString(") ")

					appendRows++
					insertRows++
					if  appendRows == batch {
						// executebatch
						insertSql := buffers.String()
						connection.Exec("use " + db)
						affectedRows := executeBatchInsert(insertSql, connection)

						successRows[threadIndex] += affectedRows
						currSuccessRows += affectedRows

						buffers.Reset()
						buffers.WriteString(INSERT_PREFIX)
						lastTableName = ""
						appendRows = 0
					}
				}else {
					// finished insert current table
					break
				}
			}

			scaleTableMap[tableName].insertRows = insertRows

		}
		
		// left := len(rows)
		if  appendRows > 0 {
			// executebatch
			insertSql := buffers.String()
			connection.Exec("use " + db)
			affectedRows := executeBatchInsert(insertSql, connection)
			
			successRows[threadIndex] += affectedRows
			currSuccessRows += affectedRows

			buffers.Reset()
		}

		// log.Printf("thread-%d finished insert %d rows, used %d ms.", threadIndex, currSuccessRows, time.Since(threadStartTime)/1e6)

		if vnum != 0 {
			// thread finished insert data
			// log.Printf("thread-%d exit\n", threadIndex)
			break
		}

		if(num == 0){
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

func buildSql(rows []tableRows) string{

	var lastTableName string

	buffers := bytes.Buffer{}

	for i, row := range rows {
		if i == 0 {
			lastTableName = row.tableName
			buffers.WriteString(INSERT_PREFIX)
			buffers.WriteString(row.tableName)
			buffers.WriteString(" values")
			buffers.WriteString(row.value)
			continue
		}

		if lastTableName == row.tableName {
			buffers.WriteString(row.value)
		}else {
			buffers.WriteString(" ")
			buffers.WriteString(row.tableName)
			buffers.WriteString(" values")
			buffers.WriteString(row.value)
			lastTableName = row.tableName
		}
	}

	inserSql := buffers.String()
	return inserSql
}

func buildRow(tableName string, currentTime int64, subTableInfo *dataRows, currentRow map[string]interface{}) tableRows{

	tableRows := tableRows{tableName: tableName}

	buffers := bytes.Buffer{}

	buffers.WriteString("(")
	buffers.WriteString(fmt.Sprintf("%v", currentTime))
	buffers.WriteString(",")

	for _,field := range subTableInfo.config.Fields {
		buffers.WriteString(getFieldValue(currentRow[strings.ToLower(field.Name)]))
		buffers.WriteString(",")
	}

	buffers.Truncate(buffers.Len()-1)
	buffers.WriteString(")")

	insertSql := buffers.String()
	tableRows.value = insertSql

	return tableRows
}

func executeBatchInsert(insertSql string, connection *sql.DB) int64 {
	result, error := connection.Exec(insertSql)
	if error != nil {
		log.Printf("execute insertSql %s error, %s\n", insertSql, error)
		return 0
	}
	affected, _ := result.RowsAffected()
	if affected < 0 {
		affected = 0
	}
	return affected
	// return 0
}

func getFieldValue(fieldValue interface{}) string {
	return fmt.Sprintf("'%v'", fieldValue)
}

func getConnection() *sql.DB{
	db, err := sql.Open(DRIVER_NAME, dataSourceName)
	if err != nil {
		panic(err)
	}
	return db
}


func getSubTableNameValue(suffix interface{}) string {
	return fmt.Sprintf("%v", suffix)
}

func hash(s string) int {
	v := int(crc32.ChecksumIEEE([]byte(s)))
	if v < 0 {
		return -v
	}
	return v
}

func readFile(config dataimport.CaseConfig) dataRows {
	fileFormat := strings.ToLower(config.Format)
	if fileFormat == JSON_FORMAT {
		return readJSONFile(config)
	} else if fileFormat == CSV_FORMAT {
		return readCSVFile(config)
	}

	log.Printf("the file %s is not supported yet\n", config.FilePath)
	return dataRows{}
}

func readCSVFile(config dataimport.CaseConfig) dataRows {
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
		// need suffix、 primarykey and at least one other field
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
		primaryKeyValue := getPrimaryKeyMillisec(config.Timestamp, config.TimestampType, config.TimestampTypeFormat, dataMap)
		if primaryKeyValue == -1 {
			log.Printf("the Timestamp[%s] of line %d is not valid, will filtered.\n", config.Timestamp, lineNum)
			continue
		}

		dataMap[config.Timestamp] = primaryKeyValue

		rows.rows = append(rows.rows, dataMap)
	}
	return rows
}

func readJSONFile(config dataimport.CaseConfig) dataRows {

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

		primaryKeyValue := getPrimaryKeyMillisec(config.Timestamp, config.TimestampType, config.TimestampTypeFormat, line)
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
func getPrimaryKeyMillisec(key string, valueType string, valueFormat string, line map[string]interface{}) int64 {
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
	return  t.UnixNano()/1e6
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

func checkUserCaseConfig(caseName string, caseConfig *dataimport.CaseConfig) {

	if len(caseConfig.Stname) == 0 {
		log.Fatalf("the stname of case %s can't be empty\n", caseName)
	}

	caseConfig.Stname = strings.ToLower(caseConfig.Stname)

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
			}else {
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
	flag.StringVar(&cfg, "cfg", "config/cfg.toml", "configuration file which describes usecase and data format.")
	flag.StringVar(&cases, "cases", "sensor_info", "usecase for dataset to be imported. Multiple choices can be separated by comma, for example, -cases sensor_info,camera_detection.")
	flag.IntVar(&hnum, "hnum", 100, "magnification factor of the sample tables. For example, if hnum is 100 and in the sample data there are 10 tables, then 10x100=1000 tables will be created in the database.")
	flag.IntVar(&vnum, "vnum", 1000, "copies of the sample records in each table. If set to 0，this program will never stop simulating and importing data even if the timestamp has passed current time.")
	flag.Int64Var(&delay, "delay", DEFAULT_DELAY, "the delay time interval(millisecond) to continue generating data when vnum set 0.")
	flag.Int64Var(&tick, "tick", 2000, "the tick time interval(millisecond) to print statistic info.")
	flag.IntVar(&save, "save", 0, "whether to save the statistical info into 'statistic' table. 0 is disabled and 1 is enabled.")
	flag.StringVar(&saveTable, "savetb", DEFAULT_STATISTIC_TABLE, "the table to save 'statistic' info when save set 1.")
	flag.IntVar(&thread, "thread", 10, "number of threads to import data.")
	flag.IntVar(&batch, "batch", 100, "rows of records in one import batch.")
	flag.IntVar(&auto, "auto", 0, "whether to use the starttime and interval specified by users when simulating the data. 0 is disabled and 1 is enabled.")
	flag.StringVar(&starttimestr, "start", "", "the starting timestamp of simulated data, in the format of yyyy-MM-dd HH:mm:ss.SSS. If not specified, the ealiest timestamp in the sample data will be set as the starttime.")
	flag.Int64Var(&interval, "interval", DEFAULT_INTERVAL, "time inteval between two consecutive records, in the unit of millisecond. Only valid when auto is 1.")
	flag.StringVar(&host, "host", "127.0.0.1", "tdengine server ip.")
	flag.IntVar(&port, "port", 6030, "tdengine server port.")
	flag.StringVar(&user, "user", "root", "user name to login into the database.")
	flag.StringVar(&password, "password", "taosdata", "the import tdengine user password")
	flag.IntVar(&dropdb, "dropdb", 0, "whether to drop the existing datbase. 1 is yes and 0 otherwise.")
	flag.StringVar(&db, "db", "", "name of the database to store data.")
	flag.StringVar(&dbparam, "dbparam", "", "database configurations when it is created.")

	flag.Parse()
}

func printArg()  {
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
	fmt.Println("-start:", starttimestr)
	fmt.Println("-interval:", interval)
	fmt.Println("-host:", host)
	fmt.Println("-port", port)
	fmt.Println("-user", user)
	fmt.Println("-password", password)
	fmt.Println("-dropdb", dropdb)
	fmt.Println("-db", db)
	fmt.Println("-dbparam", dbparam)
}
