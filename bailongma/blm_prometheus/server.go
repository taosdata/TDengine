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
	"container/list"
	"crypto/md5"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	_ "github.com/taosdata/driver-go/taosSql"

	"github.com/prometheus/prometheus/prompb"
)

type Bailongma struct {
}

type nametag struct {
	tagmap   map[string]string
	taglist  *list.List
	annotlen int
}

var (
	daemonIP      string
	daemonName    string
	httpworkers   int
	sqlworkers    int
	batchSize     int
	buffersize    int
	dbname        string
	dbuser        string
	dbpassword    string
	rwport        string
	apiport       string
	debugprt      int
	taglen        int
	taglimit      int = 1024
	tagnumlimit   int
	tablepervnode int
)

// Global vars
var (
	bufPool         sync.Pool
	batchChans      []chan string              //multi table one chan
	nodeChans       []chan prompb.WriteRequest //multi node one chan
	inputDone       chan struct{}
	workersGroup    sync.WaitGroup
	reportTags      [][2]string
	reportHostname  string
	taosDriverName  string = "taosSql"
	IsSTableCreated sync.Map
	IsTableCreated  sync.Map
	tagstr          string
	blmLog          *log.Logger
	tdurl           string
	logNameDefault  string = "/var/log/taos/blm_prometheus.log"
)
var scratchBufPool = &sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, 1024)
	},
}

type FieldDescriptiion struct {
	fname   string
	ftype   string
	flength int
	fnote   string
}
type tableStruct struct {
	status string
	head   []string
	data   []FieldDescriptiion
	rows   int64
}

// Parse args:
func init() {
	flag.StringVar(&daemonIP, "tdengine-ip", "127.0.0.1", "TDengine host IP.")
	flag.StringVar(&daemonName, "tdengine-name", "", "TDengine host Name. in K8S, could be used to lookup TDengine's IP")
	flag.StringVar(&apiport, "tdengine-api-port", "6020", "TDengine restful API port")
	flag.IntVar(&batchSize, "batch-size", 100, "Batch size (input items).")
	flag.IntVar(&httpworkers, "http-workers", 1, "Number of parallel http requests handler .")
	flag.IntVar(&sqlworkers, "sql-workers", 1, "Number of parallel sql handler.")
	flag.StringVar(&dbname, "dbname", "prometheus", "Database name where to store metrics")
	flag.StringVar(&dbuser, "dbuser", "root", "User for host to send result metrics")
	flag.StringVar(&dbpassword, "dbpassword", "taosdata", "User password for Host to send result metrics")
	flag.StringVar(&rwport, "port", "10203", "remote write port")
	flag.IntVar(&debugprt, "debugprt", 0, "if 0 not print, if 1 print the sql")
	flag.IntVar(&taglen, "tag-length", 50, "the max length of tag string,default is 30")
	flag.IntVar(&buffersize, "buffersize", 100, "the buffer size of metrics received")
	flag.IntVar(&tagnumlimit, "tag-num", 16, "the number of tags in a super table, default is 8")
	flag.IntVar(&tablepervnode, "table-num", 10000, "the number of tables per TDengine Vnode can create, default 10000")

	flag.Parse()

	if daemonName != "" {
		s, _ := net.LookupIP(daemonName)
		daemonIP = fmt.Sprintf("%s", s[0])

		fmt.Println(daemonIP)
		fmt.Println(s[0])
		daemonIP = daemonIP + ":0"

		tdurl = daemonName
	} else {
		tdurl = daemonIP
		daemonIP = daemonIP + ":0"
	}

	tagstr = fmt.Sprintf(" binary(%d)", taglen)
	logFile, err := os.OpenFile(logNameDefault, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	blmLog = log.New(logFile, "", log.LstdFlags)
	blmLog.SetPrefix("BLM_PRM")
	blmLog.SetFlags(log.LstdFlags | log.Lshortfile)
	blmLog.Printf("host: ip")
	blmLog.Printf(daemonIP)
	blmLog.Printf("  port: ")
	blmLog.Printf(rwport)
	blmLog.Printf("  database: ")
	blmLog.Println(dbname)

}

func main() {

	for i := 0; i < httpworkers; i++ {
		nodeChans = append(nodeChans, make(chan prompb.WriteRequest, buffersize))
	}

	createDatabase(dbname)

	for i := 0; i < httpworkers; i++ {
		workersGroup.Add(1)
		go NodeProcess(i)
	}

	for i := 0; i < sqlworkers; i++ {
		batchChans = append(batchChans, make(chan string, batchSize))
	}

	for i := 0; i < sqlworkers; i++ {
		workersGroup.Add(1)
		go processBatches(i)
	}

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		addr := strings.Split(r.RemoteAddr, ":")
		idx := TAOShashID([]byte(addr[0]))

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		r.Body.Close()
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		nodeChans[idx%httpworkers] <- req
		
	})
	http.HandleFunc("/check", func(w http.ResponseWriter, r *http.Request) {

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		//blmLog.Println(string(compressed))
		var output string = ""
		schema, ok := IsSTableCreated.Load(string(compressed))
		if !ok {
			output = "the stable is not created!"
		} else {
			ntag := schema.(nametag)
			tbtaglist := ntag.taglist
			tbtagmap := ntag.tagmap
			//annotlen := ntag.annotlen
			output = "tags: "
			for e := tbtaglist.Front(); e != nil; e = e.Next() {
				output = output + e.Value.(string) + " | "
			}
			output = output + "\ntagmap: "
			s := fmt.Sprintln(tbtagmap)
			output = output + s
		}

		res := queryTableStruct(string(compressed))
		output = output + "\nTable structure:\n" + res
		s := fmt.Sprintf("query result:\n %s\n", output)
		//blmLog.Println(s)
		w.Write([]byte(s))
		w.WriteHeader(http.StatusOK)
	})
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	if debugprt == 5 {
		TestSerialization()
	}
	blmLog.Fatal(http.ListenAndServe(":"+rwport, nil))

}

func queryTableStruct(tbname string) string {
	client := new(http.Client)
	s := fmt.Sprintf("describe %s.%s", dbname, tbname)
	body := strings.NewReader(s)
	req, _ := http.NewRequest("GET", "http://"+tdurl+":"+apiport+"/rest/sql", body)
	//fmt.Println("http://" + tdurl + ":" + apiport + "/rest/sql" + s)
	req.SetBasicAuth(dbuser, dbpassword)
	resp, err := client.Do(req)

	if err != nil {
		blmLog.Println(err)
		fmt.Println(err)
		return ""
	} else {
		compressed, _ := ioutil.ReadAll(resp.Body)
		defer resp.Body.Close()
		return string(compressed)
	}
}

func TAOShashID(ba []byte) int {
	var sum int = 0
	for i := 0; i < len(ba); i++ {
		sum += int(ba[i] - '0')
	}
	return sum
}

func NodeProcess(workerid int) error {

	for req := range nodeChans[workerid] {

		ProcessReq(req)

	}

	return nil
}

func ProcessReq(req prompb.WriteRequest) error {
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonIP+")/"+dbname)
	if err != nil {
		blmLog.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()

	for _, ts := range req.Timeseries {
		err = HandleStable(ts, db)
	}
	return err
}

func HandleStable(ts *prompb.TimeSeries, db *sql.DB) error {
	taglist := list.New()
	tbtaglist := list.New()
	tagmap := make(map[string]string)
	tbtagmap := make(map[string]string)
	m := make(model.Metric, len(ts.Labels))
	tagnum := tagnumlimit
	var hasName bool = false
	var metricsName string
	var tbn string = ""
	var nt nametag
	//var annotlen int
	//fmt.Println(ts)
	j := 0
	for _, l := range ts.Labels {
		m[model.LabelName(l.Name)] = model.LabelValue(l.Value)

		if string(l.Name) == "__name__" {
			metricsName = string(l.Value)
			tbn += metricsName
			hasName = true
			continue
		}
		j++
		ln := strings.ToLower(string(l.Name))
		OrderInsertS(ln, taglist)
		//taglist.PushBack(ln)
		s := string(l.Value)
		tbn += s
		//tagHash += s
		if j <= tagnum {
			OrderInsertS(ln, tbtaglist)
			//tbtaglist.PushBack(ln)
			if len(s) > taglen {
				s = s[:taglen]
			}
			tbtagmap[ln] = "y"
		}
		tagmap[ln] = s
	}

	if debugprt == 2 {
		t := ts.Samples[0].Timestamp
		var ns int64 = 0
		if t/1000000000 > 10 {
			tm := t / 1000
			ns = t - tm*1000
		}
		blmLog.Printf(" Ts: %s, value: %f, ", time.Unix(t/1000, ns), ts.Samples[0].Value)
		blmLog.Println(ts)
	}

	if !hasName {
		info := fmt.Sprintf("no name metric")
		panic(info)
	}
	stbname := tablenameEscape(metricsName)
	var ok bool
	schema, ok := IsSTableCreated.Load(stbname)
	if !ok { // no local record of super table structure
		nt.taglist = tbtaglist
		nt.tagmap = tbtagmap
		stbdescription := queryTableStruct(stbname) //query the super table from TDengine
		var stbst map[string]interface{}
		err := json.Unmarshal([]byte(stbdescription), &stbst)
		if err == nil { //query tdengine table success!
			status := stbst["status"].(string)
			if status == "succ" { //yes, the super table was already created in TDengine
				taostaglist := list.New()
				taostagmap := make(map[string]string)
				dt := stbst["data"]
				for _, fd := range dt.([]interface{}) {
					fdc := fd.([]interface{})
					if fdc[3].(string) == "tag" && fdc[0].(string) != "taghash" {
						tmpstr := fdc[0].(string)
						taostaglist.PushBack(tmpstr[2:])
						taostagmap[tmpstr[2:]] = "y"
					}
				}
				nt.taglist = taostaglist
				nt.tagmap = taostagmap
				tbtaglist = nt.taglist
				tbtagmap = nt.tagmap
				//	annotlen = ntag.annotlen
				var sqlcmd string
				i := 0
				for _, l := range ts.Labels {
					k := strings.ToLower(string(l.Name))
					if k == "__name__" {
						continue
					}
					i++
					if i < tagnumlimit {
						_, ok := tbtagmap[k]
						if !ok {
							sqlcmd = "alter table " + stbname + " add tag t_" + k + tagstr + "\n"
							_, err := execSql(dbname, sqlcmd, db)
							if err != nil {
								blmLog.Println(err)
								errorcode := fmt.Sprintf("%s", err)
								if strings.Contains(errorcode, "duplicated column names") {
									tbtaglist.PushBack(k)
									//OrderInsertS(k, tbtaglist)
									tbtagmap[k] = "y"
								}
							} else {
								tbtaglist.PushBack(k)
								//OrderInsertS(k, tbtaglist)
								tbtagmap[k] = "y"
							}
						}
					}
				}
				IsSTableCreated.Store(stbname, nt)
			} else { // no, the super table haven't been created in TDengine, create it.
				var sqlcmd string
				sqlcmd = "create table if not exists " + stbname + " (ts timestamp, value double) tags(taghash binary(34)"
				for e := tbtaglist.Front(); e != nil; e = e.Next() {
					sqlcmd = sqlcmd + ",t_" + e.Value.(string) + tagstr
				}
				//annotlen = taglimit - i*taglen
				//nt.annotlen = annotlen
				//annotationstr := fmt.Sprintf(" binary(%d)", annotlen)
				//sqlcmd = sqlcmd + ", annotation " + annotationstr + ")\n"
				sqlcmd = sqlcmd + ")\n"
				_, err := execSql(dbname, sqlcmd, db)
				if err == nil {
					IsSTableCreated.Store(stbname, nt)
				} else {
					blmLog.Println(err)
				}
			}
		} else { //query TDengine table error
			blmLog.Println(err)
		}
	} else {
		ntag := schema.(nametag)
		tbtaglist = ntag.taglist
		tbtagmap = ntag.tagmap
		//	annotlen = ntag.annotlen
		var sqlcmd string
		i := 0
		for _, l := range ts.Labels {
			k := strings.ToLower(string(l.Name))
			if k == "__name__" {
				continue
			}
			i++
			if i < tagnumlimit {
				_, ok := tbtagmap[k]
				if !ok {
					sqlcmd = "alter table " + stbname + " add tag t_" + k + tagstr + "\n"
					_, err := execSql(dbname, sqlcmd, db)
					if err != nil {
						blmLog.Println(err)
						errorcode := fmt.Sprintf("%s", err)
						if strings.Contains(errorcode, "duplicated column names") {
							tbtaglist.PushBack(k)
							//OrderInsertS(k, tbtaglist)
							tbtagmap[k] = "y"
						}
					} else {
						tbtaglist.PushBack(k)
						//OrderInsertS(k, tbtaglist)
						tbtagmap[k] = "y"
					}
				}
			}
		}
	}

	tbnhash := "MD5_" + md5V2(tbn)
	_, tbcreated := IsTableCreated.Load(tbnhash)

	if !tbcreated {
		var sqlcmdhead, sqlcmd string
		sqlcmdhead = "create table if not exists " + tbnhash + " using " + stbname + " tags(\""
		sqlcmd = ""
		i := 0
		for e := tbtaglist.Front(); e != nil; e = e.Next() {
			tagvalue, has := tagmap[e.Value.(string)]
			if len(tagvalue) > taglen {
				tagvalue = tagvalue[:taglen]
			}
			if i == 0 {
				if has {
					sqlcmd = sqlcmd + "\"" + tagvalue + "\""
				} else {
					sqlcmd = sqlcmd + "null"
				}
				i++
			} else {
				if has {
					sqlcmd = sqlcmd + ",\"" + tagvalue + "\""
				} else {
					sqlcmd = sqlcmd + ",null"
				}
			}

		}
		/*
			var annotation string = ""
			for t := taglist.Front(); t != nil; t = t.Next() {
				_, has := tbtagmap[t.Value.(string)]
				if !has {
					tagvalue, _ := tagmap[t.Value.(string)]
					annotation += t.Value.(string) + "=" + tagvalue + ","
				}
			}
			if len(annotation) > annotlen-2 {
				annotation = annotation[:annotlen-2]
			}*/
		//sqlcmd = sqlcmd + ",\"" + annotation + "\")\n"
		var keys []string
		var tagHash = ""
		for t := range tagmap {
			keys = append(keys, t)
		}
		sort.Strings(keys)
		for _, k := range keys {
			tagHash += tagmap[k]
		}

		sqlcmd = sqlcmd + ")\n"
		sqlcmd = sqlcmdhead + md5V2(tagHash) + "\"," + sqlcmd
		_, err := execSql(dbname, sqlcmd, db)
		if err == nil {
			IsTableCreated.Store(tbnhash, true)
		}
	}
	serilizeTDengine(ts, tbnhash, db)
	return nil
}

func tablenameEscape(mn string) string {
	stbb := strings.ReplaceAll(mn, ":", "_") // replace : in the metrics name to adapt the TDengine
	stbc := strings.ReplaceAll(stbb, ".", "_")
	stbname := strings.ReplaceAll(stbc, "-", "_")
	if len(stbname) > 60 {
		stbname = stbname[:60]
	}
	return stbname
}

func serilizeTDengine(m *prompb.TimeSeries, tbn string, db *sql.DB) error {
	idx := TAOShashID([]byte(tbn))
	sqlcmd := " " + tbn + " values("
	vl := m.Samples[0].GetValue()
	vls := strconv.FormatFloat(vl, 'E', -1, 64)

	if vls == "NaN" {
		vls = "null"
	}
	tl := m.Samples[0].GetTimestamp()
	tls := strconv.FormatInt(tl, 10)
	sqlcmd = sqlcmd + tls + "," + vls + ")\n"
	batchChans[idx%sqlworkers] <- sqlcmd
	return nil
}

func createDatabase(dbname string) {
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonIP+")/")
	if err != nil {
		log.Fatalf("Open database error: %s\n", err)
	}
	defer db.Close()
	sqlcmd := fmt.Sprintf("create database if not exists %s tables %d", dbname, tablepervnode)
	_, err = db.Exec(sqlcmd)
	sqlcmd = fmt.Sprintf("use %s", dbname)
	_, err = db.Exec(sqlcmd)
	checkErr(err)
	return
}

func execSql(dbname string, sqlcmd string, db *sql.DB) (sql.Result, error) {
	if len(sqlcmd) < 1 {
		return nil, nil
	}
	if debugprt == 2 {
		blmLog.Println(sqlcmd)
	}
	res, err := db.Exec(sqlcmd)
	if err != nil {
		blmLog.Printf("execSql Error: %s sqlcmd: %s\n", err, sqlcmd)

	}
	return res, err
}

func checkErr(err error) {
	if err != nil {
		blmLog.Println(err)
	}
}

func md5V2(str string) string {
	data := []byte(str)
	has := md5.Sum(data)
	md5str := fmt.Sprintf("%x", has)
	return md5str
}

func processBatches(iworker int) {
	var i int
	db, err := sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonIP+")/"+dbname)
	if err != nil {
		blmLog.Printf("processBatches Open database error: %s\n", err)
		var count int = 5
		for {
			if err != nil && count > 0 {
				<-time.After(time.Second * 1)
				_, err = sql.Open(taosDriverName, dbuser+":"+dbpassword+"@/tcp("+daemonIP+")/"+dbname)
				count--
			} else {
				if err != nil {
					blmLog.Printf("processBatches Error: %s open database\n", err)
					return
				}
				break
			}
		}
	}
	defer db.Close()
	sqlcmd := make([]string, batchSize+1)
	i = 0
	sqlcmd[i] = "Import into"
	i++
	//blmLog.Printf("processBatches")
	for onepoint := range batchChans[iworker] {
		sqlcmd[i] = onepoint
		i++
		if i > batchSize {
			i = 1
			//blmLog.Printf(strings.Join(sqlcmd, ""))
			_, err := db.Exec(strings.Join(sqlcmd, ""))
			if err != nil {
				blmLog.Printf("processBatches error %s", err)
				var count int = 2
				for {
					if err != nil && count > 0 {
						<-time.After(time.Second * 1)
						_, err = db.Exec(strings.Join(sqlcmd, ""))
						count--
					} else {
						if err != nil {
							blmLog.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
						}
						break
					}

				}
			}
		}
	}
	if i > 1 {
		i = 1
		//		blmLog.Printf(strings.Join(sqlcmd, ""))
		_, err := db.Exec(strings.Join(sqlcmd, ""))
		if err != nil {
			var count int = 2
			for {
				if err != nil && count > 0 {
					<-time.After(time.Second * 1)
					_, err = db.Exec(strings.Join(sqlcmd, ""))
					count--
				} else {
					if err != nil {
						blmLog.Printf("Error: %s sqlcmd: %s\n", err, strings.Join(sqlcmd, ""))
					}
					break
				}
			}
		}
	}

	workersGroup.Done()
}

func TestSerialization() {
	var req prompb.WriteRequest
	var ts []*prompb.TimeSeries
	var tse prompb.TimeSeries
	var sample *prompb.Sample
	var label prompb.Label
	var lbs []*prompb.Label
	promPath, err := os.Getwd()
	if err != nil {
		fmt.Printf("can't get current dir :%s \n", err)
		os.Exit(1)
	}
	promPath = filepath.Join(promPath, "testData/blm_prometheus.log")
	testfile, err := os.OpenFile(promPath, os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("Open file error!", err)
		return
	}
	defer testfile.Close()
	fmt.Println(promPath)
	buf := bufio.NewReader(testfile)
	i := 0
	lasttime := "20:40:20"
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				fmt.Println("File read ok! line:", i)
				break
			} else {
				fmt.Println("Read file error!", err)
				return
			}
		}
		sa := strings.Split(line, " ")

		if strings.Contains(line, "server.go:201:") {
			if sa[3] != lasttime {
				nodeChans[0] <- req
				lasttime = sa[3]
				req.Timeseries = req.Timeseries[:0]
				ts = ts[:0]
			}
			tse.Samples = make([]prompb.Sample, 0)
			T, _ := strconv.ParseInt(sa[7][:(len(sa[7])-1)], 10, 64)
			V, _ := strconv.ParseFloat(sa[9][:(len(sa[9])-1)], 64)
			sample = &prompb.Sample{
				Value:     V,
				Timestamp: T,
			}
			tse.Samples = append(tse.Samples, *sample)
		} else if strings.Contains(line, "server.go:202:") {
			lbs = make([]*prompb.Label, 0)
			lb := strings.Split(line[45:], "{")
			label.Name = "__name__"
			label.Value = lb[0]
			lbs = append(lbs, &label)
			lbc := strings.Split(lb[1][:len(lb[1])-1], ", ")
			for i = 0; i < len(lbc); i++ {
				content := strings.Split(lbc[i], "=\"")
				label.Name = content[0]
				if i == len(lbc)-1 {
					label.Value = content[1][:len(content[1])-2]
				} else {

					label.Value = content[1][:len(content[1])-1]
				}
				lbs = append(lbs, &label)
			}
			tse.Labels = lbs
			ts = append(ts, &tse)
			req.Timeseries = ts
		}

	}

}

func TAOSstrCmp(a string, b string) bool {
	//return if a locates before b in a dictrionary.
	for i := 0; i < len(a) && i < len(b); i++ {
		if int(a[i]-'0') > int(b[i]-'0') {
			return false
		} else if int(a[i]-'0') < int(b[i]-'0') {
			return true
		}
	}
	if len(a) > len(b) {
		return false
	} else {
		return true
	}
}

func OrderInsertS(s string, l *list.List) {
	e := l.Front()
	if e == nil {
		l.PushFront(s)
		return
	}

	for e = l.Front(); e != nil; e = e.Next() {
		str := e.Value.(string)

		if TAOSstrCmp(str, s) {
			continue
		} else {
			l.InsertBefore(s, e)
			return
		}
	}
	l.PushBack(s)
	return
}
