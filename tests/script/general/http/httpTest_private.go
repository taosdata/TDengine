package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"flag"
)

var (
	token    string
	url      string
	config   Config
	request  int64
	period   int64
	errorNum int64
)

type Config struct {
	HostIp       string `json:"hostIp"`
	ConnNum      int    `json:"connNum"`
	InsertModel  string `json:"insertModel"`
	WaitTime     int    `json:"waitTime"`
	TableDesc    string `json:"tableDesc"`
	TablePrefix  string `json:"tablePrefix"`
	TablePerConn int    `json:"tablePerConn"`
	TableCreate  bool   `json:"tableCreate"`
	TableStart   int    `json:"tableStart"`
	DbName       string `json:"dbName"`
	DbReplica    int    `json:"dbReplica"`
	DbKeep       int    `json:"dbKeep"`
	DbDays       int    `json:"dbDays"`
	MetricsName  string `json:"metricsName"`
	TagNum       int    `json:"tagNum"`
	DataNum      int    `json:"dataNum"`
	DataBegin    int64  `json:"dataBegin"`
	DataInterval int    `json:"dataInterval"`
	DataBatch    int    `json:"dataBatch"`
	DataInsert   bool   `json:"dataInsert"`
	DataRandom   bool   `json:"dataRandom"`
}

type TokenResult struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
	Desc   string `json:"desc"`
}

type JsonResult struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
}

func readFile(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		println("taos_cloud.json not found")
		panic(err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	err = dec.Decode(&config)
	if err != nil {
		println("taos_cloud.json parse error")
		panic(err)
	}

	if config.TagNum <= 0 {
		config.TagNum = 1
	}
	request = 0
	period = 0
	errorNum = 0

	fmt.Println("================config parameters======================")
	fmt.Println("HostIp:", config.HostIp)
	fmt.Println("connNum:", config.ConnNum)
	fmt.Println("insertModel:", config.InsertModel)
	fmt.Println("waitTime:", config.WaitTime)
	fmt.Println("tableDesc:", config.TableDesc)
	fmt.Println("tablePrefix:", config.TablePrefix)
	fmt.Println("tablePerConn:", config.TablePerConn)
	fmt.Println("tableCreate:", config.TableCreate)
	fmt.Println("tableStart:", config.TableStart)
	fmt.Println("dbName:", config.DbName)
	fmt.Println("dbReplica:", config.DbReplica)
	fmt.Println("dbKeep:", config.DbKeep)
	fmt.Println("dbDays:", config.DbDays)
	fmt.Println("metricsName:", config.MetricsName)
	fmt.Println("tagNum:", config.TagNum)
	fmt.Println("dataNum:", config.DataNum)
	fmt.Println("dataBegin:", config.DataBegin)
	fmt.Println("dataInterval:", config.DataInterval)
	fmt.Println("dataBatch:", config.DataBatch)
	fmt.Println("dataInsert:", config.DataInsert)
	fmt.Println("dataRandom:", config.DataRandom)

	fmt.Println("================http token=============================")
	token, err = getToken()
	url = fmt.Sprintf("http://%s:%d/rest/sql", config.HostIp, 6020)

	fmt.Println("httpToken:", token)
	fmt.Println("httpUrl:", url)

	if err != nil {
		panic(err)
	}
}

func getToken() (string, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d/rest/login/%s/%s", config.HostIp, 6020, "root", "taosdata"))
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

func exec(client *http.Client, sql string) {
	for times := 0; times < 1; times++ {

		req, err1 := http.NewRequest("POST", url, bytes.NewReader([]byte(sql)))
		if err1 != nil {
			continue
		}
		req.Header.Add("Authorization", "Taosd "+token)

		begin := time.Now()
		resp, err := client.Do(req)

		if err != nil {
			continue
		}

		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			resp.Body.Close()
			continue
		}

		spend := (time.Since(begin).Nanoseconds())

		var jsonResult JsonResult
		err = json.Unmarshal(data, &jsonResult)
		if err != nil {
			resp.Body.Close()
			continue
		}

		if jsonResult.Status != "succ" {
			resp.Body.Close()
			continue
		}
		atomic.AddInt64(&request, 1)
		if (request < 103) {
			return
		}
		
		atomic.AddInt64(&period, spend)
		if request%5000 == 0 && request != 0 {
			requestAvg := float64(period) / float64(1000000) / float64(request)
			qps := float64(1000) / float64(requestAvg) * float64(config.ConnNum)
			dps := qps * float64(config.DataBatch)
			fmt.Println("====== req:", request, ", error:", errorNum, ", qps:", int64(qps), ", wait:", int64(requestAvg), "ms", ", data per second:", int64(dps))
		}
		return
	}
	fmt.Println("xxxx>sql:", sql, ", retryTimes:", 10)
	errorNum++
}

func createDb() {
	if !config.TableCreate {
		return
	}

	client := &http.Client{}

	fmt.Println("\n================create database =====================")
	sql := fmt.Sprintf("create database %s keep %d", config.DbName, config.DbKeep)
	exec(client, sql)
}

func createTb() {
	if !config.TableCreate {
		return
	}
	client := &http.Client{}

	fmt.Println("\n================create table ========================")
	sql := fmt.Sprintf("create table %s.%s(%s) tags (orgno int)", config.DbName, config.MetricsName, config.TableDesc)
	exec(client, sql)
	tbNum := config.TablePerConn*config.ConnNum + config.TableStart
	for i := config.TableStart; i < tbNum; i++ {
		sql := fmt.Sprintf("create table %s.%s%d using %s.%s tags(%d)", config.DbName, config.TablePrefix, i, config.DbName, config.MetricsName, i%config.TagNum+1)
		exec(client, sql)
	}

}

func insertTable(conn int) {
	client := &http.Client{}

	tbStart := conn*config.TablePerConn + config.TableStart
	tmStart := config.DataBegin

	for j := 0; j < config.DataNum; j++ {
		for i := 0; i < config.TablePerConn; i++ {
			tbIndex := i + tbStart

			tmVal := int64(j)*int64(config.DataInterval) + tmStart

			dataVal := j
			if config.DataRandom {
				dataVal = rand.Intn(1000)
			}

			sql := fmt.Sprintf("insert into %s.%s%d values(%d, %d)", config.DbName, config.TablePrefix, tbIndex, tmVal, dataVal)
			for k := 1; k < config.DataBatch; k++ {
				tmVal := int64(j)*int64(config.DataInterval) + int64(k) + tmStart

				dataVal := j + k
				if config.DataRandom {
					dataVal = rand.Intn(1000)
				}

				sql += fmt.Sprintf("(%d, %d)", tmVal, dataVal)
			}

			
			exec(client, sql)
			if config.WaitTime != 0 {
				time.Sleep(time.Millisecond * time.Duration(config.WaitTime))
			}
		}
		j += (config.DataBatch - 1)
	}
}

func insertLoop(conn int) {
	client := &http.Client{}

	tbStart := conn*config.TablePerConn + config.TableStart
	tmStart := config.DataBegin

	for j := 0; j < config.DataNum; j++ {

		for i := 0; i < config.TablePerConn; i++ {
			tbIndex := i + tbStart

			tmVal := int64(j)*int64(config.DataInterval) + tmStart

			dataVal := j
			if config.DataRandom {
				dataVal = rand.Intn(1000)
			}

			sql := fmt.Sprintf("insert into %s.%s%d values(%d, %d)", config.DbName, config.TablePrefix, tbIndex, tmVal, dataVal)
			for k := 1; k < config.DataBatch; k++ {
				tmVal := int64(j)*int64(config.DataInterval) + int64(k) + tmStart

				dataVal := j + k
				if config.DataRandom {
					dataVal = rand.Intn(1000)
				}
				sql += fmt.Sprintf("values(%d, %d)", tmVal, dataVal)
			}

			j += (config.DataBatch - 1)

			exec(client, sql)

			if config.WaitTime != 0 {
				time.Sleep(time.Millisecond * time.Duration(config.WaitTime))
			}
		}

	}
}

func insertTb(wg *sync.WaitGroup, conn int) {
	defer wg.Done()

	if !config.DataInsert {
		return
	}

	if config.InsertModel == "insertTable" {
		insertTable(conn)
	} else {
		insertLoop(conn)
	}
}

func selectData(wg *sync.WaitGroup, conn int) {
	defer wg.Done()

	client := &http.Client{}

	for i := 0; i < config.DataNum; i++ {
		exec(client, config.TableDesc)
	}
}

func main() {
	filename := flag.String("config", "taos_cloud.json", "config file name")
	
	flag.Parse()
	
	readFile(*filename)
	
	fmt.Println("\n================http test start======================")

	var wg sync.WaitGroup

	if config.InsertModel == "selectData" {
		fmt.Println("\n================select data  ========================")
		for i := 0; i < config.ConnNum; i++ {
			wg.Add(1)
			go selectData(&wg, i)
		}
	} else {
		createDb()
		createTb()

		if config.DataInsert {
			fmt.Println("\n================insert data  ========================")
		}

		for i := 0; i < config.ConnNum; i++ {
			wg.Add(1)
			go insertTb(&wg, i)
		}
	}

	wg.Wait()

	fmt.Println("\n================http test stop ======================")
	requestAvg := float64(period) / float64(1000000) / float64(request)
	qps := float64(1000) / float64(requestAvg) * float64(config.ConnNum)
	dps := qps * float64(config.DataBatch)
	fmt.Println("====== req:", request, ", error:", errorNum, ", qps:", int64(qps), ", wait:", int64(requestAvg), "ms", ", data per second:", int64(dps))
}
