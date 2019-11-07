package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var (
	token    string
	url      string
	config   Config
	request  int64
	begin    time.Time
	errorNum int64
)

type Config struct {
	HostIp      string `json:"hostIp"`
	TableNum    int    `json:"tableNum"`
	DbName      string `json:"dbName"`
	MetricsName string `json:"metricsName"`
	DataNum     int    `json:"dataNum"`
	BatchNum    int    `json:"batchNum"`
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
		println("taos.json not found")
		panic(err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	err = dec.Decode(&config)
	if err != nil {
		println("taos.json parse error")
		panic(err)
	}

	request = 0
	errorNum = 0

	fmt.Println("================config parameters======================")
	fmt.Println("HostIp:", config.HostIp)
	fmt.Println("TableNum:", config.TableNum)
	fmt.Println("dbName:", config.DbName)
	fmt.Println("metricsName:", config.MetricsName)
	fmt.Println("dataNum:", config.DataNum)
	fmt.Println("batchNum:", config.BatchNum)

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
	for reTryTimes := 0; reTryTimes < 10; reTryTimes++ {

		req, err1 := http.NewRequest("POST", url, bytes.NewReader([]byte(sql)))
		if err1 != nil {
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

		if (request*int64(config.BatchNum))%100000 == 0 && request != 0 {
			spend := time.Since(begin).Seconds()
			if spend >= 1 && spend < 10000000 {
				total := (request - errorNum - 2 - int64(config.TableNum)) * int64(config.BatchNum)
				fmt.Printf("request:%d, error:%d, insert:%d, spend:%.2f seconds, dps:%.1f \n", request, errorNum, total, spend, float64(total)/float64(spend))
			}
		}

		return
	}

	//fmt.Println("exec failed, sql:", sql)
	errorNum++
}

func createDb() {
	fmt.Println("================create database =====================")

	client := &http.Client{}
	sql := fmt.Sprintf("create database %s", config.DbName)
	exec(client, sql)
}

func createTb() {
	fmt.Println("================create table ========================")

	client := &http.Client{}
	sql := fmt.Sprintf("create table %s.%s(ts timestamp, f1 int, f2 int) tags (tb int)", config.DbName, config.MetricsName)
	exec(client, sql)

	for i := 0; i < config.TableNum; i++ {
		sql := fmt.Sprintf("create table %s.t%d using %s.%s tags(%d)", config.DbName, i, config.DbName, config.MetricsName, i)
		exec(client, sql)
	}
}

func insertData(wg *sync.WaitGroup, tableIndex int) {
	defer wg.Done()

	client := &http.Client{}
	beginTime := int64(1519833600000)

	for i := 0; i < config.DataNum; i += config.BatchNum {
		var sql bytes.Buffer
		sql.WriteString(fmt.Sprintf("insert into %s.t%d values", config.DbName, tableIndex))

		for j := 0; j < config.BatchNum; j++ {
			sql.WriteString(fmt.Sprintf("(%d,%d,%d)", beginTime+int64(i)+int64(j), rand.Intn(1000), rand.Intn(1000)))
		}
		exec(client, sql.String())
	}
}

func main() {
	filename := flag.String("config", "http.json", "config file name")

	flag.Parse()

	readFile(*filename)

	fmt.Println("\n================http test start======================")

	createDb()
	createTb()

	begin = time.Now()

	var wg sync.WaitGroup

	fmt.Println("================insert data  ========================")
	for i := 0; i < config.TableNum; i++ {
		wg.Add(1)
		go insertData(&wg, i)
	}

	wg.Wait()

	fmt.Println("\n================http test stop ======================")

	spend := time.Since(begin).Seconds()

	total := (request - errorNum - 2 - int64(config.TableNum)) * int64(config.BatchNum)
	fmt.Printf("request:%d, error:%d, insert:%d, spend:%.2f seconds, dps:%.1f \n", request, errorNum, total, spend, float64(total)/float64(spend))
}
