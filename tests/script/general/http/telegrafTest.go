package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"io/ioutil"
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
	template string
)

type Config struct {
	HostIp       string `json:"hostIp"`
	MachineNum   int    `json:"machineNum"`
	LoopNum      int    `json:"loopNum"`
	DbName       string `json:"dbName"`
	DataBegin    int64  `json:"dataBegin"`
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

func readConf(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		println("%s not found", filename)
		panic(err)
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	err = dec.Decode(&config)
	if err != nil {
		println("%s parse error", filename)
		panic(err)
	}

	request = 0
	period = 0
	errorNum = 0

	fmt.Println("================config parameters======================")
	fmt.Println("HostIp:", config.HostIp)
	fmt.Println("MachineNum:", config.MachineNum)
	fmt.Println("LoopNum:", config.LoopNum)
	fmt.Println("dbName:", config.DbName)
	fmt.Println("dataBegin:", config.DataBegin)
	
	fmt.Println("================http token=============================")
	token, err = getToken()
	url = fmt.Sprintf("http://%s:%d/telegraf/%s", config.HostIp, 6020, config.DbName)

	fmt.Println("httpToken:", token)
	fmt.Println("httpUrl:", url)

	if err != nil {
		panic(err)
	}
}

func readReq(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		println("%s not found", filename)
		panic(err)
	}
	defer file.Close()

	data, _ := ioutil.ReadAll(file)
	
	template = string(data[:])
 
	//fmt.Println(template)
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
	for times := 0; times < 10; times++ {

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
			fmt.Println(data)
			resp.Body.Close()
			continue
		}

		spend := (time.Since(begin).Nanoseconds())

		var jsonResult JsonResult
		err = json.Unmarshal(data, &jsonResult)
		if err != nil {
			fmt.Println("parse json error: ", string(data[:]))	
			resp.Body.Close()
			continue
		}

		
		atomic.AddInt64(&request, 1)		
		atomic.AddInt64(&period, spend)

		if request%1000 == 0 && request != 0 {
			requestAvg := float64(period) / float64(1000000) / float64(request)
			qps := float64(1000) / float64(requestAvg) * float64(config.MachineNum)
			dps := qps * float64(22)
			fmt.Println("====== req:", request, ", error:", errorNum, ", qps:", int64(qps), ", wait:", int64(requestAvg), "ms", ", data per second:", int64(dps))
		}
		return
	}
	//fmt.Println("xxxx>sql:", sql, ", retryTimes:", 10)
	fmt.Println("exec sql failed")
	errorNum++
}

func writeData(wg *sync.WaitGroup, tbIndex int) {
	defer wg.Done()
	client := &http.Client{}
	
	tbName := fmt.Sprintf("t%d", tbIndex)
	
	for j := 0; j < config.LoopNum; j++ {
		tmVal := fmt.Sprintf("%d", int64(j)*int64(10000) + config.DataBegin)
		//fmt.Println(tmVal)

		req1 := strings.Replace(template, "panshi-gsl", tbName, -1)
		req2 := strings.Replace(req1, "1536750390000", tmVal, -1)
		
		//fmt.Println(req2)
		exec(client, req2)
	}
}

func main() {
	filename := flag.String("config", "telegraf.json", "config file name")
	
	flag.Parse()
	
	readReq("telegraf.req")
	
	readConf(*filename)
	
	fmt.Println("\n================telegraf test start======================")

	var wg sync.WaitGroup
	
	for i := 0; i < config.MachineNum; i++ {
		wg.Add(1)
		go writeData(&wg, i)
	}

	wg.Wait()

	fmt.Println("\n================telegraf test stop ======================")

	requestAvg := float64(period) / float64(1000000) / float64(request)
	qps := float64(1000) / float64(requestAvg) * float64(config.MachineNum)
	dps := qps * float64(22)
	fmt.Println("====== req:", request, ", error:", errorNum, ", qps:", int64(qps), ", wait:", int64(requestAvg), "ms", ", data per second:", int64(dps))
}
