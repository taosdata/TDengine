package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Tags struct {
	Location string `json:"location"`
	Groupid  int32  `json:"groupid"`
}

type Metric struct {
	Metric    string `json:"metric"`
	Timestamp int64  `json:"timestamp"`
	Value     int32  `json:"value"`
	Tags      Tags   `json:"tags"`
}

func main() {
	client := http.Client{}
	for i := 0; i < 10; i++ {
		metric := Metric{"voltage", time.Now().UnixMilli(), 1, Tags{"A", 1}}
		json, err := json.Marshal(metric)
		if err != nil {
			panic(err)
		}
		req, err := http.NewRequest("POST", "http://localhost:6041/opentsdb/v1/put/json/rest_go", bytes.NewBuffer(json))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Basic cm9vdDp0YW9zZGF0YQ==")
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\n", resp)
		time.Sleep(time.Second)

	}
}
