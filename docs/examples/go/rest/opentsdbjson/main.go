// Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
//
// SPDX-License-Identifier: AGPL-3.0-or-later
//
// This program is free software: you can use, redistribute, and/or modify
// it under the terms of the GNU Affero General Public License, version 3
// or later ("AGPL"), as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful, but WITHOUT
// ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// FITNESS FOR A PARTICULAR PURPOSE.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

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
