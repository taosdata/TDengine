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
    "flag"
    "fmt"
    "log"
    "os"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/influxdata/influxdb1-client/v2"
)

type ProArgs struct {
    host           string
    username       string
    password       string
    db             string
    sql            string
    dataDir        string
    filesNum       int
    writeClients   int
    rowsPerRequest int
}

type WriteInfo struct {
    threadId int
    sID      int
    eID      int
}

type StatisInfo struct {
    totalRows int64
}

var statis StatisInfo

func main() {
    // Configuration
    var arguments ProArgs

    // Parse options
    flag.StringVar(&(arguments.host), "host", "http://localhost:8086", "Server host to connect")
    flag.StringVar(&(arguments.db), "db", "db", "DB to insert data")
    flag.StringVar(&(arguments.username), "user", "", "Username used to connect to server")
    flag.StringVar(&(arguments.password), "pass", "", "Password used to connect to server")
    flag.StringVar(&(arguments.sql), "sql", "./sqlCmd.txt", "File name of SQL commands")
    flag.StringVar(&(arguments.dataDir), "dataDir", "./testdata", "Raw csv data")
    flag.IntVar(&(arguments.filesNum), "numOfFiles", 10, "Number of files int dataDir ")
    flag.IntVar(&(arguments.writeClients), "writeClients", 0, "Number of write clients")
    flag.IntVar(&(arguments.rowsPerRequest), "rowsPerRequest", 100, "Number of rows per request")

    flag.Parse()
    statis.totalRows = 0

    if arguments.writeClients > 0 {
        writeData(&arguments)
    } else {
        readData(&arguments)
    }
}

func writeData(arguments *ProArgs) {
    log.Println("write data")
    log.Println("---- writeClients:", arguments.writeClients)
    log.Println("---- dataDir:", arguments.dataDir)
    log.Println("---- numOfFiles:", arguments.filesNum)
    log.Println("---- rowsPerRequest:", arguments.rowsPerRequest)

    var wg sync.WaitGroup
    wg.Add(arguments.writeClients)

    st := time.Now()

    a := arguments.filesNum / arguments.writeClients
    b := arguments.filesNum % arguments.writeClients
    last := 0
    for i := 0; i < arguments.writeClients; i++ {
        var wInfo WriteInfo
        wInfo.threadId = i + 1
        wInfo.sID = last
        if i < b {
            wInfo.eID = last + a
        } else {
            wInfo.eID = last + a - 1
        }
        last = wInfo.eID + 1
        go writeDataImp(&wInfo, &wg, arguments)
    }

    wg.Wait()

    elapsed := time.Since(st)
    seconds := float64(elapsed) / float64(time.Second)

    log.Println("---- Spent", seconds, "seconds to insert", statis.totalRows, "records, speed:", float64(statis.totalRows)/seconds, "Rows/Second")
}

func writeDataImp(wInfo *WriteInfo, wg *sync.WaitGroup, arguments *ProArgs) {
    defer wg.Done()

    log.Println("Thread", wInfo.threadId, "writing sID", wInfo.sID, "eID", wInfo.eID)

    // Connect to the server
    conn, err := client.NewHTTPClient(client.HTTPConfig{
        Addr:     arguments.host,
        Username: arguments.username,
        Password: arguments.password,
	Timeout: 300 * time.Second,
    })

    if err != nil {
        log.Fatal(err)
    }

    defer conn.Close()

    // Create database
    _, err = queryDB(conn, fmt.Sprintf("create database %s", arguments.db), arguments.db)
    if err != nil {
        log.Fatal(err)
    }

    // Write data
    counter := 0
    totalRecords := 0

    bp, err := client.NewBatchPoints(client.BatchPointsConfig{
        Database:  arguments.db,
        Precision: "ms",
    })
    if err != nil {
        log.Fatal(err)
    }

    for j := wInfo.sID; j <= wInfo.eID; j++ {
        fileName := fmt.Sprintf("%s/testdata%d.csv", arguments.dataDir, j)
        fs, err := os.Open(fileName)
        if err != nil {
            log.Printf("failed to open file %s", fileName)
            log.Fatal(err)
        }
        log.Printf("open file %s success", fileName)

        bfRd := bufio.NewReader(fs)
        for {
            sline, err := bfRd.ReadString('\n')
            if err != nil {
                break
            }

            sline = strings.TrimSuffix(sline, "\n")
            s := strings.Split(sline, " ")
            if len(s) != 6 {
                continue
            }

            // Create a point and add to batch
            tags := map[string]string{
                "devid":    s[0],
                "devname":  s[1],
                "devgroup": s[2],
            }

            timestamp, _ := strconv.ParseInt(s[3], 10, 64)
            temperature, _ := strconv.ParseInt(s[4], 10, 32)
            humidity, _ := strconv.ParseFloat(s[5], 64)

            fields := map[string]interface{}{
                "temperature": temperature,
                "humidity":    humidity,
            }

            pt, err := client.NewPoint("devices", tags, fields, time.Unix(0, timestamp * int64(time.Millisecond)))
            if err != nil {
                log.Fatalln("Error: ", err)
            }

            bp.AddPoint(pt)
            counter++

            if counter >= arguments.rowsPerRequest {
                if err := conn.Write(bp); err != nil {
                    log.Fatal(err)
                }

                totalRecords += counter
                counter = 0
                bp, err = client.NewBatchPoints(client.BatchPointsConfig{
                    Database:  arguments.db,
                    Precision: "ms",
                })
                if err != nil {
                    log.Fatal(err)
                }
            }
        }

        fs.Close()
    }

    totalRecords += counter
    if counter > 0 {
        if err := conn.Write(bp); err != nil {
            log.Fatal(err)
        }
    }

    atomic.AddInt64(&statis.totalRows, int64(totalRecords))
}

func readData(arguments *ProArgs) {
    log.Println("read data")
    log.Println("---- sql:", arguments.sql)

    conn, err := client.NewHTTPClient(client.HTTPConfig{
        Addr:     arguments.host,
        Username: arguments.username,
        Password: arguments.password,
	Timeout: 300 * time.Second,
    })

    if err != nil {
        log.Fatal(err)
    }

    defer conn.Close()

    fs, err := os.Open(arguments.sql)
    if err != nil {
        log.Printf("failed to open file %s", arguments.sql)
        log.Fatal(err)
    }
    log.Printf("open file %s success", arguments.sql)

    bfRd := bufio.NewReader(fs)

    for {
        sline, err := bfRd.ReadString('\n')
        if err != nil {
            break
        }

        sline = strings.TrimSuffix(sline, "\n")

        st := time.Now()

        _, err = queryDB(conn, sline, arguments.db)
        if err != nil {
            log.Fatal(err)
        }

        elapsed := time.Since(st)
        seconds := float64(elapsed) / float64(time.Second)

        log.Println("---- Spent", seconds, "seconds to query ", sline)
    }
}

func queryDB(conn client.Client, cmd string, db string) (res []client.Result, err error) {
    query := client.Query{
        Command:  cmd,
        Database: db,
    }

    response, err := conn.Query(query)
    if err == nil {
        if response.Error() != nil {
            return res, response.Error()
        }
        res = response.Results
    } else {
        return res, err
    }

    return res, nil
}
