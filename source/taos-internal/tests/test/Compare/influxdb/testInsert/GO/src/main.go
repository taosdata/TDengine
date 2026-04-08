package main

import (
    "fmt"
    "log"
    "os"
    "io"
    "bufio"
    "time"
    "flag"
    "strconv"
    "strings"
    "sync"
    "regexp"

    "github.com/influxdata/influxdb/client/v2"
)


type TagValue struct {
    value string
    percent float64
}

type TagInfo struct {
    key string
    values []TagValue
}

type SchemaInfo struct {
    metrics []string
    tags []TagInfo
}

type ProArgs struct {
    host string
    username string
    password string
    db   string
    detector_prefix string
    num_of_detector int
    points_per_detector int
    points_per_batch int
    connections int
    fschema string
    fsample string
    start_time int64
    time_interval int64
    schema  SchemaInfo
    sample_data [][]interface{}
}

type WriteInfo struct {
    threadId int
    sID      int
    eID      int
}

func main() {
    // Configuration
    var arguments ProArgs
    var err error

    // Parse options
    flag.StringVar(&(arguments.host)                , "host"        , "http://localhost:8086"                           , "Server host to connect")
    flag.StringVar(&(arguments.db)                  , "db"          , "db"                                              , "DB to insert data")
    flag.StringVar(&(arguments.username)            , "user"        , ""                                                , "Username used to connect to server")
    flag.StringVar(&(arguments.password)            , "pass"        , ""                                                , "Password used to connect to server")
    flag.StringVar(&(arguments.detector_prefix)     , "tag_prefix"  , "card"                                                , "Tag prefix")
    flag.StringVar(&(arguments.fschema)             , "schema"      , "/home/taos/Documents/Comparison/data/schema.txt" , "schema file")
    flag.StringVar(&(arguments.fsample)             , "sample"      , "/home/taos/Documents/Comparison/data/sample.txt" , "sample file")

    flag.IntVar(&(arguments.num_of_detector)     , "detectors"   , 100   , "Tag prefix")
    flag.IntVar(&(arguments.points_per_detector) , "points"      , 10000 , "Number of points per detector")
    flag.IntVar(&(arguments.points_per_batch)    , "batch"       , 10000 , "Number of points in a batch")
    flag.IntVar(&(arguments.connections)   , "connections" , 1             , "Number of connections")
    flag.Int64Var(&(arguments.start_time)    , "start_time" , 1545038786000 , "Start time")
    flag.Int64Var(&(arguments.time_interval) , "interval"   , 10000         , "Sample time interval")

    flag.Parse()

    // Load sample data
    arguments.schema, arguments.sample_data, err = loadSampleData(arguments.fschema, arguments.fsample)
    if err != nil {
        log.Fatal(err)
    }

    // fmt.Println(arguments.sample_data)

    // Synchronize insertion
    var wg sync.WaitGroup
    wg.Add(arguments.connections)

    st := time.Now()
    a := arguments.num_of_detector / arguments.connections
    b := arguments.num_of_detector % arguments.connections
    last := 0
    for i := 0; i < arguments.connections; i++ {
        var wInfo WriteInfo
        wInfo.threadId = i+1
        wInfo.sID = last
        if i < b {
            wInfo.eID = last + a
        } else {
            wInfo.eID = last + a -1
        }
        last = wInfo.eID + 1
        go write_data(&wInfo, &wg, &arguments)
    }

    wg.Wait()
    elapsed := time.Since(st)
    seconds := float64(elapsed) / float64(time.Second)
    total_records := arguments.num_of_detector * arguments.points_per_detector

    fmt.Println("Spent", seconds, "seconds to insert", total_records, "records, speed:", float64(total_records)/seconds, "R/s")
}

func write_data(wInfo *WriteInfo, wg *sync.WaitGroup, arguments *ProArgs) {
    // fmt.Println(arguments.schema)
    defer wg.Done()

    fmt.Println("Thread", wInfo.threadId, "writing sID", wInfo.sID, "eID", wInfo.eID)

    // Connect to the server
    conn, err := client.NewHTTPClient(client.HTTPConfig{
        Addr: arguments.host,
        Username: arguments.username,
        Password: arguments.password,
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
    bp, err := client.NewBatchPoints(client.BatchPointsConfig{
        Database:  arguments.db,
        Precision: "ms",
    })
    if err != nil {
        log.Fatal(err)
    }

    sample_data_counter := 0;

    // fmt.Println(arguments.points_per_detector, wInfo.sID, wInfo.eID)
    for i := 0; i < arguments.points_per_detector; i++ {
        var tt int64 = arguments.start_time + int64(i)* arguments.time_interval

        for j := wInfo.sID; j <= wInfo.eID; j++ {
            // CREATE POINT
            // create tags
            tags := map[string]string{};
            tags["detector"] = fmt.Sprintf("%s%d", arguments.detector_prefix, j)
            // if arguments.schema.tags == nil || len(arguments.schema.tags)  == 0 {
            //     tags["detector"] = fmt.Sprintf("%s%d", arguments.detector_prefix, j)
            // } else {
            if arguments.schema.tags != nil && len(arguments.schema.tags)  != 0 {
                for k := 0; k < len(arguments.schema.tags); k++ {
                    spercent := 0.0
                    for idx := 0; idx < len(arguments.schema.tags[k].values); idx++ {
                        spercent += arguments.schema.tags[k].values[idx].percent
                        if float64(j+1) <= spercent * float64(arguments.num_of_detector){
                            // fmt.Println(i, j, k, spercent, arguments.schema.tags[k].key, arguments.schema.tags[k].values[idx].value)
                            tags[arguments.schema.tags[k].key] = arguments.schema.tags[k].values[idx].value
                            break
                        }
                    }
                }
            }
            // tags := map[string]string{"detector": }
            // fmt.Println(arguments.sample_data);
            record := arguments.sample_data[sample_data_counter]
            sample_data_counter = (sample_data_counter + 1 ) % len(arguments.sample_data)
            fields := make(map[string]interface{})

            for ncount := 0; ncount < len(arguments.schema.metrics); ncount++ {
                fields[arguments.schema.metrics[ncount]] = record[ncount]
            }

            // TODO : monify time here
            pt, err := client.NewPoint("monitor", tags, fields, time.Unix(0, tt * int64(time.Millisecond)))
            if err != nil {
                log.Fatal(err)
            }

            bp.AddPoint(pt)

            counter++

            if counter >= arguments.points_per_batch {
                if err := conn.Write(bp); err != nil {
                    log.Fatal(err)
                }


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
    }

    if counter > 0 {
        if err := conn.Write(bp); err != nil {
            log.Fatal(err)
        }
    }

}

func queryDB(conn client.Client, cmd string, db string) (res []client.Result, err error) {
    query := client.Query{
        Command: cmd,
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

func loadSampleData(fschema string, fsample string) (schema SchemaInfo, sample_data [][]interface{}, err error){

    var types []string

    /* Read schema */
    fs, err := os.Open(fschema)
    if err != nil {
        return schema, sample_data, err
    }

    defer fs.Close()

    bfRd := bufio.NewReader(fs)

    for {
        sline, err := bfRd.ReadString('\n')
        if err != nil {
            if err == io.EOF{
                break
            }
            return schema, sample_data, err
        }

        sline = strings.TrimSuffix(sline, "\n")

        comment_line, _ := regexp.Compile("^\\s*#.*$")
        empty_line, _ := regexp.Compile("^\\s*$")
        if (comment_line.MatchString(sline) || empty_line.MatchString(sline)) {
            continue;
        }
        // handle the line
        if strings.Contains(sline, ":") { // tag schema
            s := strings.Split(sline, ":")
            var ntag TagInfo
            ntag.key = strings.Split(s[0], " ")[0]
            for _, token := range strings.Split(s[1], ",") {
                var nvalue TagValue

                // s_t := strings.Fields(strings.Trim(token, " "), " ")
                s_t := strings.Fields(token)
                nvalue.value = strings.Trim(s_t[0], " '")
                nvalue.percent, _ = strconv.ParseFloat(s_t[1], 64)

                ntag.values = append(ntag.values, nvalue)
            }

            schema.tags = append(schema.tags, ntag)
        } else { // data schema
            s := strings.Split(sline, ",")
            for _, token := range s {
                name := strings.Split(strings.Trim(token, " "), " ")
                schema.metrics = append(schema.metrics, name[0])
                types = append(types, name[1])
            }
        }
    }

    /* Read sample data */
    ft, err := os.Open(fsample)
    if err != nil {
        return schema, sample_data, err
    }

    defer ft.Close()

    bfRd = bufio.NewReader(ft)

    for {
        line, err := bfRd.ReadString('\n')
        if err != nil {
            if err == io.EOF {
                break
            }

            return schema, sample_data, err
        }
        line = strings.TrimSuffix(line, "\n")

        // convert line to []interface{}
        var record []interface{}
        for idx, token := range strings.Split(line, ",") {
            if types[idx] == "int" || types[idx] == "tinyint" || types[idx] == "smallint" || types[idx] == "bigint" {
                data, _ := strconv.Atoi(strings.Trim(token, " "));
                record = append(record, data)
            } else if types[idx] == "float" || types[idx] == "double" {
                data, _ := strconv.ParseFloat(strings.Trim(token, " "), 32)
                record = append(record, data)
            } else if types[idx] == "bool" {
                data, _ := strconv.ParseBool(strings.Trim(token, " "))
                record = append(record, data)
            } else if types[idx][0:6] == "binary" {
                data := strings.Trim(token, "' ")
                record = append(record, data)
            }
        }


        sample_data = append(sample_data, record)
    }

    return schema, sample_data, nil
}

func get_curr_time_in_sec() (float64) {
    now := time.Now()
    secs := now.Unix()
    nanos := now.UnixNano()

    return float64(secs) + float64(nanos) * 1E-9
}
