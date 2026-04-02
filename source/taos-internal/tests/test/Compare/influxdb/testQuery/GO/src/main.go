package main

import (
    "os"
    "io"
    "flag"
    "fmt"
    "log"
    "time"
    "bufio"
    "strings"
    "sync"

    "github.com/influxdata/influxdb/client/v2"
)

type ProArgs struct {
    host string
    username string
    password string
    db string
    command_file string
    query_time int
    connections int
}

func main() {
    // Configuration
    var arguments ProArgs

    flag.StringVar(&(arguments.host)          , "host"        , "http://localhost:8086"                           , "Server host to connect")
    flag.StringVar(&(arguments.username)      , "user"        , ""                                                , "Username used to connect to server")
    flag.StringVar(&(arguments.password)      , "pass"        , ""                                                , "Password used to connect to server")
    flag.StringVar(&(arguments.db)            , "db"          , "db"                                              , "DB to insert data")
    flag.StringVar(&(arguments.command_file)  , "command_file", "/home/taos/Documents/Comparison/influxdb/testQuery/query_cmd.txt" , "Command file name")
    flag.IntVar(&(arguments.query_time)       , "query_time"  , 5, "Query time")
    flag.IntVar(&(arguments.connections)      , "connections" , 1, "Query time")

    flag.Parse()

    // Loda query commands from file
    query_commands, err := load_command_from_file(arguments.command_file)
    if err != nil {
        log.Fatal(err)
    }

    var wg sync.WaitGroup

    wg.Add(arguments.connections)

    for i := 0; i < arguments.connections; i++ {
        go query_data(i, &arguments, query_commands, &wg)
    }

    wg.Wait()
}

func query_data(threadId int, arguments *ProArgs, query_commands []string, wg *sync.WaitGroup) {
    defer wg.Done()
    // Connect to server
    conn, err := client.NewHTTPClient(client.HTTPConfig{
        Addr     : arguments.host,
        Username : arguments.username,
        Password : arguments.password,
    })

    if err != nil {
        log.Fatal(err)
    }

    defer conn.Close()

    // Query commands
    for index, cmd := range query_commands {
        fmt.Println(fmt.Sprintf("Thread %d Command %d: %s------------", threadId, index, cmd))

        var tt float64 = 0

        for i := 0; i < arguments.query_time; i++ {
            count := 0
            st := get_curr_time_in_sec()

            res, err := queryDB(conn, cmd, arguments.db)
            if err != nil {
                log.Fatal(err)
            }

            for _, _ = range res[0].Series[0].Values {
                count++;
            }

            et := get_curr_time_in_sec()
            tt += (et - st)
            fmt.Println(fmt.Sprintf("    Thread %d Query %d, using %f seconds to retrieve %d records", threadId, i, et - st,  count))
        }

        fmt.Println(fmt.Sprintf("    Thread %d Average time: %f seconds===============", threadId, tt / float64(arguments.query_time)));
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

func load_command_from_file(fcommand string) (query_commands []string, err error) {

    fp, err := os.Open(fcommand)
    if err != nil {
        return query_commands, err
    }

    defer fp.Close()

    bfRd := bufio.NewReader(fp)

    for {
        line, err := bfRd.ReadString('\n')
        if err != nil {
            if err == io.EOF {
                break
            }

            return query_commands, err
        }
        line = strings.TrimSuffix(line, "\n")

        query_commands = append(query_commands, line)
    }

    return query_commands, nil

}

func get_curr_time_in_sec() (float64) {

    now := time.Now()
    secs := now.Unix()
    nanos := now.UnixNano()

    return float64(secs) + float64(nanos) * 1E-9
}
