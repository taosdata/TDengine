package main

import (
    "database/sql"
    "fmt"
    "os"

    _ "github.com/taosdata/driver-go/v2/taosRestful"
)

func main() {
    dsn := os.Getenv("TDENGINE_GO_DSN")
    taos, err := sql.Open("taosRestful", dsn)
    if err != nil {
        fmt.Println("failed to connect TDengine, err:", err)
        return
    }
    _, err = taos.Exec("select server_version()")
    if err != nil {
	fmt.Println(err)
    }
    fmt.Println("connect success")
    defer taos.Close()
}
