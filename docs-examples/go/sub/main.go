package main

import (
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"time"

	taos "github.com/taosdata/driver-go/v2/af"
)

func main() {
	db, err := taos.Open("", "", "", "log", 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()
	topic, err := db.Subscribe(false, "taoslogtail", "select ts, level, ipaddr, content from log", time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer topic.Unsubscribe(true)
	for {
		func() {
			rows, err := topic.Consume()
			defer func() { rows.Close(); time.Sleep(time.Second) }()
			if err != nil {
				fmt.Println(err)
				os.Exit(3)
			}
			for {
				values := make([]driver.Value, 4)
				err := rows.Next(values)
				if err == io.EOF {
					break
				} else if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(4)
				}
				ts := values[0].(time.Time)
				level := values[1].(int8)
				ipaddr := values[2].(string)
				content := values[3].(string)
				fmt.Printf("%s %d %s %s\n", ts.Format(time.StampMilli), level, ipaddr, content)
			}
		}()
	}
}

// 未完成
