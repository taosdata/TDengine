package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/taosdata/driver-go/v3/ws/unified"
)

func getenv(key, def string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	return value
}

func getenvInt(key string, def int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return def
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return def
	}
	return parsed
}

func touchFile(path string) {
	if strings.TrimSpace(path) == "" {
		return
	}
	_ = ioutil.WriteFile(path, []byte(time.Now().Format(time.RFC3339Nano)+"\n"), 0644)
}

func main() {
	dsn := getenv("PROBE_DSN", "root:taosdata@ws(127.0.0.1:6041,127.0.0.1:6042)/")
	markFile := strings.TrimSpace(os.Getenv("PROBE_MARK_FILE"))
	readyFile := strings.TrimSpace(os.Getenv("PROBE_READY_FILE"))
	totalSec := getenvInt("PROBE_TOTAL_SEC", 25)
	intervalMs := getenvInt("PROBE_INTERVAL_MS", 200)
	postNeedSuccess := getenvInt("PROBE_POST_SUCCESS", 5)
	if totalSec <= 0 {
		totalSec = 25
	}
	if intervalMs <= 0 {
		intervalMs = 200
	}
	if postNeedSuccess <= 0 {
		postNeedSuccess = 1
	}

	dbName := getenv("PROBE_DB", fmt.Sprintf("unified_failover_probe_%d", time.Now().UnixNano()))
	tableName := "t_probe"

	connector, err := unified.OpenConnector(dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse dsn failed: %v\n", err)
		os.Exit(2)
	}
	cfg := connector.Config()
	cfg.ReadTimeout = 2 * time.Second
	cfg.WriteTimeout = 2 * time.Second
	cfg.AutoReconnect = true
	cfg.ReconnectIntervalMs = 50
	cfg.ReconnectRetryCount = 80

	client, err := unified.NewClient(&cfg, "/ws")
	if err != nil {
		fmt.Fprintf(os.Stderr, "new unified client failed: %v\n", err)
		os.Exit(3)
	}
	defer client.Close()

	if err = client.Connect(); err != nil {
		fmt.Fprintf(os.Stderr, "connect failed: %v\n", err)
		os.Exit(4)
	}

	if _, err = client.Exec(0, "create database if not exists "+dbName); err != nil {
		fmt.Fprintf(os.Stderr, "create database failed: %v\n", err)
		os.Exit(5)
	}
	defer func() {
		_, _ = client.Exec(0, "drop database if exists "+dbName)
	}()

	if _, err = client.Exec(0, "create table if not exists "+dbName+"."+tableName+"(ts timestamp, v int)"); err != nil {
		fmt.Fprintf(os.Stderr, "create table failed: %v\n", err)
		os.Exit(6)
	}

	fmt.Printf("probe start endpoints=%s db=%s total_sec=%d interval_ms=%d\n", strings.Join(cfg.Endpoints, ","), dbName, totalSec, intervalMs)

	var preSuccess int
	var postSuccess int
	var failCount int
	markerSeen := false
	readyMarked := false
	deadline := time.Now().Add(time.Duration(totalSec) * time.Second)

	for time.Now().Before(deadline) {
		if !markerSeen && markFile != "" {
			if _, statErr := os.Stat(markFile); statErr == nil {
				markerSeen = true
				fmt.Println("marker detected: start validating post-failover success")
			}
		}

		_, err = client.Exec(0, "select server_version()")
		phase := "pre"
		if markerSeen {
			phase = "post"
		}
		if err != nil {
			failCount++
			fmt.Printf("query err phase=%s pre=%d post=%d fail=%d err=%v\n", phase, preSuccess, postSuccess, failCount, err)
		} else {
			if markerSeen {
				postSuccess++
			} else {
				preSuccess++
				if !readyMarked && preSuccess >= 3 {
					touchFile(readyFile)
					readyMarked = true
					fmt.Println("ready marker created")
				}
			}
			fmt.Printf("query ok phase=%s pre=%d post=%d fail=%d\n", phase, preSuccess, postSuccess, failCount)
		}

		if markerSeen && postSuccess >= postNeedSuccess {
			break
		}

		time.Sleep(time.Duration(intervalMs) * time.Millisecond)
	}

	if preSuccess == 0 {
		fmt.Fprintln(os.Stderr, "probe failed: no successful queries before failover")
		os.Exit(7)
	}
	if markFile != "" && !markerSeen {
		fmt.Fprintln(os.Stderr, "probe failed: failover marker not observed")
		os.Exit(8)
	}
	if markerSeen && postSuccess < postNeedSuccess {
		fmt.Fprintf(os.Stderr, "probe failed: post-failover successful queries=%d, need=%d\n", postSuccess, postNeedSuccess)
		os.Exit(9)
	}

	fmt.Printf("PASS failover probe: pre_success=%d post_success=%d fail=%d\n", preSuccess, postSuccess, failCount)
}
