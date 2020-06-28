package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/taosdata/driver-go/taosSql"
)

type argument struct {
	Type string        `json:"type"`
	Min  int           `json:"min"`
	Max  int           `json:"max"`
	List []interface{} `json:"list, omitempty"`
}

type script struct {
	isQuery bool       `json:"-"`
	numArgs int        `json:"-"`
	Sql     string     `json:"sql"`
	Args    []argument `json:"args"`
}

func (arg *argument) check() (int, error) {
	if arg.Type == "list" {
		if len(arg.List) == 0 {
			return 0, errors.New("list cannot be empty")
		}
		return 1, nil
	}

	if arg.Max < arg.Min {
		return 0, errors.New("invalid min/max value")
	}

	if arg.Type == "string" {
		if arg.Min < 0 {
			return 0, errors.New("negative string length")
		}
	}

	if arg.Type == "int" && arg.Min == 0 && arg.Max == 0 {
		arg.Max = arg.Min + 100
	}

	if arg.Type == "range" {
		return 2, nil
	}

	return 1, nil
}

func (arg *argument) generate(args []interface{}) []interface{} {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

	switch arg.Type {
	case "bool":
		if rand.Intn(2) == 1 {
			args = append(args, true)
		} else {
			args = append(args, false)
		}

	case "int":
		v := rand.Intn(arg.Max-arg.Min+1) + arg.Min
		args = append(args, v)

	case "range":
		v := rand.Intn(arg.Max-arg.Min) + arg.Min
		args = append(args, v)
		v = rand.Intn(arg.Max-v+1) + v
		args = append(args, v)

	case "string":
		l := rand.Intn(arg.Max-arg.Min+1) + arg.Min
		sb := strings.Builder{}
		for i := 0; i < l; i++ {
			sb.WriteByte(chars[rand.Intn(len(chars))])
		}
		args = append(args, sb.String())

	case "list":
		v := arg.List[rand.Intn(len(arg.List))]
		args = append(args, v)
	}

	return args
}

type statitics struct {
	succeeded         int64
	failed            int64
	succeededDuration int64
	failedDuration    int64
}

var (
	server     string
	database   string
	fetch      bool
	concurrent uint
	startAt    time.Time
	shouldStop int64
	wg         sync.WaitGroup
	stat       statitics
	scripts    []script
)

func loadScript(path string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()

	e = json.NewDecoder(f).Decode(&scripts)
	if e != nil {
		return e
	}

	for i := 0; i < len(scripts); i++ {
		s := &scripts[i]
		s.Sql = strings.TrimSpace(s.Sql)
		s.isQuery = strings.ToLower(s.Sql[:6]) == "select"

		for j := 0; j < len(s.Args); j++ {
			arg := &s.Args[j]
			arg.Type = strings.ToLower(arg.Type)
			n, e := arg.check()
			if e != nil {
				return fmt.Errorf("script %d argument %d: %s", i, j, e.Error())
			}
			s.numArgs += n
		}
	}

	return nil
}

func buildSql() (string, bool) {
	s := scripts[rand.Intn(len(scripts))]
	args := make([]interface{}, 0, s.numArgs)
	for i := 0; i < len(s.Args); i++ {
		args = s.Args[i].generate(args)
	}
	return fmt.Sprintf(s.Sql, args...), s.isQuery
}

func runTest() {
	defer wg.Done()
	db, e := sql.Open("taosSql", "root:taosdata@tcp("+server+":0)/"+database)
	if e != nil {
		fmt.Printf("failed to connect to database: %s\n", e.Error())
		return
	}
	defer db.Close()

	for atomic.LoadInt64(&shouldStop) == 0 {
		str, isQuery := buildSql()
		start := time.Now()

		if isQuery {
			var rows *sql.Rows
			if rows, e = db.Query(str); rows != nil {
				if fetch {
					for rows.Next() {
					}
				}
				rows.Close()
			}
		} else {
			_, e = db.Exec(str)
		}

		duration := time.Now().Sub(start).Microseconds()
		if e != nil {
			atomic.AddInt64(&stat.failed, 1)
			atomic.AddInt64(&stat.failedDuration, duration)
		} else {
			atomic.AddInt64(&stat.succeeded, 1)
			atomic.AddInt64(&stat.succeededDuration, duration)
		}
	}
}

func getStatPrinter() func(tm time.Time) {
	var last statitics
	lastPrintAt := startAt

	return func(tm time.Time) {
		var current statitics

		current.succeeded = atomic.LoadInt64(&stat.succeeded)
		current.failed = atomic.LoadInt64(&stat.failed)
		current.succeededDuration = atomic.LoadInt64(&stat.succeededDuration)
		current.failedDuration = atomic.LoadInt64(&stat.failedDuration)

		seconds := int64(tm.Sub(startAt).Seconds())
		format := "\033K %02v:%02v:%02v | TOTAL REQ | TOTAL TIME(us) | TOTAL AVG(us) | REQUEST |  TIME(us)  |  AVERAGE(us)  |\n"
		fmt.Printf(format, seconds/3600, seconds%3600/60, seconds%60)
		fmt.Println("------------------------------------------------------------------------------------------------")

		tr := current.succeeded + current.failed
		td := current.succeededDuration + current.failedDuration
		r := tr - last.succeeded - last.failed
		d := td - last.succeededDuration - last.failedDuration
		ta, a := 0.0, 0.0
		if tr > 0 {
			ta = float64(td) / float64(tr)
		}
		if r > 0 {
			a = float64(d) / float64(r)
		}
		format = "\033[K    TOTAL | %9v | %14v | %13.2f | %7v | %10v | % 13.2f |\n"
		fmt.Printf(format, tr, td, ta, r, d, a)

		tr = current.succeeded
		td = current.succeededDuration
		r = tr - last.succeeded
		d = td - last.succeededDuration
		ta, a = 0.0, 0.0
		if tr > 0 {
			ta = float64(td) / float64(tr)
		}
		if r > 0 {
			a = float64(d) / float64(r)
		}
		format = "\033[K  SUCCESS | \033[32m%9v\033[0m | \033[32m%14v\033[0m | \033[32m%13.2f\033[0m | \033[32m%7v\033[0m | \033[32m%10v\033[0m | \033[32m%13.2f\033[0m |\n"
		fmt.Printf(format, tr, td, ta, r, d, a)

		tr = current.failed
		td = current.failedDuration
		r = tr - last.failed
		d = td - last.failedDuration
		ta, a = 0.0, 0.0
		if tr > 0 {
			ta = float64(td) / float64(tr)
		}
		if r > 0 {
			a = float64(d) / float64(r)
		}
		format = "\033[K     FAIL | \033[31m%9v\033[0m | \033[31m%14v\033[0m | \033[31m%13.2f\033[0m | \033[31m%7v\033[0m | \033[31m%10v\033[0m | \033[31m%13.2f\033[0m |\n"
		fmt.Printf(format, tr, td, ta, r, d, a)

		last = current
		lastPrintAt = tm
	}
}

func main() {
	flag.StringVar(&server, "server", "localhost", "host name or IP address of TDengine server")
	flag.StringVar(&database, "db", "test", "database name")
	flag.BoolVar(&fetch, "fetch", false, "fetch result or not")
	flag.UintVar(&concurrent, "concurrent", 1, "number of concurrent queries")
	flag.Parse()

	scriptFile := flag.Arg(0)
	if scriptFile == "" {
		scriptFile = "script.json"
	}
	if e := loadScript(scriptFile); e != nil {
		fmt.Println("failed to load script file:", e.Error())
		return
	} else if len(scripts) == 0 {
		fmt.Println("there's no script in the script file")
		return
	}

	rand.Seed(time.Now().UnixNano())

	fmt.Println()
	fmt.Printf("SERVER: %s    DATABASE: %s    CONCURRENT QUERIES: %d    FETCH DATA: %v\n", server, database, concurrent, fetch)
	fmt.Println()

	startAt = time.Now()
	printStat := getStatPrinter()
	printStat(startAt)

	for i := uint(0); i < concurrent; i++ {
		wg.Add(1)
		go runTest()
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	ticker := time.NewTicker(time.Second)

	fmt.Println("Ctrl + C to exit....\033[1A")

LOOP:
	for {
		select {
		case <-interrupt:
			break LOOP
		case tm := <-ticker.C:
			fmt.Print("\033[5A")
			printStat(tm)
		}
	}

	atomic.StoreInt64(&shouldStop, 1)
	fmt.Print("\033[100D'Ctrl + C' received, Waiting started query to stop...")

	wg.Wait()
	fmt.Print("\033[5A\033[100D")
	printStat(time.Now())
	fmt.Println()
}
