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

type testCase struct {
	isQuery bool       `json:"-"`
	numArgs int        `json:"-"`
	Weight  int        `json:"weight"`
	SQL     string     `json:"sql"`
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

func (tc *testCase) buildSql() string {
	args := make([]interface{}, 0, tc.numArgs)
	for i := 0; i < len(tc.Args); i++ {
		args = tc.Args[i].generate(args)
	}
	return fmt.Sprintf(tc.SQL, args...)
}

type statitics struct {
	succeeded         int64
	failed            int64
	succeededDuration int64
	failedDuration    int64
}

var (
	host     string
	port     uint
	database string
	user     string
	password string
	fetch    bool

	chLog       chan string
	wgLog       sync.WaitGroup
	startAt     time.Time
	shouldStop  int64
	wgTest      sync.WaitGroup
	stat        statitics
	totalWeight int
	cases       []testCase
)

func loadTestCaseFromFile(file *os.File) error {
	if e := json.NewDecoder(file).Decode(&cases); e != nil {
		return e
	}

	if len(cases) == 0 {
		return fmt.Errorf("no test case loaded.")
	}

	for i := 0; i < len(cases); i++ {
		c := &cases[i]
		c.SQL = strings.TrimSpace(c.SQL)
		c.isQuery = strings.ToLower(c.SQL[:6]) == "select"
		if c.Weight < 0 {
			return fmt.Errorf("test %d: negative weight", i)
		}
		totalWeight += c.Weight

		for j := 0; j < len(c.Args); j++ {
			arg := &c.Args[j]
			arg.Type = strings.ToLower(arg.Type)
			n, e := arg.check()
			if e != nil {
				return fmt.Errorf("test case %d argument %d: %s", i, j, e.Error())
			}
			c.numArgs += n
		}
	}

	if totalWeight == 0 {
		for i := 0; i < len(cases); i++ {
			cases[i].Weight = 1
		}
		totalWeight = len(cases)
	}

	return nil
}

func loadTestCase(pathOrSQL string) error {
	if f, e := os.Open(pathOrSQL); e == nil {
		defer f.Close()
		return loadTestCaseFromFile(f)
	}

	pathOrSQL = strings.TrimSpace(pathOrSQL)
	if strings.ToLower(pathOrSQL[:6]) != "select" {
		return fmt.Errorf("'%s' is not a valid file or SQL statement", pathOrSQL)
	}

	cases = append(cases, testCase{
		isQuery: true,
		Weight:  1,
		numArgs: 0,
		SQL:     pathOrSQL,
	})
	totalWeight = 1

	return nil
}

func selectTestCase() *testCase {
	sum, target := 0, rand.Intn(totalWeight)
	var c *testCase
	for i := 0; i < len(cases); i++ {
		c = &cases[i]
		sum += c.Weight
		if sum > target {
			break
		}
	}
	return c
}

func runTest() {
	defer wgTest.Done()
	db, e := sql.Open("taosSql", fmt.Sprintf("%s:%s@tcp(%s:%v)/%s", user, password, host, port, database))
	if e != nil {
		fmt.Printf("failed to connect to database: %s\n", e.Error())
		return
	}
	defer db.Close()

	for atomic.LoadInt64(&shouldStop) == 0 {
		c := selectTestCase()
		str := c.buildSql()

		start := time.Now()
		if c.isQuery {
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
			if chLog != nil {
				chLog <- str + ": " + e.Error()
			}
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
		format := "\033[47;30m %02v:%02v:%02v | TOTAL REQ | TOTAL TIME(us) | TOTAL AVG(us) | REQUEST |  TIME(us)  |  AVERAGE(us)  |\033[0m\n"
		fmt.Printf(format, seconds/3600, seconds%3600/60, seconds%60)

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
		format = "    TOTAL | %9v | %14v | %13.2f | %7v | %10v | % 13.2f |\n"
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
		format = "  SUCCESS | \033[32m%9v\033[0m | \033[32m%14v\033[0m | \033[32m%13.2f\033[0m | \033[32m%7v\033[0m | \033[32m%10v\033[0m | \033[32m%13.2f\033[0m |\n"
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
		format = "     FAIL | \033[31m%9v\033[0m | \033[31m%14v\033[0m | \033[31m%13.2f\033[0m | \033[31m%7v\033[0m | \033[31m%10v\033[0m | \033[31m%13.2f\033[0m |\n"
		fmt.Printf(format, tr, td, ta, r, d, a)

		last = current
		lastPrintAt = tm
	}
}

func startLogger(path string) error {
	if len(path) == 0 {
		return nil
	}

	f, e := os.Create(path)
	if e != nil {
		return e
	}

	chLog = make(chan string, 100)
	wgLog.Add(1)
	go func() {
		for s := range chLog {
			if f != nil {
				f.WriteString(s)
				f.WriteString("\n")
			}
		}
		f.Close()
		wgLog.Done()
	}()

	return nil
}

func main() {
	var concurrency uint
	var logPath string
	flag.StringVar(&host, "h", "localhost", "host name or IP address of TDengine server")
	flag.UintVar(&port, "P", 0, "port (default 0)")
	flag.StringVar(&database, "d", "test", "database name")
	flag.StringVar(&user, "u", "root", "user name")
	flag.StringVar(&password, "p", "taosdata", "password")
	flag.BoolVar(&fetch, "f", true, "fetch result or not")
	flag.UintVar(&concurrency, "c", 4, "concurrency, number of goroutines for query")
	flag.StringVar(&logPath, "l", "", "path of log file (default: no log)")
	flag.Parse()

	if e := startLogger(logPath); e != nil {
		fmt.Println("failed to open log file:", e.Error())
		return
	}

	pathOrSQL := flag.Arg(0)
	if len(pathOrSQL) == 0 {
		pathOrSQL = "cases.json"
	}
	if e := loadTestCase(pathOrSQL); e != nil {
		fmt.Println("failed to load test cases:", e.Error())
		return
	}

	rand.Seed(time.Now().UnixNano())

	fmt.Printf("\nSERVER: %s    DATABASE: %s    CONCURRENCY: %d    FETCH DATA: %v\n\n", host, database, concurrency, fetch)

	startAt = time.Now()
	printStat := getStatPrinter()
	printStat(startAt)

	for i := uint(0); i < concurrency; i++ {
		wgTest.Add(1)
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
			fmt.Print("\033[4A")
			printStat(tm)
		}
	}

	atomic.StoreInt64(&shouldStop, 1)
	fmt.Print("\033[100D'Ctrl + C' received, Waiting started query to stop...")
	wgTest.Wait()

	if chLog != nil {
		close(chLog)
		wgLog.Wait()
	}
	fmt.Print("\033[4A\033[100D")
	printStat(time.Now())
	fmt.Println()
}
