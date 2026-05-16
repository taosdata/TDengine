package main

import (
	"database/sql/driver"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/taosdata/driver-go/v3/common"
	commonstmt "github.com/taosdata/driver-go/v3/common/stmt"
	"github.com/taosdata/driver-go/v3/ws/unified"
)

var (
	dsn       = flag.String("dsn", "root:taosdata@ws(127.0.0.1:6041)/", "TDengine DSN (without db name, appended automatically)")
	duration  = flag.Duration("duration", 0, "test duration (0 = run until interrupted)")
	interval  = flag.Duration("interval", 200*time.Millisecond, "interval between iterations")
	statItvl  = flag.Duration("stat-interval", 10*time.Second, "interval between stats output")
	workers   = flag.Int("workers", 1, "number of concurrent workers")
	pprofAddr = flag.String("pprof", "127.0.0.1:6060", "pprof HTTP listen address")
)

const dbName = "unified_stability"

// buildDSN appends autoReconnect and database name to the base DSN.
// Input DSN must end with "/" (no db name).
// Returns two DSNs: one without db (for setup) and one with db (for workers).
func buildDSN(baseDSN string) (setupDSN string, workerDSN string) {
	sep := "?"
	if strings.Contains(baseDSN, "?") {
		sep = "&"
	}
	setupDSN = baseDSN + sep + "autoReconnect=true"
	// Replace trailing "/?" or "/&" pattern to insert db name before params.
	// e.g. "root:taosdata@ws(host:6041)/" -> "root:taosdata@ws(host:6041)/unified_stability"
	idx := strings.LastIndex(baseDSN, "/")
	if idx >= 0 {
		workerDSN = baseDSN[:idx+1] + dbName + baseDSN[idx+1:]
	} else {
		workerDSN = baseDSN
	}
	sep = "?"
	if strings.Contains(workerDSN, "?") {
		sep = "&"
	}
	workerDSN = workerDSN + sep + "autoReconnect=true"
	return
}

type stats struct {
	queryOK  uint64
	queryErr uint64
	stmtOK   uint64
	stmtErr  uint64
	slOK     uint64
	slErr    uint64
	iters    uint64
}

func main() {
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	// Start pprof HTTP server
	go func() {
		log.Printf("pprof listening on %s", *pprofAddr)
		if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
			log.Printf("pprof server error: %v", err)
		}
	}()

	setupDSN, workerDSN := buildDSN(*dsn)

	client, err := unified.Open(setupDSN)
	if err != nil {
		log.Fatalf("open failed: %v", err)
	}
	defer client.Close()

	if _, err = client.Exec(0, "create database if not exists "+dbName); err != nil {
		log.Fatalf("create database failed: %v", err)
	}
	if _, err = client.Exec(0, fmt.Sprintf("create table if not exists %s.t_query(ts timestamp, v int)", dbName)); err != nil {
		log.Fatalf("create table t_query failed: %v", err)
	}
	if _, err = client.Exec(0, fmt.Sprintf("create table if not exists %s.t_stmt(ts timestamp, v bigint, s binary(64))", dbName)); err != nil {
		log.Fatalf("create table t_stmt failed: %v", err)
	}
	client.Close()

	log.Printf("stability test start: workers=%d interval=%s duration=%s", *workers, *interval, *duration)
	log.Printf("  setup DSN: %s", setupDSN)
	log.Printf("  worker DSN: %s", workerDSN)

	stop := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	if *duration > 0 {
		go func() {
			time.Sleep(*duration)
			close(stop)
		}()
	}
	go func() {
		<-sig
		log.Println("signal received, shutting down...")
		close(stop)
	}()

	var s stats

	// stats printer
	go func() {
		var m runtime.MemStats
		for {
			select {
			case <-stop:
				return
			case <-time.After(*statItvl):
			}
			runtime.GC()
			runtime.ReadMemStats(&m)
			log.Printf("[stats] iters=%d query_ok=%d query_err=%d stmt_ok=%d stmt_err=%d sl_ok=%d sl_err=%d goroutines=%d alloc=%dKB sys=%dMB heap_objects=%d",
				atomic.LoadUint64(&s.iters),
				atomic.LoadUint64(&s.queryOK), atomic.LoadUint64(&s.queryErr),
				atomic.LoadUint64(&s.stmtOK), atomic.LoadUint64(&s.stmtErr),
				atomic.LoadUint64(&s.slOK), atomic.LoadUint64(&s.slErr),
				runtime.NumGoroutine(),
				m.Alloc/1024, m.Sys/1024/1024,
				m.HeapObjects,
			)
		}
	}()

	done := make(chan struct{})
	for i := 0; i < *workers; i++ {
		go func(workerID int) {
			defer func() { done <- struct{}{} }()
			runWorker(workerID, workerDSN, stop, &s)
		}(i)
	}

	for i := 0; i < *workers; i++ {
		<-done
	}

	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	log.Printf("[final] iters=%d query_ok=%d query_err=%d stmt_ok=%d stmt_err=%d sl_ok=%d sl_err=%d alloc=%dKB sys=%dMB heap_objects=%d",
		atomic.LoadUint64(&s.iters),
		atomic.LoadUint64(&s.queryOK), atomic.LoadUint64(&s.queryErr),
		atomic.LoadUint64(&s.stmtOK), atomic.LoadUint64(&s.stmtErr),
		atomic.LoadUint64(&s.slOK), atomic.LoadUint64(&s.slErr),
		m.Alloc/1024, m.Sys/1024/1024,
		m.HeapObjects,
	)
}

func runWorker(id int, workerDSN string, stop chan struct{}, s *stats) {
	client, err := unified.Open(workerDSN)
	if err != nil {
		log.Printf("[worker %d] open failed: %v", id, err)
		return
	}
	defer client.Close()

	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}

		doQuery(client, id, r, s)
		doStmt(client, id, r, s)
		doSchemaless(client, id, r, s)
		atomic.AddUint64(&s.iters, 1)
	}
}

func doQuery(client *unified.Client, id int, r *rand.Rand, s *stats) {
	ts := time.Now().Add(-time.Duration(r.Intn(86400)) * time.Second)
	tsMs := ts.UnixNano() / int64(time.Millisecond)
	v := r.Intn(100000)

	sql := fmt.Sprintf("insert into %s.t_query values(%d, %d)", dbName, tsMs, v)
	if _, err := client.Exec(0, sql); err != nil {
		atomic.AddUint64(&s.queryErr, 1)
		log.Printf("[worker %d] query insert err: %v", id, err)
		return
	}

	rows, err := client.Query(0, fmt.Sprintf("select ts, v from %s.t_query order by ts desc limit 5", dbName))
	if err != nil {
		atomic.AddUint64(&s.queryErr, 1)
		log.Printf("[worker %d] query select err: %v", id, err)
		return
	}
	count := 0
	for {
		values := make([]driver.Value, 2)
		if err = rows.Next(values); err != nil {
			if err == io.EOF {
				break
			}
			atomic.AddUint64(&s.queryErr, 1)
			log.Printf("[worker %d] query next err: %v", id, err)
			_ = rows.Close()
			return
		}
		count++
	}
	_ = rows.Close()
	atomic.AddUint64(&s.queryOK, 1)
}

func doStmt(client *unified.Client, id int, r *rand.Rand, s *stats) {
	stmt, err := client.InitStmt(0)
	if err != nil {
		atomic.AddUint64(&s.stmtErr, 1)
		log.Printf("[worker %d] stmt init err: %v", id, err)
		return
	}
	defer func() { _ = stmt.Close(0) }()

	if err = stmt.Prepare(0, fmt.Sprintf("insert into %s.t_stmt values(?,?,?)", dbName)); err != nil {
		atomic.AddUint64(&s.stmtErr, 1)
		log.Printf("[worker %d] stmt prepare err: %v", id, err)
		return
	}

	batchSize := 3 + r.Intn(8)
	tsCols := make([]driver.Value, batchSize)
	vCols := make([]driver.Value, batchSize)
	sCols := make([]driver.Value, batchSize)
	baseTs := time.Now().UTC().Round(time.Millisecond)
	for i := 0; i < batchSize; i++ {
		tsCols[i] = baseTs.Add(time.Duration(i) * time.Millisecond)
		if r.Intn(5) == 0 {
			vCols[i] = nil
		} else {
			vCols[i] = int64(r.Intn(1000000))
		}
		sCols[i] = fmt.Sprintf("w%d_r%d", id, r.Intn(10000))
	}

	if err = stmt.Bind([]*commonstmt.TaosStmt2BindData{
		{Cols: [][]driver.Value{tsCols, vCols, sCols}},
	}); err != nil {
		atomic.AddUint64(&s.stmtErr, 1)
		log.Printf("[worker %d] stmt bind err: %v", id, err)
		return
	}

	affected, err := stmt.Exec(0)
	if err != nil {
		atomic.AddUint64(&s.stmtErr, 1)
		log.Printf("[worker %d] stmt exec err: %v", id, err)
		return
	}
	if affected != batchSize {
		atomic.AddUint64(&s.stmtErr, 1)
		log.Printf("[worker %d] stmt affected mismatch: want=%d got=%d", id, batchSize, affected)
		return
	}
	atomic.AddUint64(&s.stmtOK, 1)
}

func doSchemaless(client *unified.Client, id int, r *rand.Rand, s *stats) {
	tsMs := time.Now().UnixNano() / int64(time.Millisecond)
	line := fmt.Sprintf("sl_meters,worker=%d current=%di32,voltage=%di32 %d",
		id, r.Intn(100), 200+r.Intn(40), tsMs)

	if err := client.SchemalessInsert(common.GetReqID(), line, unified.InfluxDBLineProtocol, "ms", 0, ""); err != nil {
		atomic.AddUint64(&s.slErr, 1)
		log.Printf("[worker %d] schemaless insert err: %v", id, err)
		return
	}
	atomic.AddUint64(&s.slOK, 1)
}
