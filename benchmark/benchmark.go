package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

const (
	taosDb         = "root:taosdata@tcp(localhost:6030)/"
	benchmarkDb    = "benchmark"
	insertCmd      = "insert"
	batchInsertCmd = "batch"
	queryCmd       = "query"
	avgCmd         = "avg"
	normalType     = "normal"
	jsonType       = "json"
	stb            = "stb"
	jtb            = "jtb"
	queryStb       = "select * from stb"
	queryJtb       = "select ts, bl, i8, i16, i32, i64, u8, u16, u32, u64, f32, d64, bnr, nchr, jtag->\"k0\", jtag->\"k1\", jtag->\"k2\", jtag->\"k3\" from jtb;"
	avgStbSql      = "select avg(d64) from stb"
	avgJtbSql      = "select avg(d64) from jtb"
	maxTableCnt    = 10000
)

func main() {
	ctx := context.Background()
	cmd := flag.String("s", "connect", "Benchmark stage, \"connect\",\"insert\",\"query\",\"avg\",\"batch\",\"clean\",default \"connect\\\"")
	types := flag.String("t", "normal", "Benchmark data type, table with\"json\" tag,table with \"normal\" column type,default \"normal\"")
	tableCount := flag.Int("b", 1, "number of target tables,only for insert.Default 1 tables")
	numOfRow := flag.Int("r", 1, "number of record per table,only for insert.Default 1 records")
	times := flag.Int("n", 1, "number of times to run.Default 1 time.")
	debug := flag.Bool("debug", false, "debug model")
	flag.Parse()

	if *debug {
		log.Printf("[debug] benchmark:\n cmd-[%s]\n types-[%s]\n table count-[%d]\n num of row-[%d]\n execute times-[%d]\n",
			*cmd, *types, *tableCount, *numOfRow, *times)
	}

	tableCnt := *tableCount
	if tableCnt > maxTableCnt {
		tableCnt = maxTableCnt
	}

	b, err := newBench(taosDb)
	panicIf("init connection ", err)
	defer b.close()

	useCmd := fmt.Sprintf("use %s", benchmarkDb)
	_, err = b.taos.Exec(useCmd)
	panicIf(useCmd, err)

	switch *cmd {
	case insertCmd:
		b.insert(ctx, *types, tableCnt)
	case batchInsertCmd:
		b.batchInsert(ctx, *types, tableCnt, *numOfRow)
	case queryCmd:
		b.query(ctx, *types, *times)
	case avgCmd:
		b.average(ctx, *types, *times)
	}
}

type bench struct {
	taos *sql.DB
}

func newBench(dbUrl string) (*bench, error) {
	taos, err := sql.Open("taosSql", dbUrl)
	return &bench{taos: taos}, err
}

func (b *bench) close() {
	_ = b.taos.Close()
}

func (b *bench) insert(ctx context.Context, types string, tableCnt int) {
	table := stb
	if types == jsonType {
		table = jtb
	}
	begin := time.Now().UnixNano() / int64(time.Millisecond)

	for i := 0; i < tableCnt; i++ {
		_, err := b.taos.ExecContext(ctx,
			fmt.Sprintf(
				"insert into %s_%d values(%d, true, -1, -2, -3, -4, 1, 2, 3, 4, 3.1415, 3.14159265358979, 'bnr_col_1', 'ncr_col_1')",
				table,
				i,
				begin))
		panicIf("single insert", err)
	}
}

func (b *bench) batchInsert(ctx context.Context, types string, tableCnt, numOfRows int) {
	table := stb
	if types == jsonType {
		table = jtb
	}
	begin := time.Now().UnixNano() / int64(time.Millisecond)

	for i := 0; i < tableCnt; i++ {
		tableName := fmt.Sprintf("%s_%d", table, i)
		batchSql := batchInsertSql(begin, tableName, numOfRows)
		_, err := b.taos.ExecContext(ctx, batchSql)
		panicIf("batch insert", err)
	}
}

func (b *bench) query(ctx context.Context, types string, times int) {
	if types == normalType {
		for i := 0; i < times; i++ {
			rs, err := b.taos.QueryContext(ctx, queryStb)
			panicIf("query normal", err)
			readStbRow(rs)
		}
		return
	}

	if types == jsonType {
		for i := 0; i < times; i++ {
			rs, err := b.taos.QueryContext(ctx, queryJtb)
			panicIf("query json", err)
			readJtbRow(rs)
		}
	}
}

func (b *bench) average(ctx context.Context, types string, times int) {
	query := avgStbSql
	if types == jsonType {
		query = avgJtbSql
	}

	for i := 0; i < times; i++ {
		rs, err := b.taos.QueryContext(ctx, query)
		panicIf("average", err)

		for rs.Next() {
			var avg float64
			err = rs.Scan(&avg)
			panicIf("scan average data", err)
		}
	}
}

func readStbRow(rs *sql.Rows) {
	defer func() { _ = rs.Close() }()

	for rs.Next() {
		var (
			ts    time.Time
			bl    bool
			i8    int8
			i16   int16
			i32   int32
			i64   int64
			u8    uint8
			u16   uint16
			u32   uint32
			u64   uint64
			f32   float32
			d64   float64
			bnr   string
			nchar string
			t0    bool
			t1    uint8
			t2    uint16
			t3    uint32
			t4    uint64
			t5    int8
			t6    int16
			t7    int32
			t8    int64
			t9    float32
			t10   float64
			t11   string
			t12   string
		)
		err := rs.Scan(&ts, &bl, &i8, &i16, &i32, &i64, &u8, &u16, &u32, &u64, &f32, &d64, &bnr, &nchar, &t0, &t1, &t2,
			&t3, &t4, &t5, &t6, &t7, &t8, &t9, &t10, &t11, &t12)
		panicIf("read row", err)
	}
}

func readJtbRow(rs *sql.Rows) {
	defer func() { _ = rs.Close() }()

	for rs.Next() {
		var (
			ts    time.Time
			bl    bool
			i8    int8
			i16   int16
			i32   int32
			i64   int64
			u8    uint8
			u16   uint16
			u32   uint32
			u64   uint64
			f32   float32
			d64   float64
			bnr   string
			nchar string
			k0    string
			k1    string
			k2    string
			k3    string
		)
		err := rs.Scan(&ts, &bl, &i8, &i16, &i32, &i64, &u8, &u16, &u32, &u64, &f32, &d64, &bnr, &nchar, &k0, &k1, &k2, &k3)
		panicIf("read row", err)
	}
}

func batchInsertSql(begin int64, table string, numOfRows int) string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "insert into %s values ", table)

	for i := 0; i < numOfRows; i++ {
		fmt.Fprintf(&buffer, "(%d, %t, %d, %d, %d, %d, %d, %d, %d, %d, %.4f, %f, '%s', '%s')",
			begin+int64(i),
			rand.Intn(2) == 1,      // bl
			rand.Intn(256)-128,     // i8 [-128, 127]
			rand.Intn(65535)-32768, // i16 [-32768, 32767]
			rand.Int31(),           // i32 [-2^31, 2^31-1]
			rand.Int63(),           // i64 [-2^63, 2^63-1]
			rand.Intn(256),         // u8
			rand.Intn(65535),       // u16
			rand.Uint32(),          // u32
			rand.Uint64(),          // u64
			rand.Float32(),         // f32
			rand.Float64(),         // d64
			randStr(20),            // bnr
			randStr(20),            // nchr
		)
	}
	buffer.WriteString(";")
	return buffer.String()
}

const chars = "01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStr(n int) string {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		r := rand.Intn(63)
		buf.WriteString(chars[r : r+1])
	}
	return buf.String()
}

func panicIf(msg string, err error) {
	if err != nil {
		panic(fmt.Errorf("%s %v", msg, err))
	}
}
