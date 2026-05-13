package main

import (
	"fmt"
	"time"

	"github.com/taosdata/driver-go/v3/bench/standard/executor"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func main() {
	test := executor.NewTDTest(executor.DefaultNativeDriverName, executor.DefaultNativeDSN)
	{
		test.Clean()
		test.PrepareWrite()
		s := time.Now()
		singleCount := 1000
		test.BenchmarkWriteSingleCommon(singleCount)
		writeSingleCommonCost := time.Since(s)
		fmt.Printf("write single common, count: %d,cost: %d ns,average: %f ns\n", singleCount, writeSingleCommonCost.Nanoseconds(), float64(writeSingleCommonCost.Nanoseconds())/float64(singleCount))

		test.Clean()
		test.PrepareWrite()
		batchCount := 1000
		batch := 100
		s = time.Now()
		test.BenchmarkWriteBatchJson(batchCount, batch)
		writeBatchCost := time.Since(s)
		fmt.Printf("write batch common, count: %d,cost: %d ns,average: %f ns\n", batchCount, writeBatchCost.Nanoseconds(), float64(writeBatchCost.Nanoseconds())/float64(batch*batchCount))
	}

	{
		test.PrepareWrite()
		s := time.Now()
		singleCount := 1000
		test.BenchmarkWriteSingleJson(singleCount)
		writeSingleCommonCost := time.Since(s)
		fmt.Printf("write single json, count: %d,cost: %d ns,average: %f ns\n", singleCount, writeSingleCommonCost.Nanoseconds(), float64(writeSingleCommonCost.Nanoseconds())/float64(singleCount))

		test.Clean()
		test.PrepareWrite()
		batchCount := 1000
		batch := 100
		s = time.Now()
		test.BenchmarkWriteBatchJson(batchCount, batch)
		writeBatchCost := time.Since(s)
		fmt.Printf("write batch json, count: %d,cost: %d ns,average: %f ns\n", batchCount, writeBatchCost.Nanoseconds(), float64(writeBatchCost.Nanoseconds())/float64(batch*batchCount))
	}
}
