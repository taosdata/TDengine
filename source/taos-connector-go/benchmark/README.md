# Benchmark for TDengine-go-driver

## Test tool

We use [hyperfine](https://github.com/sharkdp/hyperfine) to test TDengin-go-driver

## Test case

- insert
- batch insert
- query
- average

## Usage

```shell
sh run_bench.sh ${BENCHMARK_TIMES} ${BATCH_TABLES} ${BATCH_ROWS}
```

- BENCHMARK_TIMES: ${BENCHMARK_TIMES} identifies how many tests [Hyperfine](https://github.com/sharkdp/hyperfine) will
  perform.
- BATCH_TABLES: ${BENCHMARK_TIMES} identifies how many sub-tables will be used in batch insert testing case. In this
  benchmark, there are 10000 sub-tables in each super table. So this value should not greater than 10000.
- BATCH_ROWS: ${BATCH_ROWS} identifies how many rows will be inserted into each sub-table in batch insert case.
  The maximum SQL length in TDengine is 1M. Therefore, if this parameter is too large, the benchmark will fail. In this
  benchmark, this value should not greater than 5000.

example:

```shell
sh run_bench.sh 10 100 1000
```
