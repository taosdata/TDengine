# TDengine Multi-thread Query Test

A simple C program to test TDengine's `taos_query_a` async query functionality with multi-threading and QPS statistics.

## Features

- Dynamic loading of TDengine library
- Multi-threaded async queries using `taos_query_a`
- Individual thread QPS calculation
- Sum of all threads QPS statistics
- Thread-safe result collection
- Random table suffix generation for load distribution

## Usage

```bash
# Compile
make all

# Run test
./taos_query_a_simple <lib_path> <host> <user> <pass> <database> <sql_prefix> [thread_count] [queries_per_thread] [sub_table_prefix]

# Examples
./taos_query_a_simple /usr/lib/libtaos.so localhost root taosdata test "select last(ts, r32) from test." 4 10 "d"
./taos_query_a_simple /usr/lib/libtaos.so localhost root taosdata test "SELECT * FROM meters" 8 5 
```

## Parameters

- `lib_path`: Path to TDengine dynamic library
- `host`: TDengine server host
- `user`: Database username
- `pass`: Database password
- `database`: Database name
- `sql_prefix`: SQL query prefix to execute
- `thread_count`: Number of threads (optional, default: 4)
- `queries_per_thread`: Number of queries per thread (optional, default: 1)
- `sub_table_prefix`: Sub-table prefix for load distribution (optional, default: "")

## Output

The program will display:
- Individual thread QPS for each thread
- Total QPS (sum of all threads QPS)
- Query execution status and timing

## Requirements

- GCC compiler
- TDengine client library
- Linux system with pthread support
- Running TDengine server