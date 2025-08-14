# TDengine Query Test Tool

A C program to test TDengine's query functionality with multi-threading and performance analysis.

## Features

- Multi-threaded query execution using `taos_query_a`
- Performance testing with QPS statistics
- Support for different query patterns
- Thread-safe result collection
- Configurable test parameters

## Usage

### Compilation

```bash
make all
```

### Basic Usage

```bash
./taos_query_a_simple <test_mode>
```

### Test Modes

#### Mode 1: Table Name IN Query Test

```bash
./taos_query_a_simple 1
```

**SQL Query:**

```sql
SELECT tbname, last(*) 
FROM test.meters 
WHERE tbname IN ('d1','d2','d3','d4','d5','d6','d7','d8','d9','d10',
                 'd11','d12','d13','d14','d15','d16','d17','d18','d19','d20',
                 'd21','d22','d23','d24','d25','d26','d27','d28','d29','d30',
                 'd31','d32','d33','d34','d35','d36','d37','d38','d39','d40',
                 'd41','d42','d43','d44','d45','d46','d47','d48','d49','d50',
                 'd51','d52','d53','d54','d55','d56','d57','d58','d59','d60',
                 'd61','d62','d63','d64','d65','d66','d67','d68','d69','d70',
                 'd71','d72','d73','d74','d75','d76','d77','d78','d79','d80',
                 'd81','d82','d83','d84','d85','d86','d87','d88','d89','d90',
                 'd91','d92','d93','d94','d95','d96','d97','d98','d99','d100') 
PARTITION BY tbname;
```

**Parameters:**

- **Thread Count**: 16 threads
- **Queries per Thread**: 100 queries
- **Max Concurrent Queries**: 5 per thread
- **QPS Rate Multiplier**: 100x
- **Purpose**: Tests query performance with `tbname IN` condition filtering multiple tables

#### Mode 2: Sub-table Query Test

```bash
./taos_query_a_simple 2
```

**SQL Query:**

```sql
SELECT last(ts, r32) FROM test.d1;
```

**Parameters:**

- **Thread Count**: 16 threads
- **Queries per Thread**: 10,000 queries
- **Max Concurrent Queries**: 5 per thread
- **QPS Rate Multiplier**: 1x
- **Purpose**: Tests query performance on a single sub-table

## Connection Parameters

Both modes use the same connection settings:

- **Library Path**: `../../debug/build/lib/libtaos.so`
- **Host**: `127.0.0.1`
- **User**: `root`
- **Password**: `taosdata`
- **Database**: `test`

## Performance Metrics

The program measures and reports:

- **Individual Thread QPS**: Performance of each thread
- **Total QPS**: Sum of all threads' QPS (multiplied by rate factor)
- **Query Execution Time**: Duration for completing all queries
- **Concurrent Query Management**: Controls max concurrent queries per thread

## Examples

```bash
# Run table name IN query test (Mode 1)
./taos_query_a_simple 1

# Run sub-table query test (Mode 2)
./taos_query_a_simple 2
```

## Test Scenarios

### Mode 1: Table Name IN Query Testing

Tests query performance with `tbname IN` conditions:
- Queries 100 specific tables using `tbname IN` clause
- Uses `PARTITION BY tbname` for grouping
- Tests filtering performance on multiple tables

### Mode 2: Sub-table Query Testing

Tests standard query performance on sub-tables:
- Simple aggregation query on a single sub-table (`test.d1`)
- High query volume (10,000 queries per thread)
- Baseline performance measurement for sub-table queries

## Output

The program displays:
- Individual thread performance metrics
- Overall system performance statistics
- Query execution status and timing
- Error messages if any issues occur

## Requirements

- GCC compiler
- TDengine client library
- Linux system with pthread support
- Running TDengine server
- Proper database connection configuration

## Configuration

Make sure your TDengine server is running and accessible before executing the test program. The program will connect to the default TDengine instance unless modified in the source code.
