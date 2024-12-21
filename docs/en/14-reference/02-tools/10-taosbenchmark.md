---
title: taosBenchmark Reference
sidebar_label: taosBenchmark
slug: /tdengine-reference/tools/taosbenchmark
---

TaosBenchmark is a performance benchmarking tool for TDengine products, providing insertion, query, and subscription performance testing for TDengine products, and outputting performance indicators.

## Installation

taosBenchmark provides two installation methods:

- taosBenchmark is the default installation component in the TDengine installation package, which can be used after installing TDengine. For how to install TDengine, please refer to [TDengine Installation](../../../get started/)

- Compile and install taos tools separately, refer to [taos tools](https://github.com/taosdata/taos-tools) .

## Operation

### Configuration and Operation Methods

taosBbenchmark supports three operating modes:
- No parameter mode
- Command line mode
- JSON configuration file mode
The command-line approach is a subset of the functionality of JSON configuration files, which immediately uses the command line and then the configuration file, with the parameters specified by the command line taking precedence.

**Ensure that the TDengine cluster is running correctly before running taosBenchmark.**

### Running Without Command Line Arguments


Execute the following command to quickly experience taosBenchmark performing a write performance test on TDengine based on the default configuration.
```shell
taosBenchmark
```

When running without parameters, taosBenchmark defaults to connecting to the TDengine cluster specified in `/etc/taos/taos.cfg `.
After successful connection, a smart meter example database test, super meters, and 10000 sub meters will be created, with 10000 records per sub meter. If the test database already exists, it will be deleted before creating a new one.

### Running Using Command Line Configuration Parameters

When running taosBenchmark using command line parameters and controlling its behavior, the `-f <json file>` parameter cannot be used. All configuration parameters must be specified through the command line. Below is an example of using command line mode to test the write performance of taosBenchmark.

```shell
taosBenchmark -I stmt -n 200 -t 100
```

The above command `taosBenchmark` will create a database named `test`, establish a supertable `meters` within it, create 100 subtables in the supertable, and insert 200 records into each subtable using parameter binding.

### Running Using a Configuration File

Running in configuration file mode provides all functions, so parameters can be configured to run in the configuration file.  

```shell
taosBenchmark -f <json file>
```

**Below are a few examples of configuration files:**

#### JSON Configuration File Example for Insertion Scenario

<details>
<summary>insert.json</summary>

```json
{{#include /taos-tools/example/insert.json}}
```

</details>

#### Example JSON Configuration File for Query Scenario

<details>
<summary>query.json</summary>

```json
{{#include /taos-tools/example/query.json}}
```

</details>

#### Example JSON Configuration File for Subscription Scenario

<details>
<summary>tmq.json</summary>

```json
{{#include /taos-tools/example/tmq.json}}
```

</details>

## Detailed Explanation of Command Line Parameters

- **-f/--file \<json file>** :
  The JSON configuration file to use, specifying all parameters. This parameter cannot be used simultaneously with other command line parameters. There is no default value.

- **-c/--config-dir \<dir>** :
  The directory where the TDengine cluster configuration files are located, default path is /etc/taos.

- **-h/--host \<host>** :
  Specifies the FQDN of the TDengine server to connect to, default value is localhost.

- **-P/--port \<port>** :
  The port number of the TDengine server to connect to, default value is 6030.

- **-I/--interface \<insertMode>** :
  Insert mode, options include taosc, rest, stmt, sml, sml-rest, corresponding to normal writing, restful interface writing, parameter binding interface writing, schemaless interface writing, restful schemaless interface writing (provided by taosAdapter). Default value is taosc.

- **-u/--user \<user>** :
  Username for connecting to the TDengine server, default is root.

- **-U/--supplement-insert** :
  Insert data without pre-creating database and tables, default is off.

- **-p/--password \<passwd>** :
  Password for connecting to the TDengine server, default value is taosdata.

- **-o/--output \<file>** :
  Path of the output file for results, default value is ./output.txt.

- **-T/--thread \<threadNum>** :
  Number of threads for inserting data, default is 8.

- **-B/--interlace-rows \<rowNum>** :
  Enables interlaced insertion mode and specifies the number of rows to insert into each subtable at a time. Interlaced insertion mode means inserting the specified number of rows into each subtable in sequence and repeating this process until all subtable data is inserted. Default value is 0, meaning data is inserted into one subtable completely before moving to the next.

- **-i/--insert-interval \<timeInterval>** :
  Specifies the insertion interval for interlaced insertion mode, in ms, default value is 0. Only effective when `-B/--interlace-rows` is greater than 0. It means that the data insertion thread will wait for the time interval specified by this value after inserting interlaced records for each subtable before proceeding to the next round of writing.

- **-r/--rec-per-req \<rowNum>** :
  Number of data rows requested per TDengine write, default value is 30000.

- **-t/--tables \<tableNum>** :
  Specifies the number of subtables, default is 10000.

- **-S/--timestampstep \<stepLength>** :
  Timestamp step length for inserting data into each subtable, in ms, default value is 1.

- **-n/--records \<recordNum>** :
  Number of records to insert per subtable, default value is 10000.

- **-d/--database \<dbName>** :
  Name of the database to use, default value is test.

- **-b/--data-type \<colType>** :
  Data column types for the supertable. If not used, the default is three data columns, types are FLOAT, INT, FLOAT.

- **-l/--columns \<colNum>** :
  Total number of data columns for the supertable. If both this parameter and `-b/--data-type` are set, the final number of columns is the larger of the two. If the number specified by this parameter is greater than the number of columns specified by `-b/--data-type`, the unspecified column types default to INT, for example: `-l 5 -b float,double`, then the final columns are `FLOAT,DOUBLE,INT,INT,INT`. If the number of columns specified is less than or equal to the number specified by `-b/--data-type`, the result is the columns and types specified by `-b/--data-type`, for example: `-l 3 -b float,double,float,bigint`, then the final columns are `FLOAT,DOUBLE,FLOAT,BIGINT`.

- **-L/--partial-col-num \<colNum>**:
  Specifies that only some columns are written with data, while other columns are NULL. By default, data is written to all columns.

- **-A/--tag-type \<tagType>**:
  Tag column types for the supertable. `nchar` and `binary` types can set length simultaneously, for example:

```shell
taosBenchmark -A INT,DOUBLE,NCHAR,BINARY(16)
```

If no tag types are set, the default is two tags, with types INT and BINARY(16).
Note: In some shells like bash, "()" needs to be escaped, so the command should be:

```shell
taosBenchmark -A INT,DOUBLE,NCHAR,BINARY\(16\)
```

- **-w/--binwidth \<length>**:
  The default length for `nchar` and `binary` types, default value is 64.

- **-m/--table-prefix \<tablePrefix>**:
  The prefix for the subtable names, default value is "d".

- **-E/--escape-character**:
  A toggle parameter, specifies whether to use escape characters in supertable and subtable names. The default is not to use.

- **-C/--chinese**:
  A toggle parameter, specifies whether `nchar` and `binary` use Unicode Chinese characters. The default is not to use.

- **-N/--normal-table**:
  A toggle parameter, specifies to only create basic tables, not supertables. Default is false. Only available when the insertion mode is taosc, stmt, or rest.

- **-M/--random**:
  A toggle parameter, data to be inserted are generated random values. Default is false. If this parameter is configured, the data to be inserted will be randomly generated. For numeric type tag/columns, the values will be random within the type's range. For NCHAR and BINARY type tag/columns, the values will be random strings within the specified length range.

- **-x/--aggr-func**:
  A toggle parameter, indicates to query aggregate functions after insertion. Default is false.

- **-y/--answer-yes**:
  A toggle parameter, requires user confirmation after prompt to continue. Default is false.

- **-O/--disorder \<Percentage>**:
  Specifies the percentage probability of disorder data, with a range of [0,50]. Default is 0, meaning no disorder data.

- **-R/--disorder-range \<timeRange>**:
  Specifies the timestamp fallback range for disorder data. The generated disorder timestamps are the timestamps that should be used in non-disorder situations minus a random value within this range. Only effective when the disorder data percentage specified by `-O/--disorder` is greater than 0.

- **-F/--prepare_rand \<Num>**:
  The number of unique values in the generated random data. If it is 1, it means all data are the same. Default value is 10000.

- **-a/--replica \<replicaNum>**:
  Specifies the number of replicas when creating the database, default value is 1.

- **-k/--keep-trying \<NUMBER>**: Number of retries after failure, default is no retry. Requires version v3.0.9 or above.

- **-z/--trying-interval \<NUMBER>**: Retry interval in milliseconds, effective only when retries are specified with -k. Requires version v3.0.9 or above.

- **-v/--vgroups \<NUMBER>**:
  Specifies the number of vgroups when creating the database, only valid for TDengine v3.0+.

- **-V/--version**:
  Displays version information and exits. Cannot be used with other parameters.

- **-?/--help**:
  Displays help information and exits. Cannot be used with other parameters.


## Output performance indicators

#### Write indicators

After writing is completed, a summary performance metric will be output in the last two lines in the following format:
``` bash
SUCC: Spent 8.527298 (real 8.117379) seconds to insert rows: 10000000 with 8 thread(s) into test 1172704.41 (real 1231924.74) records/second
SUCC: insert delay, min: 19.6780ms, avg: 64.9390ms, p90: 94.6900ms, p95: 105.1870ms, p99: 130.6660ms, max: 157.0830ms
```
First line write speed statistics:
- Spent: Total write time, in seconds, counting from the start of writing the first data to the end of the last data. This indicates that a total of 8.527298 seconds were spent
- Real: Total write time (calling the engine), excluding the time spent preparing data for the testing framework. Purely counting the time spent on engine calls, The time spent is 8.117379 seconds. If 8.527298-8.117379=0.409919 seconds, it is the time spent preparing data for the testing framework
- Rows: Write the total number of rows, which is 10 million pieces of data
- Threads: The number of threads being written, which is 8 threads writing simultaneously
- Records/second write speed = `total write time` / `total number of rows written`, real in parentheses is the same as before, indicating pure engine write speed

Second line single write delay statistics:  
- min: Write minimum delay
- avg: Write normal delay
- p90: Write delay p90 percentile delay number
- p95: Write delay p95 percentile delay number
- p99: Write delay p99 percentile delay number
- max: maximum write delay
Through this series of indicators, the distribution of write request latency can be observed

#### Query indicators
The query performance test mainly outputs the QPS indicator of query request speed, and the output format is as follows:

``` bash
complete query with 3 threads and 10000 query delay avg: 	0.002686s min: 	0.001182s max: 	0.012189s p90: 	0.002977s p95: 	0.003493s p99: 	0.004645s SQL command: select ...
INFO: Total specified queries: 30000
INFO: Spend 26.9530 second completed total queries: 30000, the QPS of all threads: 1113.049
```

- The first line represents the percentile distribution of query execution and query request delay for each of the three threads executing 10000 queries. The SQL command is the test query statement
- The second line indicates that a total of 10000 * 3 = 30000 queries have been completed
- The third line indicates that the total query time is 26.9653 seconds, and the query rate per second (QPS) is 1113.049 times/second

#### Subscription metrics

The subscription performance test mainly outputs consumer consumption speed indicators, with the following output format:
``` bash
INFO: consumer id 0 has poll total msgs: 376, period rate: 37.592 msgs/s, total rows: 3760000, period rate: 375924.815 rows/s
INFO: consumer id 1 has poll total msgs: 362, period rate: 36.131 msgs/s, total rows: 3620000, period rate: 361313.504 rows/s
INFO: consumer id 2 has poll total msgs: 364, period rate: 36.378 msgs/s, total rows: 3640000, period rate: 363781.731 rows/s
INFO: consumerId: 0, consume msgs: 1000, consume rows: 10000000
INFO: consumerId: 1, consume msgs: 1000, consume rows: 10000000
INFO: consumerId: 2, consume msgs: 1000, consume rows: 10000000
INFO: Consumed total msgs: 3000, total rows: 30000000
```
- Lines 1 to 3 real-time output of the current consumption speed of each consumer, msgs/s represents the number of consumption messages, each message contains multiple rows of data, and rows/s represents the consumption speed calculated by rows
- Lines 4 to 6 show the overall statistics of each consumer after the test is completed, including the total number of messages consumed and the total number of lines
- The overall statistics of all consumers in line 7, `msgs` represents how many messages were consumed in total, `rows` represents how many rows of data were consumed in total

## Configuration File Parameters Detailed Explanation

### General Configuration Parameters

The parameters listed in this section apply to all functional modes.

- **filetype**: The function to test, possible values are `insert`, `query`, and `subscribe`. Corresponding to insert, query, and subscribe functions. Only one can be specified in each configuration file.
- **cfgdir**: Directory where the TDengine client configuration file is located, default path is /etc/taos.

- **host**: Specifies the FQDN of the TDengine server to connect to, default value is localhost.

- **port**: The port number of the TDengine server to connect to, default value is 6030.

- **user**: Username for connecting to the TDengine server, default is root.

- **password** : Password for connecting to the TDengine server, default value is taosdata.

### Configuration Parameters for Insertion Scenarios

In insertion scenarios, `filetype` must be set to `insert`. For this parameter and other common parameters, see Common Configuration Parameters.

- **keep_trying**: Number of retries after failure, default is no retry. Requires version v3.0.9 or above.

- **trying_interval**: Interval between retries in milliseconds, effective only when retries are specified in keep_trying. Requires version v3.0.9 or above.
- **childtable_from and childtable_to**: Specifies the range of child tables to write to, the interval is [childtable_from, childtable_to).

- **continue_if_fail**: Allows users to define behavior after failure

  “continue_if_fail”:  “no”, taosBenchmark exits automatically upon failure, default behavior
  “continue_if_fail”: “yes”, taosBenchmark warns the user and continues writing
  “continue_if_fail”: “smart”, if the child table does not exist upon failure, taosBenchmark will create the child table and continue writing

#### Database Related Configuration Parameters

Parameters related to database creation are configured in the `dbinfo` section of the json configuration file, specific parameters are as follows. Other parameters correspond to those specified in TDengine's `create database`, see [../../taos-sql/database]

- **name**: Database name.

- **drop**: Whether to delete the database before insertion, options are "yes" or "no", "no" means do not create. Default is to delete.

#### Stream Computing Related Configuration Parameters

Parameters related to stream computing are configured in the `stream` section of the json configuration file, specific parameters are as follows.

- **stream_name**: Name of the stream computing, mandatory.

- **stream_stb**: Name of the supertable corresponding to the stream computing, mandatory.

- **stream_sql**: SQL statement for the stream computing, mandatory.

- **trigger_mode**: Trigger mode for the stream computing, optional.

- **watermark**: Watermark for the stream computing, optional.

- **drop**: Whether to create stream computing, options are "yes" or "no", "no" means do not create.

#### Supertable Related Configuration Parameters

Parameters related to supertable creation are configured in the `super_tables` section of the json configuration file, specific parameters are as follows.

- **name**: Supertable name, must be configured, no default value.

- **child_table_exists**: Whether the child table already exists, default is "no", options are "yes" or "no".

- **child_table_count**: Number of child tables, default is 10.

- **child_table_prefix**: Prefix for child table names, mandatory, no default value.

- **escape_character**: Whether the supertable and child table names contain escape characters, default is "no", options are "yes" or "no".

- **auto_create_table**: Effective only when insert_mode is taosc, rest, stmt and child_table_exists is "no", "yes" means taosBenchmark will automatically create non-existent tables during data insertion; "no" means all tables are created in advance before insertion.

- **batch_create_tbl_num**: Number of tables created per batch during child table creation, default is 10. Note: The actual number of batches may not match this value, if the executed SQL statement exceeds the maximum supported length, it will be automatically truncated and executed, continuing the creation.

- **data_source**: Source of the data, default is randomly generated by taosBenchmark, can be configured as "rand" and "sample". For "sample", use the file specified by the sample_file parameter.

- **insert_mode**: Insertion mode, options include taosc, rest, stmt, sml, sml-rest, corresponding to normal writing, restful interface writing, parameter binding interface writing, schemaless interface writing, restful schemaless interface writing (provided by taosAdapter). Default is taosc.

- **non_stop_mode**: Specifies whether to continue writing, if "yes" then insert_rows is ineffective, writing stops only when Ctrl + C stops the program. Default is "no", i.e., stop after writing a specified number of records. Note: Even in continuous writing mode, insert_rows must still be configured as a non-zero positive integer.

- **line_protocol** : Use line protocol to insert data, effective only when insert_mode is sml or sml-rest, options include line, telnet, json.

- **tcp_transfer** : Communication protocol in telnet mode, effective only when insert_mode is sml-rest and line_protocol is telnet. If not configured, the default is the http protocol.

- **insert_rows** : The number of records inserted per subtable, default is 0.

- **childtable_offset** : Effective only when child_table_exists is yes, specifies the offset when getting the subtable list from the supertable, i.e., starting from which subtable.

- **childtable_limit** : Effective only when child_table_exists is yes, specifies the limit when getting the subtable list from the supertable.

- **interlace_rows** : Enables interlaced insertion mode and specifies the number of rows to insert into each subtable at a time. Interlaced insertion mode means inserting the number of rows specified by this parameter into each subtable in turn and repeating this process until all subtable data is inserted. The default value is 0, i.e., data is inserted into one subtable before moving to the next subtable.

- **insert_interval** : Specifies the insertion interval for interlaced insertion mode, in ms, default value is 0. Only effective when `-B/--interlace-rows` is greater than 0. It means that the data insertion thread will wait for the time interval specified by this value after inserting interlaced records for each subtable before proceeding to the next round of writing.

- **partial_col_num** : If this value is a positive number n, then only the first n columns are written to, effective only when insert_mode is taosc and rest, if n is 0 then all columns are written to.

- **disorder_ratio** : Specifies the percentage probability of out-of-order data, its value range is [0,50]. Default is 0, i.e., no out-of-order data.

- **disorder_range** : Specifies the timestamp rollback range for out-of-order data. The generated out-of-order timestamp is the timestamp that should be used under non-out-of-order conditions minus a random value within this range. Only effective when `-O/--disorder` specifies a disorder data percentage greater than 0.

- **timestamp_step** : The timestamp step for inserting data into each subtable, unit consistent with the database's `precision`, default value is 1.

- **start_timestamp** : The starting timestamp for each subtable, default value is now.

- **sample_format** : The type of sample data file, currently only supports "csv".

- **sample_file** : Specifies a csv format file as the data source, effective only when data_source is sample. If the number of data rows in the csv file is less than or equal to prepared_rand, then the csv file data will be read in a loop until it matches prepared_rand; otherwise, only prepared_rand number of rows will be read. Thus, the final number of data rows generated is the smaller of the two.

- **use_sample_ts** : Effective only when data_source is sample, indicates whether the csv file specified by sample_file contains the first column timestamp, default is no. If set to yes, then use the first column of the csv file as the timestamp, since the same subtable timestamp cannot be repeated, the amount of data generated depends on the same number of data rows in the csv file, at this time insert_rows is ineffective.

- **tags_file** : Effective only when insert_mode is taosc, rest. The final value of the tag is related to childtable_count, if the tag data rows in the csv file are less than the given number of subtables, then the csv file data will be read in a loop until the childtable_count specified subtable number is generated; otherwise, only childtable_count rows of tag data will be read. Thus, the final number of subtables generated is the smaller of the two.

- **primary_key** : Specifies whether the supertable has a composite primary key, values are 1 and 0, composite primary key columns can only be the second column of the supertable, after specifying the generation of composite primary keys, ensure that the second column meets the data type of composite primary keys, otherwise an error will occur
- **repeat_ts_min** : Numeric type, when composite primary key is enabled, specifies the minimum number of records with the same timestamp to be generated, the number of records with the same timestamp is a random value within the range [repeat_ts_min, repeat_ts_max], when the minimum value equals the maximum value, it is a fixed number
- **repeat_ts_max** : Numeric type, when composite primary key is enabled, specifies the maximum number of records with the same timestamp to be generated
- **sqls** : Array of strings type, specifies the array of sql to be executed after the supertable is successfully created, the table name specified in sql must be prefixed with the database name, otherwise an unspecified database error will occur


#### Tag and Data Column Configuration Parameters

Specify the configuration parameters for tag and data columns in `super_tables` under `columns` and `tag`.

- **type**: Specifies the column type, refer to the data types supported by TDengine.
  Note: The JSON data type is special, it can only be used for tags, and when using JSON type as a tag, there must be only this one tag. In this case, count and len represent the number of key-value pairs in the JSON tag and the length of each KV pair's value respectively, with value defaulting to string.

- **len**: Specifies the length of the data type, effective for NCHAR, BINARY, and JSON data types. If configured for other data types, if it is 0, it means the column is always written with a null value; if not 0, it is ignored.

- **count**: Specifies the number of times this type of column appears consecutively, for example, "count": 4096 can generate 4096 columns of the specified type.

- **name**: The name of the column, if used with count, for example "name": "current", "count":3, then the names of the 3 columns are current, current_2, current_3 respectively.

- **min**: The minimum value for the data type of the column/tag. Generated values will be greater than or equal to the minimum value.

- **max**: The maximum value for the data type of the column/tag. Generated values will be less than the minimum value.

- **scalingFactor**: Floating-point precision enhancement factor, only effective when the data type is float/double, valid values range from 1 to 1000000 positive integers. Used to enhance the precision of generated floating points, especially when min or max values are small. This attribute enhances the precision after the decimal point by powers of 10: a scalingFactor of 10 means enhancing the precision by 1 decimal place, 100 means 2 places, and so on.

- **fun**: This column data is filled with functions, currently only supports sin and cos functions, input parameters are converted from timestamps to angle values, conversion formula: angle x = input time column ts value % 360. Also supports coefficient adjustment, random fluctuation factor adjustment, displayed in a fixed format expression, such as fun="10*sin(x)+100*random(5)", x represents the angle, ranging from 0 ~ 360 degrees, the increment step is consistent with the time column step. 10 represents the multiplication coefficient, 100 represents the addition or subtraction coefficient, 5 represents the fluctuation amplitude within a 5% random range. Currently supports int, bigint, float, double four data types. Note: The expression is in a fixed pattern and cannot be reversed.

- **values**: The value domain for nchar/binary column/tag, will randomly select from the values.

- **sma**: Adds this column to SMA, value is "yes" or "no", default is "no".

- **encode**: String type, specifies the first level encoding algorithm for this column in two-level compression, see creating supertables for details.

- **compress**: String type, specifies the second level encryption algorithm for this column in two-level compression, see creating supertables for details.

- **level**: String type, specifies the compression rate of the second level encryption algorithm for this column in two-level compression, see creating supertables for details.

- **gen**: String type, specifies the method of generating data for this column, if not specified it is random, if specified as "order", it will increase sequentially by natural numbers.

- **fillNull**: String type, specifies whether this column randomly inserts NULL values, can be specified as "true" or "false", only effective when generate_row_rule is 2.

#### Insertion Behavior Configuration Parameters

- **thread_count**: The number of threads for inserting data, default is 8.

- **thread_bind_vgroup**: Whether the vgroup is bound with the writing thread during writing, binding can improve writing speed, values are "yes" or "no", default is "no". Set to "no" to maintain the same behavior as before. When set to "yes", if the thread_count is equal to the number of vgroups in the database, thread_count is automatically adjusted to the number of vgroups; if thread_count is less than the number of vgroups, the number of writing threads is not adjusted, one thread writes one vgroup data after another, while maintaining the rule that only one thread can write into a vgroup at a time.

- **create_table_thread_count** : The number of threads for creating tables, default is 8.

- **connection_pool_size** : The number of pre-established connections with the TDengine server. If not configured, it defaults to the specified number of threads.

- **result_file** : The path to the result output file, default is ./output.txt.

- **confirm_parameter_prompt** : A toggle parameter that requires user confirmation after a prompt to continue. The default value is false.

- **interlace_rows** : Enables interleaved insertion mode and specifies the number of rows to insert into each subtable at a time. Interleaved insertion mode refers to inserting the specified number of rows into each subtable in sequence and repeating this process until all subtable data has been inserted. The default value is 0, meaning data is inserted into one subtable completely before moving to the next.
  This parameter can also be configured in `super_tables`; if configured, the settings in `super_tables` take higher priority and override the global settings.

- **insert_interval** :
  Specifies the insertion interval for interleaved insertion mode in milliseconds, default is 0. This only takes effect when `-B/--interlace-rows` is greater than 0. It means that the data insertion thread will wait for the specified interval after inserting interleaved records for each subtable before proceeding to the next round of writing.
  This parameter can also be configured in `super_tables`; if configured, the settings in `super_tables` take higher priority and override the global settings.

- **num_of_records_per_req** :
  The number of data rows requested per write to TDengine, default is 30000. If set too high, the TDengine client driver will return corresponding error messages, and this parameter needs to be reduced to meet the writing requirements.

- **prepare_rand** : The number of unique values in the generated random data. If it is 1, it means all data are the same. The default value is 10000.

- **pre_load_tb_meta** : Whether to pre-load the meta data of subtables, values are “yes” or "no". When there are a large number of subtables, turning on this option can improve the writing speed.

### Configuration Parameters for Query Scenarios

In query scenarios, `filetype` must be set to `query`.
`query_times` specifies the number of times to run the query, numeric type.

Query scenarios can control the execution of slow query statements by setting `kill_slow_query_threshold` and `kill_slow_query_interval` parameters, where threshold controls that queries exceeding the specified exec_usec time will be killed by taosBenchmark, in seconds; interval controls the sleep time to avoid continuous slow query CPU consumption, in seconds.

For other common parameters, see Common Configuration Parameters.

#### Configuration Parameters for Executing Specified Query Statements

Configuration parameters for querying specified tables (can specify supertables, subtables, or regular tables) are set in `specified_table_query`.

- **mixed_query**  "yes": `Mixed Query`  "no": `Normal Query`,  default is "no"  
`Mixed Query`: All SQL statements in `sqls` are grouped by the number of threads, with each thread executing one group. Each SQL statement in a thread needs to perform `query_times` queries.  
`Normal Query `: Each SQL in `sqls` starts `threads` and exits after executing `query_times` times. The next SQL can only be executed after all previous SQL threads have finished executing and exited.  
Regardless of whether it is a `Normal Query` or `Mixed Query`, the total number of query executions is the same. The total number of queries = `sqls` * `threads` * `query_times`. The difference is that `Normal Query` starts  `threads` for each SQL query, while ` Mixed Query` only starts  `threads` once to complete all SQL queries. The number of thread startups for the two is different.  

- **query_interval** : Query interval, in seconds, default is 0.

- **threads** : Number of threads executing the SQL query, default is 1.

- **sqls**:
  - **sql**: The SQL command to execute, required.
  - **result**: File to save the query results, if not specified, results are not saved.

#### Configuration Parameters for Querying Supertables

Configuration parameters for querying supertables are set in `super_table_query`.  
The thread mode of the super table query is the same as the `Normal Query` mode of the specified query statement described above, except that `sqls` is filled all sub tables.

- **stblname** : The name of the supertable to query, required.

- **query_interval** : Query interval, in seconds, default is 0.

- **threads** : Number of threads executing the SQL query, default is 1.

- **sqls** :
  - **sql** : The SQL command to execute, required; for supertable queries, keep "xxxx" in the SQL command, the program will automatically replace it with all subtable names of the supertable.
  - **result** : File to save the query results, if not specified, results are not saved.

### Configuration Parameters for Subscription Scenarios

In subscription scenarios, `filetype` must be set to `subscribe`, this parameter and other common parameters see Common Configuration Parameters.

#### Configuration Parameters for Executing Specified Subscription Statements

Configuration parameters for subscribing to specified tables (can specify supertables, subtables, or regular tables) are set in `specified_table_query`.

- **threads/concurrent** : Number of threads executing the SQL, default is 1.

- **sqls** :
  - **sql** : The SQL command to execute, required.

#### Data Type Writing Comparison Table in Configuration File

| #   |     **Engine**      | **taosBenchmark**
| --- | :----------------: | :---------------:
| 1   |  TIMESTAMP         |    timestamp
| 2   |  INT               |    int
| 3   |  INT UNSIGNED      |    uint
| 4   |  BIGINT            |    bigint
| 5   |  BIGINT UNSIGNED   |    ubigint
| 6   |  FLOAT             |    float
| 7   |  DOUBLE            |    double
| 8   |  BINARY            |    binary
| 9   |  SMALLINT          |    smallint
| 10  |  SMALLINT UNSIGNED |    usmallint
| 11  |  TINYINT           |    tinyint
| 12  |  TINYINT UNSIGNED  |    utinyint
| 13  |  BOOL              |    bool
| 14  |  NCHAR             |    nchar
| 15  |  VARCHAR           |    varchar
| 16  |  VARBINARY         |    varbinary
| 17  |  GEOMETRY          |    geometry
| 18  |  JSON              |    json

Note: Data types in the taosBenchmark configuration file must be in lowercase to be recognized.
