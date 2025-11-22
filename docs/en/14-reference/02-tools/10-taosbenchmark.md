---
title: taosBenchmark Reference
sidebar_label: taosBenchmark
slug: /tdengine-reference/tools/taosbenchmark
toc_max_heading_level: 4
---

TaosBenchmark is a performance benchmarking tool for TDengine products, providing insertion, query, and subscription performance testing for TDengine products, and outputting performance indicators.

## Get

taosBenchmark is the default installation component in the TDengine server and client installation package. It can be used after installation, refer to [TDengine Installation](../../../get-started/)

## Startup

taosbenchmark supports three operating modes:

- No Parameter Mode
- Command Line Mode
- JSON Configuration File Mode

The `Command Line Mode` is a subset of the `JSON Configuration File Mode` function. When both are used at the same time, the `Command Line Mode` takes precedence.

:::tip
Ensure that the TDengine cluster is running correctly before running taosBenchmark.
:::

### No Parameter Mode

Execute the following command to quickly experience taosBenchmark performing a write performance test on TDengine based on the default configuration.

```shell
taosBenchmark
```

When running without parameters, taosBenchmark defaults to connecting to the TDengine cluster specified in `/etc/taos/taos.cfg`.
After successful connection, a smart meter example database test, super meters, and 10000 sub meters will be created, with 10000 records per sub meter. If the test database already exists, it will be deleted before creating a new one.

### Command Line Mode

The parameters supported by the command line are those frequently used in the write function. The query and subscription function does not support the command line mode.
For example:

```bash
taosBenchmark -d db -t 100 -n 1000 -T 4 -I stmt -y
```

This command means that using `taosBenchmark` will create a database named `db`, create the default super table `meters`, sub table 100, and use parameter binding (stmt) to write 1000 records for each sub table.

### JSON Configuration File Mode

Running in configuration file mode provides all functions, so parameters can be configured to run in the configuration file.  

```shell
taosBenchmark -f <json file>
```

## Command Line Parameters

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

- **-j/--output-json-file \<file>** :
  Path of the output JSON file for results.

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

- **-s/--start-timestamp \<NUMBER>**:
  Specify start timestamp to insert data for each child table

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

- **-Z/--connect-mode \<NUMBER>**: The connection method, with 0 indicating the use of native connection method, 1 indicating the use of WebSocket connection method, and default to native connection method.
- **-v/--vgroups \<NUMBER>**:
  Specifies the number of vgroups when creating the database, only valid for TDengine v3.0+.

- **-V/--version**:
  Displays version information and exits. Cannot be used with other parameters.

- **-?/--help**:
  Displays help information and exits. Cannot be used with other parameters.

## Configuration File Parameters

### General Configuration Parameters

The parameters listed in this section apply to all functional modes.

- **filetype**: The function to test, possible values are `insert`, `query`, `subscribe` and `csvfile`. Corresponding to insert, query, subscribe and generate csv file functions. Only one can be specified in each configuration file.

- **cfgdir**: Directory where the TDengine client configuration file is located, default path is /etc/taos.

- **output_dir**: The directory specified for output files. When the feature category is csvfile, it refers to the directory where the generated csv files will be saved. The default value is ./output/.

- **host**: Specifies the FQDN of the TDengine server to connect to, default value is localhost.

- **port**: The port number of the TDengine server to connect to, default value is 6030.

- **user**: Username for connecting to the TDengine server, default is root.

- **password** : Password for connecting to the TDengine server, default value is taosdata.

- **result_json_file**：The path to the result output JSON file. If not configured, the file will not be output.

### Insertion Configuration Parameters

In insertion scenarios, `filetype` must be set to `insert`. For this parameter and other common parameters, see Common Configuration Parameters.

- **keep_trying**: Number of retries after failure, default is no retry. Requires version v3.0.9 or above.

- **trying_interval**: Interval between retries in milliseconds, effective only when retries are specified in keep_trying. Requires version v3.0.9 or above.

- **childtable_from and childtable_to**: Specifies the range of child tables to write to, the interval is [childtable_from, childtable_to].

- **escape_character**: Whether the supertable and child table names contain escape characters, default is "no", options are "yes" or "no".

- **continue_if_fail**: Allows users to define behavior after failure

  "continue_if_fail":  "no", taosBenchmark exits automatically upon failure, default behavior
  "continue_if_fail": "yes", taosBenchmark warns the user and continues writing
  "continue_if_fail": "smart", if the child table does not exist upon failure, taosBenchmark will create the child table and continue writing

#### Database Parameters

Parameters related to database creation are configured in the `dbinfo` section of the json configuration file, specific parameters are as follows. Other parameters correspond to those specified in TDengine's `create database`, see [../../taos-sql/database]

- **name**: Database name.

- **drop**: Whether to delete the database before insertion, options are "yes" or "no", "no" means do not create. Default is to delete.

#### Supertable Parameters

Parameters related to supertable creation are configured in the `super_tables` section of the json configuration file, specific parameters are as follows.

- **name**: Supertable name, must be configured, no default value.

- **child_table_exists**: Whether the child table already exists, default is "no", options are "yes" or "no".

- **childtable_count**: Number of child tables, default is 10.

- **childtable_prefix**: Prefix for child table names, mandatory, no default value.

- **auto_create_table**: Effective only when insert_mode is taosc, rest, stmt and child_table_exists is "no", "yes" means taosBenchmark will automatically create non-existent tables during data insertion; "no" means all tables are created in advance before insertion.

- **batch_create_tbl_num**: Number of tables created per batch during child table creation, default is 10. Note: The actual number of batches may not match this value, if the executed SQL statement exceeds the maximum supported length, it will be automatically truncated and executed, continuing the creation.

- **data_source**: Source of the data, default is randomly generated by taosBenchmark, can be configured as "rand" and "sample". For "sample", use the file specified by the sample_file parameter.

- **insert_mode**: Insertion mode, options include taosc, rest, stmt, stmt2, sml, sml-rest, corresponding to normal writing, restful interface writing, parameter binding interface writing, schemaless interface writing, restful schemaless interface writing (provided by taosAdapter). Default is taosc.

- **non_stop_mode**: Specifies whether to continue writing (this parameter only supports `interlace_rows > 0`), if "yes" then insert_rows is ineffective, writing stops only when Ctrl + C stops the program. Default is "no", i.e., stop after writing a specified number of records. Note: Even in continuous writing mode, insert_rows must still be configured as a non-zero positive integer.

- **line_protocol** : Use line protocol to insert data, effective only when insert_mode is sml or sml-rest, options include line, telnet, json.

- **tcp_transfer** : Communication protocol in telnet mode, effective only when insert_mode is sml-rest and line_protocol is telnet. If not configured, the default is the http protocol.

- **insert_rows** : The number of records inserted per subtable, default is 0.

- **childtable_offset** : Effective only when child_table_exists is yes, specifies the offset when getting the subtable list from the supertable, i.e., starting from which subtable.

- **childtable_limit** : Effective only when child_table_exists is yes, specifies the limit when getting the subtable list from the supertable.

- **interlace_rows** : Enables interlaced insertion mode and specifies the number of rows to insert into each subtable at a time. Interlaced insertion mode means inserting the number of rows specified by this parameter into each subtable in turn and repeating this process until all subtable data is inserted. The default is 0, i.e., data is inserted into one subtable before moving to the next subtable.

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

- **use_tag_table_name**：When set to yes, the first column in the tag data within the CSV file will be the name of the subtable to be created; otherwise, the system will automatically generate subtable names for creation.

- **primary_key_name**：Specify the name of the primary key for the supertable. If not specified, the default is ts.

- **primary_key** : Specifies whether the supertable has a composite primary key, values are 1 and 0, composite primary key columns can only be the second column of the supertable, after specifying the generation of composite primary keys, ensure that the second column meets the data type of composite primary keys, otherwise an error will occur
- **repeat_ts_min** : Numeric type, when composite primary key is enabled, specifies the minimum number of records with the same timestamp to be generated, the number of records with the same timestamp is a random value within the range [repeat_ts_min, repeat_ts_max], when the minimum value equals the maximum value, it is a fixed number
- **repeat_ts_max** : Numeric type, when composite primary key is enabled, specifies the maximum number of records with the same timestamp to be generated
- **sqls** : Array of strings type, specifies the array of sql to be executed after the supertable is successfully created, the table name specified in sql must be prefixed with the database name, otherwise an unspecified database error will occur

- **csv_file_prefix**: String type, sets the prefix for the names of the generated csv files. Default value is "data".

- **csv_ts_format**: String type, sets the format of the time string in the names of the generated csv files, following the `strftime` format standard. If not set, files will not be split by time intervals. Supported patterns include:
  - %Y: Year as a four-digit number (e.g., 2025)
  - %m: Month as a two-digit number (01 to 12)
  - %d: Day of the month as a two-digit number (01 to 31)
  - %H: Hour in 24-hour format as a two-digit number (00 to 23)
  - %M: Minute as a two-digit number (00 to 59)
  - %S: Second as a two-digit number (00 to 59)

- **csv_ts_interval**: String type, sets the time interval for splitting generated csv file names. Supports daily, hourly, minute, and second intervals such as 1d/2h/30m/40s. The default value is "1d".

- **csv_output_header**: String type, sets whether the generated csv files should contain column header descriptions. The default value is "yes".

- **csv_tbname_alias**: String type, sets the alias for the tbname field in the column header descriptions of csv files. The default value is "device_id".

- **csv_compress_level**: String type, sets the compression level for generating csv-encoded data and automatically compressing it into gzip file. This process directly encodes and compresses the data, rather than first generating a csv file and then compressing it. Possible values are:
  - none: No compression
  - fast: gzip level 1 compression
  - balance: gzip level 6 compression
  - best: gzip level 9 compression

#### Tag and Data Columns

Specify the configuration parameters for tag and data columns in `super_tables` under `columns` and `tag`.

- **type**: Specifies the column type, refer to the data types supported by TDengine.
  Note: The JSON data type is special, it can only be used for tags, and when using JSON type as a tag, there must be only this one tag. In this case, count and len represent the number of key-value pairs in the JSON tag and the length of each KV pair's value respectively, with value defaulting to string.

- **len**: Specifies the length of the data type, effective for NCHAR, BINARY, and JSON data types. If configured for other data types, if it is 0, it means the column is always written with a null value; if not 0, it is ignored.

- **count**: Specifies the number of times this type of column appears consecutively, for example, "count": 4096 can generate 4096 columns of the specified type.

- **name**: The name of the column, if used with count, for example "name": "current", "count":3, then the names of the 3 columns are current, current_2, current_3 respectively.

- **min**: Float type, the minimum value for the data type of the column/tag. Generated values will be greater than or equal to the minimum value.

- **max**: Float type, the maximum value for the data type of the column/tag. Generated values will be less than the maximum value.

- **dec_min**: String type, specifies the minimum value for a column of the DECIMAL data type. This field is used when min cannot express sufficient precision. The generated values will be greater than or equal to the minimum value.

- **dec_max**: String type, specifies the maximum value for a column of the DECIMAL data type. This field is used when max cannot express sufficient precision. The generated values will be less than the maximum value.

- **precision**: The total number of digits (including digits before and after the DECIMAL point), applicable only to the DECIMAL type, with a valid range of 0 to 38.

- **scale**: The number of digits to the right of the decimal point. For the FLOAT type, the scale's valid range is 0 to 6; for the DOUBLE type, the range is 0 to 15; and for the DECIMAL type, the scale's valid range is 0 to its precision value.

- **fun**: This column data is filled with functions, currently only supports sin and cos functions, input parameters are converted from timestamps to angle values, conversion formula: angle x = input time column ts value % 360. Also supports coefficient adjustment, random fluctuation factor adjustment, displayed in a fixed format expression, such as fun="10*sin(x)+100*random(5)", x represents the angle, ranging from 0 ~ 360 degrees, the increment step is consistent with the time column step. 10 represents the multiplication coefficient, 100 represents the addition or subtraction coefficient, 5 represents the fluctuation amplitude within a 5% random range. Currently supports int, bigint, float, double four data types. Note: The expression is in a fixed pattern and cannot be reversed.

- **values**: The value domain for nchar/binary column/tag, will randomly select from the values.

- **sma**: Adds this column to SMA, value is "yes" or "no", default is "no".

- **encode**: String type, specifies the first level encoding algorithm for this column in two-level compression, see creating supertables for details.

- **compress**: String type, specifies the second level encryption algorithm for this column in two-level compression, see creating supertables for details.

- **level**: String type, specifies the compression rate of the second level encryption algorithm for this column in two-level compression, see creating supertables for details.

- **gen**: String type, specifies the method of generating data for this column, if not specified it is random, if specified as "order", it will increase sequentially by natural numbers.

- **fillNull**: String type, specifies whether this column randomly inserts NULL values, can be specified as "true" or "false", only effective when generate_row_rule is 2.

#### Insertion Behavior Parameters

- **thread_count**: The number of threads for inserting data, default is 8.

- **thread_bind_vgroup**: Whether the vgroup is bound with the writing thread during writing, binding can improve writing speed, values are "yes" or "no", default is "no". Set to "no" to maintain the same behavior as before. When set to "yes", if the thread_count is equal to the number of vgroups in the database, thread_count is automatically adjusted to the number of vgroups; if thread_count is less than the number of vgroups, the number of writing threads is not adjusted, one thread writes one vgroup data after another, while maintaining the rule that only one thread can write into a vgroup at a time.

- **create_table_thread_count** : The number of threads for creating tables, default is 8.

- **result_file** : The path to the result output file, default is ./output.txt.

- **confirm_parameter_prompt** : A toggle parameter that requires user confirmation after a prompt to continue. The value can be "yes" or "no", by default "no".

- **interlace_rows** : Enables interleaved insertion mode and specifies the number of rows to insert into each subtable at a time. Interleaved insertion mode refers to inserting the specified number of rows into each subtable in sequence and repeating this process until all subtable data has been inserted. The default is 0, meaning data is inserted into one subtable completely before moving to the next.
  This parameter can also be configured in `super_tables`; if configured, the settings in `super_tables` take higher priority and override the global settings.

- **insert_interval** :
  Specifies the insertion interval for interleaved insertion mode in milliseconds, default is 0. This only takes effect when `-B/--interlace-rows` is greater than 0. It means that the data insertion thread will wait for the specified interval after inserting interleaved records for each subtable before proceeding to the next round of writing.
  This parameter can also be configured in `super_tables`; if configured, the settings in `super_tables` take higher priority and override the global settings.

- **num_of_records_per_req** :
  The number of data rows requested per write to TDengine, default is 30000. If set too high, the TDengine client driver will return corresponding error messages, and this parameter needs to be reduced to meet the writing requirements.

- **prepare_rand** : The number of unique values in the generated random data. If it is 1, it means all data are the same. The default is 10000.

- **pre_load_tb_meta** : Whether to pre-load the meta data of subtables, values are "yes" or "no". When there are a large number of subtables, turning on this option can improve the writing speed.

### Query Parameters

`filetype` must be set to `query`.

`query_mode` connect method:

- "taosc": Native.
- "rest" : RESTful.

`query_times` specifies the number of times to run the query, numeric type.

**Note: from version 3.3.5.6 and beyond, simultaneous configuration for `specified_table_query` and `super_table_query` in a JSON file is no longer supported**

For other common parameters, see [General Configuration Parameters](#general-configuration-parameters)

#### Specified Query

Configuration parameters for querying specified tables (can specify supertables, subtables, or regular tables) are set in `specified_table_query`.  

- **mixed_query** : Query Mode . "yes" is `Mixed Query`, "no" is `General Query`, default is "no".
  `General Query`:
  Each SQL in `sqls` starts `threads` threads to query this SQL, Each thread exits after executing the `query_times` queries, and only after all threads executing this SQL have completed can the next SQL be executed.
  The total number of queries(`General Query`) = the number of `sqls` *`query_times`* `threads`  
  `Mixed Query`:
  All SQL statements in `sqls` are divided into `threads` groups, with each thread executing one group. Each SQL statement needs to execute `query_times` queries.
  The total number of queries(`Mixed Query`) = the number of `sqls` * `query_times`.

- **batch_query** : Batch query power switch.
"yes": indicates that it is enabled.
"no":  indicates that it is not enabled, and other values report errors.
Batch query refers to dividing all SQL statements in SQL into `threads` groups, with each thread executing one group.
Each SQL statement is queried only once before exiting, and the main thread waits for all threads to complete before determining if the `query_interval` parameter is set. If sleep is required for a specified time, each thread group is restarted and the previous process is repeated until the number of queries is exhausted.
Functional limitations:  
- Only supports scenarios where `mixed_query` is set to 'yes'.
- Restful queries are not supported, meaning `query_made` cannot be 'rest'.

- **query_interval** : Query interval, in millisecond, default is 0.
When the 'batch_query' switch is turned on, it indicates the interval time after each batch query is completed, When closed, it indicates the interval time between each SQL query completion.
If the execution time of the query exceeds the interval time, it will no longer wait. If the execution time of the query is less than the interval time, it is necessary to wait to make up for the interval time.

- **threads** : Number of threads executing the SQL query, default is 1.
- **sqls**:
  - **sql**: The SQL command to execute, required.
  - **result**: File to save the query results, if not specified, results are not saved.

#### Supertables

Configuration parameters for querying supertables are set in `super_table_query`.  
The thread mode of the super table query is the same as the `General Query` mode of the specified query statement described above, except that `sqls` is filled all sub tables.

- **stblname** : The name of the supertable to query, required.
- **query_interval** : Query interval, in seconds, default is 0.
- **threads** : Number of threads executing the SQL query, default is 1.
- **sqls** :
  - **sql** : The SQL command to execute, required; for supertable queries, keep "xxxx" in the SQL command, the program will automatically replace it with all subtable names of the supertable.
  - **result** : File to save the query results, if not specified, results are not saved.
  - **Note**: The maximum number of SQL arrays configured under SQL is 100.

### Subscription Parameters

In the subscription scenario, `filetype` must be set to `subscribe`. For details of this parameter and other general parameters, see [General Configuration Parameters](#general-configuration-parameters)
The subscription configuration parameters are set under `tmq_info`. The parameters are as follows:

- **concurrent**: the number of consumers who consume subscriptions, or the number of concurrent consumers. The default value is 1.
- **create_mode**: create a consumer mode.
  Which can be sequential: create in sequence. parallel: It is created at the same time. It is required and has no default value.
- **group_mode**: generate the consumer groupId mode.
  Which can take the value share: all consumers can only generate one groupId independent: Each consumer generates an independent groupId. If `group.id` is not set, this item is mandatory and has no default value.
  -**poll_delay**: The polling timeout time passed in by calling tmq_consumer_poll.
  The unit is milliseconds. A negative number means the default timeout is 1 second.
  -**enable.manual.commit**: whether manual submission is allowed.
  The value can be true: manual submission is allowed, after consuming messages, manually call tmq_commit_sync to complete the submission. false: Do not submit, default value: false.
  -**rows_file**: a file that stores consumption data.
  It can be a full path or a relative path with a file name.The actual saved file will be followed by the consumer serial number. For example, rows_file is result, and the actual file name is result_1 (consumer 1) result_2 (consumer 2).
  -**expect_rows**: the number of rows and data types expected to be consumed by each consumer.
  When the consumption reaches this number, the consumption will exit, and the consumption will continue without setting.
  -**topic_list**: specifies the topic list and array type of consumption.
   Example of topic list format: `{"name": "topic1", "sql": "select * from test.meters;"}`.
   name: Specify the topic name.
   sql:  Specify the sql statement for creating topic, Ensure that the sql is correct, and the framework will automatically create topic.

For the following parameters, see the description of [Subscription](../../../advanced/subscription/):

- **client.id**
- **auto.offset.reset**
- **enable.auto.commit**
- **enable.auto.commit**
- **msg.with.table.name**
- **auto.commit.interval.ms**
- **group.id**: If this value is not specified, the groupId will be generated by the rule specified by `group_mode`. If this value is specified, the `group_mode` parameter is ignore.

### Data Type Comparison Table

| #   |     **TDengine**      | **taosBenchmark**
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
| 19  |  DECIMAL           |    decimal
| 20  |  BLOB              |    blob 

Note: Data types in the taosBenchmark configuration file must be in lowercase to be recognized.

## Example Of Configuration Files

Below are a few examples of configuration files:

### Insertion Example

<details>
<summary>insert.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/insert.json}}
```

</details>

### Query Example

<details>
<summary>query.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/query.json}}
```

</details>

<details>
<summary>queryStb.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/queryStb.json}}
```

</details>

### Subscription Example

<details>
<summary>tmq.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/tmq.json}}
```

</details>

### Export CSV File Example

<details>
<summary>csv-export.json</summary>

```json
{{#include /TDengine/tools/taos-tools/example/csv-export.json}}
```

</details>

[Other json examples](https://github.com/taosdata/TDengine/tree/main/tools/taos-tools/example)

## Output Performance Indicators

### Write Indicators

After writing is completed, a summary performance metric will be output in the last two lines in the following format:

``` bash
SUCC: Spent 8.527298 (real 8.117379) seconds to insert rows: 10000000 with 8 thread(s) into test 1172704.41 (real 1231924.74) records/second
SUCC: insert delay, min: 19.6780ms, avg: 64.9390ms, p90: 94.6900ms, p95: 105.1870ms, p99: 130.6660ms, max: 157.0830ms
```

First line write speed statistics:

- Spent: Total write time, in seconds, counting from the start of writing the first data to the end of the last data. This indicates that a total of 8.527298 seconds were spent.
- Real: Total write time (calling the engine), excluding the time spent preparing data for the testing framework. Purely counting the time spent on engine calls, The time spent is 8.117379 seconds. If 8.527298-8.117379=0.409919 seconds, it is the time spent preparing data for the testing framework.
- Rows: Write the total number of rows, which is 10 million pieces of data.
- Threads: The number of threads being written, which is 8 threads writing simultaneously.
- Records/second write speed = `total write time` / `total number of rows written`, real in parentheses is the same as before, indicating pure engine write speed.

Second line single write delay statistics:  

- min: Write minimum delay.
- avg: Write normal delay.
- p90: Write delay p90 percentile delay number.
- p95: Write delay p95 percentile delay number.
- p99: Write delay p99 percentile delay number.
- max: maximum write delay.
Through this series of indicators, the distribution of write request latency can be observed.

### Query indicators

The query performance test mainly outputs the QPS indicator of query request speed, and the output format is as follows:

``` bash
complete query with 3 threads and 10000 query delay avg:  0.002686s min:  0.001182s max:  0.012189s p90:  0.002977s p95:  0.003493s p99:  0.004645s SQL command: select ...
INFO: Spend 26.9530 second completed total queries: 30000, the QPS of all threads: 1113.049
```

- The first line represents the percentile distribution of query execution and query request delay for each of the three threads executing 10000 queries. The SQL command is the test query statement.
- The second line indicates that the total query time is 26.9653 seconds, and the query rate per second (QPS) is 1113.049 times/second.
- If the `continue_if_fail` option is set to `yes` in the query, the last line will output the number of failed requests and error rate, the format like "error + number of failed requests (error rate)".
- QPS        = number of successful requests / time spent (in seconds)
- Error rate = number of failed requests / (number of successful requests + number of failed requests)

### Subscription indicators

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

- Lines 1 to 3 real-time output of the current consumption speed of each consumer, msgs/s represents the number of consumption messages, each message contains multiple rows of data, and rows/s represents the consumption speed calculated by rows.
- Lines 4 to 6 show the overall statistics of each consumer after the test is completed, including the total number of messages consumed and the total number of lines.
- The overall statistics of all consumers in line 7, `msgs` represents how many messages were consumed in total, `rows` represents how many rows of data were consumed in total.
