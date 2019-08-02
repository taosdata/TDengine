# TAOS SQL

TDengine provides a SQL like query language to insert or query data. You can execute the SQL statements through the TDengine Shell, or through C/C++, Java(JDBC), Python, Restful, Go, and Node.js APIs to interact with the `taosd` service.

Before reading through, please have a look at the conventions used for syntax descriptions here in this documentation.

* Squared brackets ("[]") indicate optional arguments or clauses
* Curly braces ("{}") indicate that one member from a set of choices in the braces must be chosen
* A single verticle line ("|") works a separator for multiple optional args or clauses
* Dots ("…") means repeating for as many times

## Data Types

### Timestamp

The timestamp is the most important data type in TDengine. The first column of each table must be  **`TIMESTAMP`** type, but other columns can also be  **`TIMESTAMP`** type. The following rules for timestamp: 

* String Format: `'YYYY-MM-DD HH:mm:ss.MS'`, which represents the year, month, day, hour, minute and second and milliseconds. For example,`'2017-08-12 18:52:58.128'` is a valid timestamp string. Note: timestamp string must be quoted by either single quote or double quote. 

* Epoch Time:  a timestamp value can also be a long integer representing milliseconds since the epoch. For example, the values in the above example can be represented as an epoch `1502535178128` in milliseconds. Please note the epoch time doesn't need any quotes.

* Internal Function **`NOW`** : this is the current time of the server
* If timestamp is 0 when inserting a record, timestamp will be set to the current time of the server
* Arithmetic operations can be applied to timestamp. For example: `now-2h` represents a timestamp which is 2 hours ago from the current server time. Units include `a` (milliseconds),  `s` (seconds),  `m` (minutes),  `h` (hours),  `d` (days),  `w` (weeks), `n` (months), `y` (years). **`NOW`** can be used in either insertions or queries. 

Default time precision is millisecond, you can change it to microseocnd by setting parameter enableMicrosecond in [system configuration](../administrator/#Configuration-on-Server). For epoch time, the long integer shall be microseconds since the epoch. For the above string format, MS shall be six digits. 

### Data Types

The full list of data types is listed below.  For string types of data, we will use ***M*** to indicate the maximum length of that type.

|      |  Data Type  |  Bytes  | Note                                                         |
| ---- | :---------: | :-----: | ------------------------------------------------------------ |
| 1    |   TINYINT   |    1    | A nullable integer type with a range of [-127, 127]​          |
| 2    |  SMALLINT   |    2    | A nullable integer type with a range of [-32767, 32767]​      |
| 3    |     INT     |    4    | A nullable integer type with a range of [-2^31+1, 2^31-1 ]   |
| 4    |   BIGINT    |    8    | A nullable integer type with a range of [-2^59, 2^59 ]​       |
| 5    |    FLOAT    |    4    | A standard nullable float type with 6 -7 significant digits and a range of [-3.4E38, 3.4E38] |
| 6    |   DOUBLE    |    8    | A standard nullable double float type with 15-16 significant digits and a range of [-1.7E308, 1.7E308]​ |
| 7    |    BOOL     |    1    | A nullable boolean type, [**`true`**, **`false`**]           |
| 8    |  TIMESTAMP  |    8    | A nullable timestamp type with the same usage as the primary column timestamp |
| 9    | BINARY(*M*) |   *M*   | A nullable string type whose length is *M*, any exceeded chars will be automatically truncated. This type of string only supports ASCii encoded chars. |
| 10   | NCHAR(*M*)  | 4 * *M* | A nullable string type whose length is *M*, any exceeded chars will be truncated. The **`NCHAR`** type supports Unicode encoded chars. |

All the keywords in a SQL statement are case-insensitive, but strings values are case-sensitive and must be quoted by a pair of `'` or `"`. To quote a `'` or a `"` , you can use the escape character `\`.

## Database Management

- **Create a Database**
    ```mysql
    CREATE DATABASE [IF NOT EXISTS] db_name [KEEP keep]
    ```
    Option: `KEEP` is used for data retention policy. The data records will be removed once keep-days are passed. There are more parameters related to DB storage, please check [system configuration](../administrator/#Configuration-on-Server).


- **Use a Database**
  
    ```mysql
    USE db_name
    ```
    Use or switch the current database.


- **Drop a Database**
    ```mysql
    DROP DATABASE [IF EXISTS] db_name
    ```
    Remove a database, all the tables inside the DB will be removed too, be careful.


- **List all Databases**
  
    ```mysql
    SHOW DATABASES
    ```


## Table Management

- **Create a Table**
  
    ```mysql
    CREATE TABLE [IF NOT EXISTS] tb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...])
    ```
    Note: 
    
    1) The first column must be a `timestamp`, and the system will set it as the primary key.
    
    2) The record size is limited to 4096 bytes
    
    3) For `binary` or `nchar` data types, the length must be specified. For example, binary(20) means a binary data type with 20 bytes.


- **Drop a Table**

    ```mysql
    DROP TABLE [IF EXISTS] tb_name
    ```


- **List all Tables **
    ```mysql
    SHOW TABLES [LIKE tb_name_wildcar]
    ```
    It shows all tables in the current DB. 
    
    Note: Wildcard characters can be used in the table name to filter tables. 
    Wildcard characters: 
    1) ’%’ means 0 to any number of characters. 
    2）’_’ underscore means exactly one character.


- **Print Table Schema**

    ```mysql
    DESCRIBE tb_name
    ```


- **Add a Column**

    ```mysql
    ALTER TABLE tb_name ADD COLUMN field_name data_type
    ```


- **Drop a Column**

    ```mysql
    ALTER TABLE tb_name DROP COLUMN field_name 
    ```
    If the table is created via [Super Table](), the schema can only be changed via STable. But for tables not created from STable, you can change their schema directly.

**Tips**: You can apply an operation on a table not in the current DB by concatenating DB name with the character '.', then with the table name. For example, 'demo.tb1' means the operation is applied to table `tb1` in DB `demo` even though `demo` is not the currently selected DB.   

## Inserting Records

- **Insert a Record**
    ```mysql
    INSERT INTO tb_name VALUES (field_value, ...);
    ```
    Insert a data record into table tb_name


- **Insert a Record with Selected Columns**
  
    ```mysql
    INSERT INTO tb_name (field1_name, ...) VALUES(field1_value, ...)
    ```
    Insert a data record into table tb_name, with data in selected columns. If a column is not selected, the system will put NULL there. First column (time stamp ) cant not be null, it must be inserted.


- **Insert a Batch of Records**
  
    ```mysql
    INSERT INTO tb_name VALUES (field1_value1, ...) (field1_value2, ...)...;
    ```
    Insert multiple data records into the table


- **Insert a Batch of Records with Selected Columns**
  
    ```mysql
    INSERT INTO tb_name (field1_name, ...) VALUES(field1_value1, ...) (field1_value2, ...)
    ```


- **Insert Records into Multiple Tables**
  
    ```mysql
    INSERT INTO tb1_name VALUES (field1_value1, ...)(field1_value2, ...)... 
                tb2_name VALUES (field1_value1, ...)(field1_value2, ...)...;
    ```
    Insert data records into table tb1_name and tb2_name


- **Insert Records into Multiple Tables with Selected Columns**
  
    ```mysql
    INSERT INTO tb1_name (tb1_field1_name, ...) VALUES (field1_value1, ...) (field1_value1, ...)
                tb2_name (tb2_field1_name, ...) VALUES(field1_value1, ...) (field1_value2, ...)
    ```

Note: For a table, the new record must have a timestamp bigger than the last data record, otherwise, it will be discarded and not inserted. If the timestamp is 0, the time stamp will be set to the system time on the server.

**IMPORT**: If you do want to insert a historical data record into a table, use IMPORT command instead of INSERT. IMPORT has the same syntax as INSERT. If you want to import a batch of historical records, the records must be ordered by the timestamp, otherwise, TDengine won't handle it in the right way.

## Data Query

###Query Syntax:

```mysql
SELECT {* | expr_list} FROM tb_name
    [WHERE where_condition]
    [ORDER BY _c0 { DESC | ASC }]
    [LIMIT limit [, OFFSET offset]]
    [>> export_file]
    
SELECT function_list FROM tb_name
    [WHERE where_condition]
    [LIMIT limit [, OFFSET offset]]
    [>> export_file]
```

- To query a table, use `*` to select all data from a table; or a specified list of expressions `expr_list` of columns. The SQL expression can contain alias and arithmetic operations between numeric typed columns.
- For the `WHERE` conditions, use logical operations to filter the timestamp column and all numeric columns, and wild cards to filter the two string typed columns. 
- Sort the result set by the first column timestamp `_c0` (or directly use the timestamp column name) in either descending or ascending order (by default). "Order by" could not be applied to other columns.
- Use `LIMIT` and `OFFSET` to control the number of rows returned and the starting position of the retrieved rows. LIMIT/OFFSET is applied after "order by" operations.
- Export the retrieved result set into a CSV file using `>>`. The target file's full path should be explicitly specified in the statement.

###Supported Operations of Data Filtering:

| Operation | Note                          | Applicable Data Types                 |
| --------- | ----------------------------- | ------------------------------------- |
| >         | larger than                   | **`timestamp`** and all numeric types |
| <         | smaller than                  | **`timestamp`** and all numeric types |
| >=        | larger than or equal to       | **`timestamp`** and all numeric types |
| <=        | smaller than or equal to      | **`timestamp`** and all numeric types |
| =         | equal to                      | all types                             |
| <>        | not equal to                  | all types                             |
| %         | match with any char sequences | **`binary`** **`nchar`**              |
| _         | match with a single char      | **`binary`** **`nchar`**              |

1. For two or more conditions, only AND is supported, OR is not supported yet.
2. For filtering, only a single range is supported. For example, `value>20 and value<30` is a valid condition, but `value<20 AND value<>5` is an invalid condition

### Some Examples 

- For the examples below, table tb1 is created via the following statements

    ```mysql
    CREATE TABLE tb1 (ts timestamp, col1 int, col2 float, col3 binary(50))
    ```

- Query all the records in tb1 in the last hour:
    ```mysql
    SELECT * FROM tb1 WHERE ts >= NOW - 1h
    ```


- Query all the records in tb1 between 2018-06-01 08:00:00.000 and 2018-06-02 08:00:00.000, and filter out only the records whose col3 value ends with 'nny', and sort the records by their timestamp in a descending order:
    ```mysql
    SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC
    ```


- Query the sum of col1 and col2 as alias 'complex_metric', and filter on the timestamp and col2 values. Limit the number of returned rows to 10, and offset the result by 5.
    ```mysql
    SELECT (col1 + col2) AS 'complex_metric' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' and col2 > 1.2 LIMIT 10 OFFSET 5
    ```


- Query the number of records in tb1 in the last 10 minutes, whose col2 value is larger than 3.14, and export the result to file `/home/testoutpu.csv`.
    ```mysql
    SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutpu.csv
    ```

## SQL Functions

### Aggregation Functions

TDengine supports aggregations over numerical values, they are listed below:

- **COUNT**

    ```mysql
    SELECT COUNT([*|field_name]) FROM tb_name [WHERE clause]
    ```
    Function: return the number of rows.  
    Return Data Type: `integer`.  
    Applicable Data Types: all.  
    Applied to: table/STable.  
    Note: 
    
    1) `*` can be used for all columns, as long as a column has non-NULL values, it will be counted.
    
    2) If it is on a specific column, only rows with non-NULL values will be counted 


- **AVG**
  
    ```mysql
    SELECT AVG(field_name) FROM tb_name [WHERE clause]
    ```
    Function: return the average value of a specific column.  
    Return Data Type: `double`.  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`.  
    Applied to: table/STable. 


- **WAVG**
  
    ```mysql
    SELECT WAVG(field_name) FROM tb_name WHERE clause
    ```
    Function: return the time-weighted average value of a specific column  
    Return Data Type: `double`  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`  
    Applied to: table/STable


- **SUM**
  
    ```mysql
    SELECT SUM(field_name) FROM tb_name [WHERE clause]
    ```
    Function: return the sum of a specific column.  
    Return Data Type: `long integer` or `double`.  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`.  
    Applied to: table/STable. 


- **STDDEV**
  
    ```mysql
    SELECT STDDEV(field_name) FROM tb_name [WHERE clause]
    ```
    Function: returns the standard deviation of a specific column.  
    Return Data Type: double.  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`.  
    Applied to: table. 


- **LEASTSQUARES**
    ```mysql
    SELECT LEASTSQUARES(field_name) FROM tb_name [WHERE clause]
    ```
    Function: performs a linear fit to the primary timestamp and the specified column. 
    Return Data Type: return a string of the coefficient and the interception of the fitted line.  
    Applicable Data Types: all types except timestamp, binary, nchar, bool.  
    Applied to: table.  
    Note: The timestmap is taken as the independent variable while the specified column value is taken as the dependent variables.


### Selector Functions

- **MIN**
  
    ```mysql
    SELECT MIN(field_name) FROM {tb_name | stb_name} [WHERE clause]
    ```
    Function: return the minimum value of a specific column.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types except timestamp, binary, nchar, bool.  
    Applied to: table/STable.  


- **MAX**
    ```mysql
    SELECT MAX(field_name) FROM { tb_name | stb_name } [WHERE clause]
    ```
    Function: return the maximum value of a specific column.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types except timestamp, binary, nchar, bool.  
    Applied to: table/STable.    


- **FIRST**
  
    ```mysql
    SELECT FIRST(field_name) FROM { tb_name | stb_name } [WHERE clause]
    ```
    Function: return the first non-NULL value.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types.  
    Applied to: table/STable.  
    Note: To return all columns, use first(*). 


- **LAST**
    ```mysql
    SELECT LAST(field_name) FROM { tb_name | stb_name } [WHERE clause]
    ```
    Function: return the last non-NULL value.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types.  
    Applied to: table/STable.  
    Note: To return all columns, use last(*). 


- **TOP**
  
    ```mysql
    SELECT TOP(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
    ```
    Function: return the `k` largest values.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`.  
    Applied to: table/STable.  
    Note: 
    1) Valid range of `k`: 1≤*k*≤100
    2) The associated `timestamp` will be returned too.  


- **BOTTOM**
  
    ```mysql
    SELECT BOTTOM(field_name, K) FROM { tb_name | stb_name } [WHERE clause]
    ```
    Function: return the `k` smallest values.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`.  
    Applied to: table/STable.  
    Note: 
    1) valid range of `k`: 1≤*k*≤100; 
    2) The associated `timestamp` will be returned too. 


- **PERCENTILE**
    ```mysql
    SELECT PERCENTILE(field_name, P) FROM { tb_name | stb_name } [WHERE clause]
    ```
    Function: the value of the specified column below which `P` percent of the data points fall.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`. 
    Applied to: table/STable.  
    Note: The range of `P` is `[0, 100]`. When `P=0` , `PERCENTILE` returns the equal value as `MIN`; when `P=100`, `PERCENTILE` returns the equal value as `MAX`. 


- **LAST_ROW**
    ```mysql
    SELECT LAST_ROW(field_name) FROM { tb_name | stb_name } 
    ```
    Function: return the last row.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types.  
    Applied to: table/STable.  
    Note: different from last, last_row returns the last row even if it has NULL values. 


### Transformation Functions

- **DIFF**
    ```mysql
    SELECT DIFF(field_name) FROM tb_name [WHERE clause]
    ```
    Function: return the difference between successive values of the specified column.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`.  
    Applied to: table. 


- **SPREAD**
    ```mysql
    SELECT SPREAD(field_name) FROM { tb_name | stb_name } [WHERE clause]
    ```
    Function: return the difference between the maximum and the mimimum value.  
    Return Data Type: the same data type.  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`.  
    Applied to: table/STable.  
    Note: spread gives the range of data variation in a table/supertable; it is equivalent to `MAX()` - `MIN()`


- **Arithmetic Operations**
    ```mysql
    SELECT field_name [+|-|*|/|%][Value|field_name] FROM { tb_name | stb_name }  [WHERE clause]
    ```
    Function: arithmetic operations on the selected columns.  
    Return Data Type: double.  
    Applicable Data Types: all types except `timestamp`, `binary`, `nchar`, `bool`.
    Applied to: table/STable.  
    Note: 1) bracket can be used for operation priority; 2) If a column has NULL value, the result is NULL. 


## Downsampling

Time-series data are usually sampled by sensors at a very high frequency, but more often we are only interested in the downsampled, aggregated data of each timeline. TDengine provides a convenient way to downsample the highly frequently sampled data points as well as filling the missing data with a variety of interpolation choices.

```mysql
SELECT function_list FROM tb_name 
  [WHERE where_condition]
  INTERVAL (interval)
  [FILL ({NONE | VALUE | PREV | NULL | LINEAR})]

SELECT function_list FROM stb_name 
  [WHERE where_condition]
  [GROUP BY tags]
  INTERVAL (interval)
  [FILL ({ VALUE | PREV | NULL | LINEAR})]
```

The downsampling time window is defined by `interval`, which is at least 10 milliseconds. The query returns a new series of downsampled data that has a series of fixed timestamps with an increment of `interval`. 

For the time being, only function count, avg, sum, stddev, leastsquares, percentile, min, max, first, last are supported. Functions that may return multiple rows are not supported. 

You can also use `FILL` to interpolate the intervals that don't contain any data.`FILL` currently supports four different interpolation strategies which are listed below:

| Interpolation                     | Usage                                                        |
| --------------------------------- | ------------------------------------------------------------ |
| `FILL(VALUE, val1 [, val2, ...])` | Interpolate with specified constants                         |
| `FILL(PREV)`                      | Interpolate with the value at the previous timestamp         |
| `FILL(LINEAR)`                    | Linear interpolation with the non-null values at the previous timestamp and at the next timestamp |
| `FILL(NULL)`                      | Interpolate with **`NULL`** value                            |

 A few downsampling examples:

- Find the number of data points, the maximum value of `col1` and minimum value of `col2` in a tb1 for every 10 minutes in the last 5 hours:
    ```mysql
    SELECT COUNT(*), MAX(col1), MIN(col2) FROM tb1 WHERE ts > NOW - 5h INTERVAL (10m)
    ```


- Fill the above downsampling results using constant-value interpolation:
    ```mysql
    SELECT COUNT(*), MAX(col1), MIN(col2) FROM tb1 WHERE ts > NOW - 5h INTERVAL(10m) FILL(VALUE, 0, 1, -1)
    ```
    Note that the number of constant values in `FILL()` should be equal or fewer than the number of functions in the `SELECT` clause. Exceeding fill constants will be ignored.


- Fill the above downsampling results using `PREV` interpolation:
    ```mysql
    SELECT COUNT(*), MAX(col1), MIN(col2) FROM tb1 WHERE ts > NOW - 5h INTERVAL(10m) FILL(PREV)
    ```
    This will interpolate missing data points with the value at the previous timestamp.


- Fill the above downsampling results using `NULL` interpolation:
    ```mysql
    SELECT COUNT(*), MAX(col1), MIN(col2) FROM tb1 WHERE ts > NOW - 5h INTERVAL(10m) FILL(NULL)
    ```
    Fill **`NULL`** to the interpolated data points.

Notes:
1. `FILL` can generate tons of interpolated data points if the interval is small and the queried time range is large. So always remember to specify a time range when using interpolation. For each query with interpolation, the result set can not exceed 10,000,000 records.
2. The result set will always be sorted by time in ascending order.
3. If the query object is a supertable, then all the functions will be applied to all the tables that qualify the `WHERE` conditions. If the `GROUP BY` clause is also applied, the result set will be sorted ascendingly by time in each single group, otherwise, the result set will be sorted ascendingly by time as a whole.

