# TAOS SQL

TDengine provides a SQL-style language, TAOS SQL, to insert or query data, and support other common tips. To finish this document, you should have some understanding about SQL.

TAOS SQL is the main tool for users to write and query data to TDengine. TAOS SQL provides a style and mode similar to standard SQL to facilitate users to get started quickly. Strictly speaking, TAOS SQL is not and does not attempt to provide SQL standard syntax. In addition, since TDengine does not provide deletion function for temporal structured data, the relevant function of data deletion is non-existent in TAO SQL.

Let’s take a look at the conventions used for syntax descriptions.

- The content in < > is what the user needs to enter, but do not enter < > itself
- [] indicates that the content is optional, but do not enter [] itself
- "|" means you can select one of multiple choices, but you cannot enter | yourself
- "…" means repeating for as many times

In order to better explain the rules and characteristics of SQL syntax, this document assumes that there is a data set. Take smart meters as an example, each smart meter collects three metrics: current, voltage and phase. It is modeled as follows:

```mysql
taos> DESCRIBE meters;
             Field              |        Type        |   Length    |    Note    |
=================================================================================
 ts                             | TIMESTAMP          |           8 |            |
 current                        | FLOAT              |           4 |            |
 voltage                        | INT                |           4 |            |
 phase                          | FLOAT              |           4 |            |
 location                       | BINARY             |          64 | TAG        |
 groupid                        | INT                |           4 | TAG        |
```

The data set contains data from four smart meters, which correspond to four sub-tables according to the modeling rules of TDengine, and their names are D1001, D1002, D1003 and D1004 respectively.

## <a class="anchor" id="data-type"></a> Data Types

With TDengine, the most important thing is timestamp. When creating and inserting records and querying history records, you need to specify a timestamp. The timestamp has the following rules:

- Time Format: 'YYYY-MM-DD HH:mm:ss.MS', default in milliseconds. For example,'2017-08-12 18:52:58.128'
- Internal Function **now** : this is the current time of the server
- When inserting a record, if timestamp is NOW, then use the server current time.
- Epch Time: a timestamp value can also be a long integer representing milliseconds since 1970-01-01 08:00:00.000.
- Arithmetic operations can be applied to timestamp. For example: now-2h represents a timestamp which is 2 hours ago from the current server time. Units include u( microsecond), a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks). In `select * from t1 where ts > now-2w and ts <= now-1w`, which queries data of the whole week before two weeks. To specify the interval of down sampling, you can also use n(calendar month) and y(calendar year) as time units.

Default time precision of TDengine is millisecond, you can change it to microseocnd by setting parameter enableMicrosecond. 

In TDengine, the following 10 data types can be used in data model of an ordinary table.

|      | **Data Type** | **Bytes** | **Note**                                                     |
| ---- | ------------- | --------- | ------------------------------------------------------------ |
| 1    | TIMESTAMP     | 8         | Time stamp. Default in milliseconds, and support microseconds. Starting from 1970-01-01 00:00:00. 000 (UTC/GMT), the timing cannot be earlier than this time. |
| 2    | INT           | 4         | A nullable integer type with a range of [-2^31+1, 2^31-1 ]   |
| 3    | BIGINT        | 8         | A nullable integer type with a range of [-2^59, 2^59 ]       |
| 4    | FLOAT         | 4         | A standard nullable float type with 6 -7 significant digits and a range of [-3.4E38, 3.4E38] |
| 5    | DOUBLE        | 8         | A standard nullable double float type with 15-16 significant digits and a range of [-1.7E308, 1.7E308] |
| 6    | BINARY        | Custom    | Used to record ASCII strings. Theoretically, the maximum length can be 16,374 bytes, but since each row of data can be up to 16K bytes, the actual limit is generally smaller than the theoretical value. Binary only supports string input, and single quotation marks are used at both ends of the string, otherwise all English will be automatically converted to lowercase. When using, the size must be specified. For example, binary (20) defines a string with a maximum length of 20 characters, and each character occupies 1 byte of storage space. In this case, if the user string exceeds 20 bytes, an error will be reported. For single quotation marks in strings, they can be represented by escape character backslash plus single quotation marks, that is\ '. |
| 7    | SMALLINT      | 2         | A nullable integer type with a range of [-32767, 32767]      |
| 8    | TINYINT       | 1         | A nullable integer type with a range of [-127, 127]          |
| 9    | BOOL          | 1         | Boolean type，{true, false}                                  |
| 10   | NCHAR         | Custom    | Used to record non-ASCII strings, such as Chinese characters. Each nchar character takes up 4 bytes of storage space. Single quotation marks are used at both ends of the string, and escape characters are required for single quotation marks in the string, that is \’. When nchar is used, the string size must be specified. A column of type nchar (10) indicates that the string of this column stores up to 10 nchar characters, which will take up 40 bytes of space. If the length of the user string exceeds the declared length, an error will be reported. |



**Tips**:

1. TDengine is case-insensitive to English characters in SQL statements and automatically converts them to lowercase for execution. Therefore, the user's case-sensitive strings and passwords need to be enclosed in single quotation marks.
2. Avoid using BINARY type to save non-ASCII type strings, which will easily lead to errors such as garbled data. The correct way is to use NCHAR type to save Chinese characters.

## <a class="anchor" id="management"></a>Database Management

- **Create a Database**

   ```mysql
      CREATE DATABASE [IF NOT EXISTS] db_name [KEEP keep] [DAYS days] [UPDATE 1];
   ```

Note:

1. KEEP is how long the data of the database is kept, the default is 3650 days (10 years), and the database will automatically delete the data expired;
2. UPDATE marks the database support updating the same timestamp data;
3. Maximum length of the database name is 33;
4. Maximum length of a SQL statement is 65480 characters;
5.  Database has more storage-related configuration parameters, see System Management.

- **Show current system parameters**

    ```mysql
    SHOW VARIABLES;
    ```

- **Use a database**

    ```mysql
    USE db_name;
    ```
    Use/switch database

- **Drop a database**
    ```mysql
    DROP DATABASE [IF EXISTS] db_name;
    ```
    Delete a database, all data tables included will be deleted. Please use with caution.

- **Modify database parameters**

    ```mysql
    ALTER DATABASE db_name COMP 2;
    ```
    COMP parameter modifies the database file compression flag bit, with the default value of 2 and the value range is [0, 2]. 0 means no compression, 1 means one-stage compression, and 2 means two-stage compression.

    ```mysql
    ALTER DATABASE db_name REPLICA 2;
    ```
    REPLICA parameter refers to the number of replicas of the modified database, and the value range is [1, 3]. For use in a cluster, the number of replicas must be less than or equal to the number of DNODE.

    ```mysql
    ALTER DATABASE db_name KEEP 365;
    ```
    The KEEP parameter refers to the number of days to save a modified data file. The default value is 3650, and the value range is [days, 365000]. It must be greater than or equal to the days parameter value.

   ```mysql
   ALTER DATABASE db_name QUORUM 2;
   ```
   QUORUM parameter refers to the number of confirmations required for successful data writing, and the value range is [1, 3]. For asynchronous replication, quorum is set to 1, and the virtual node with master role can confirm it by itself. For synchronous replication, it needs to be at least 2 or greater. In principle, Quorum > = 1 and Quorum < = replica number, which is required when starting a synchronization module instance.

    ```mysql
    ALTER DATABASE db_name BLOCKS 100;
    ```
   BLOCKS parameter is the number of cache-sized memory blocks in each VNODE (TSDB), so the memory size used for a VNODE equals roughly (cache * blocks). Value range is [3,1000].

    ```mysql
    ALTER DATABASE db_name CACHELAST 0;
    ```
    CACHELAST parameter controls whether last_row of the data subtable is cached in memory. The default value is 0, and the value range is [0, 1]. Where 0 means not enabled and 1 means enabled. (supported from version 2.0. 11)
    
    **Tips**: After all the above parameters are modified, show databases can be used to confirm whether the modification is successful.

- **Show all databases in system**

    ```mysql
    SHOW DATABASES;
    ```

## <a class="anchor" id="table"></a> Table Management

- Create a table
Note:

1. The first field must be a timestamp, and system will set it as the primary key;
2. The max length of table name is 192;
3. The length of each row of the table cannot exceed 16k characters;
4. Sub-table names can only consist of letters, numbers, and underscores, and cannot begin with numbers
5. If the data type binary or nchar is used, the maximum number of bytes should be specified, such as binary (20), which means 20 bytes;

- **Create a table via STable**

    ```mysql
    CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name TAGS (tag_value1, ...);
    ```
    Use a STable as template and assign tag values to create a data table.

- **Create a data table using STable as a template and specify a specific tags column**

   ```mysql
   CREATE TABLE [IF NOT EXISTS] tb_name USING stb_name (tag_name1, ...) TAGS (tag_value1, ...);
   ```
   Using the specified STable as a template, specify the values of some tags columns to create a data table. (Unspecified tags columns are set to null values.)
   Note: This method has been supported since version 2.0. 17. In previous versions, tags columns were not allowed to be specified, but the values of all tags columns must be explicitly given.

- **Create tables in batches**

   ```mysql
   CREATE TABLE [IF NOT EXISTS] tb_name1 USING stb_name TAGS (tag_value1, ...) tb_name2 USING stb_name TAGS (tag_value2, ...) ...;
   ```
   Create a large number of data tables in batches faster. (Server side 2.0. 14 and above)
   
   Note:
   1. The method of batch creating tables requires that the data table must use STable as a template.
   2. On the premise of not exceeding the length limit of SQL statements, it is suggested that the number of tables in a single statement should be controlled between 1000 and 3000, which will obtain an ideal speed of table building.

- **Drop a table**
    
    ```mysql
    DROP TABLE [IF EXISTS] tb_name;
    ```

- **Show all data table information under the current database**

    ```mysql
    SHOW TABLES [LIKE tb_name_wildcar];
    ```
    Show all data table information under the current database.
    Note: Wildcard characters can be used to match names in like. The maximum length of this wildcard character string cannot exceed 24 bytes.
    Wildcard matching: 1) '%' (percent sign) matches 0 to any number of characters; 2) '_' underscore matches one character.

- **Modify display character width online**

    ```mysql
    SET MAX_BINARY_DISPLAY_WIDTH <nn>;
    ```

- **Get schema information of a table**
  
    ```mysql
    DESCRIBE tb_name;
    ```

- **Add a column to table**

   ```mysql
   ALTER TABLE tb_name ADD COLUMN field_name data_type;
   ```
   Note:
   1. The maximum number of columns is 1024 and the minimum number is 2;
   2. The maximum length of a column name is 64; 

- **Drop a column in table**

   ```mysql
   ALTER TABLE tb_name DROP COLUMN field_name; 
   ```
   If the table is created through a STable, the operation of table schema changing can only be carried out on the STable. Moreover, the schema changes for the STable take effect for all tables created through the schema. For tables that are not created through STables, you can modify the table schema directly.

## <a class="anchor" id="super-table"></a> STable Management

Note: In 2.0. 15.0 and later versions, STABLE reserved words are supported. That is, in the instruction description later in this section, the three instructions of CREATE, DROP and ALTER need to write TABLE instead of STABLE in the old version as the reserved word.

- **Create a STable**

    ```mysql
    CREATE STABLE [IF NOT EXISTS] stb_name (timestamp_field_name TIMESTAMP, field1_name data_type1 [, field2_name data_type2 ...]) TAGS (tag1_name tag_type1, tag2_name tag_type2 [, tag3_name tag_type3]);
    ```
    Similiar to a standard table creation SQL, but you need to specify name and type of TAGS field.
    
    Note:
    
    1. Data types of TAGS column cannot be timestamp;
    2. No duplicated TAGS column names;
    3. Reversed word cannot be used as a TAGS column name;
    4. The maximum number of TAGS is 128, and at least 1 TAG allowed, with a total length of no more than 16k characters.

- **Drop a STable**

    ```mysql
    DROP STABLE [IF EXISTS] stb_name;
    ```
    Drop a STable automatically deletes all sub-tables created through the STable.

- **Show all STable information under the current database**

    ```mysql
    SHOW STABLES [LIKE tb_name_wildcard];
    ```
    View all STables under the current database and relevant information, including name, creation time, column number, tag number, number of tables created through the STable, etc.

- **Obtain schema information of a STable**

    ```mysql
    DESCRIBE stb_name;
    ```

- **Add column to STable**

    ```mysql
    ALTER STABLE stb_name ADD COLUMN field_name data_type;
    ```

- **Drop column in STable**

    ```mysql
    ALTER STABLE stb_name DROP COLUMN field_name; 
    ```

## <a class="anchor" id="tags"></a> TAG Management in STable

- **Add a tag**

    ```mysql
    ALTER STABLE stb_name ADD TAG new_tag_name tag_type;
    ```
    Add a new tag to the STable and specify a type of the new tag. The total number of tags cannot exceed 128 and the total length does not exceed 16K characters.

- **Drop a tag**

    ```mysql
    ALTER STABLE stb_name DROP TAG tag_name;
    ```
    Delete a tag of STable. After deleting the tag, all sub-tables under the STable will also automatically delete the same tag.

- **Modify a tag name**

    ```mysql
    ALTER STABLE stb_name CHANGE TAG old_tag_name new_tag_name;
    ```
    Modify a tag name of STable. After modifying, all sub-tables under the STable will automatically update the new tag name.

- **Modify a tag value of sub-table**
    
    ```mysql
    ALTER TABLE tb_name SET TAG tag_name=new_tag_value;
    ```
    Note: Except that the operation of tag value updating is carried out for sub-tables, all other tag operations (adding tags, deleting tags, etc.) can only be applied to STable, and cannot be operated on a single sub-table. After adding a tag to a STable, all tables established based on that will automatically add a new tag, and the default value is NULL.

## <a class="anchor" id="insert"></a> Data Writing

- **Insert a record**

  ```mysql
  INSERT INTO tb_name VALUES (field_value, ...);
  ```
  Insert a record into table tb_name.

- **Insert a record with data corresponding to a given column**
   
    ```mysql
    INSERT INTO tb_name (field1_name, ...) VALUES (field1_value1, ...);
    ```
    Insert a record into table tb_name, and the data corresponds to a given column. For columns that do not appear in the SQL statement, database will automatically populate them with NULL. Primary key (timestamp) cannot be NULL.

- **Insert multiple records**

    ```mysql
    INSERT INTO tb_name VALUES (field1_value1, ...) (field1_value2, ...) ...;
    ```
    Insert multiple records into table tb_name.

- **Insert multiple records into a given column**
    
    ```mysql
    INSERT INTO tb_name (field1_name, ...) VALUES (field1_value1, ...) (field1_value2, ...) ...;
    ```
    Insert multiple records into a given column of table tb_name.

- **Insert multiple records into multiple tables**
    
    ```mysql
    INSERT INTO tb1_name VALUES (field1_value1, ...) (field1_value2, ...) ...
                tb2_name VALUES (field1_value1, ...) (field1_value2, ...) ...;
    ```
    Insert multiple records into tables tb1_name and tb2_name at the same time.

- **Insert multiple records per column into multiple tables**

    ```mysql
    INSERT INTO tb1_name (tb1_field1_name, ...) VALUES (field1_value1, ...) (field1_value2, ...) ...
                tb2_name (tb2_field1_name, ...) VALUES (field1_value1, ...) (field1_value2, ...) ...;
    ```
    Insert multiple records per column into tables tb1_name and tb2_name at the same time.
    Note: The timestamp of the oldest record allowed to be inserted is relative to the current server time, minus the configured keep value (days of data retention), and the timestamp of the latest record allowed to be inserted is relative to the current server time, plus the configured days value (interval of data storage in the data file, in days). Both keep and days can be specified when the database is created, and the default values are 3650 days and 10 days, respectively.

- <a class="anchor" id="auto_create_table"></a> Automatically create a table when inserting

    ```mysql
    INSERT INTO tb_name USING stb_name TAGS (tag_value1, ...) VALUES (field_value1, ...);
    ```
    If user is not sure whether a table exists when writing data, the automatic table building syntax can be used to create a non-existent table when writing. If the table already exists, no new table will be created. When automatically creating a table, it is required to use the STable as a template and specify tags value for the data table.

- **Automatically create a table when inserting, and specify a given tags column**
  
    ```mysql
    INSERT INTO tb_name USING stb_name (tag_name1, ...) TAGS (tag_value1, ...) VALUES (field_value1, ...);
    ```
    During automatic table creation, only the values of some tags columns can be specified, and the unspecified tags columns will be null.

**History writing**: The IMPORT or INSERT command can be used. The syntax and function of IMPORT are exactly the same as those of INSERT.

Note: For SQL statements in insert type, the stream parsing strategy we adopt will still execute the correct part of SQL before the following errors are found. In the following sql, insert statement is invalid, but d1001 will still be created.

```mysql
taos> CREATE TABLE meters(ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(location BINARY(30), groupId INT);
Query OK, 0 row(s) affected (0.008245s)

taos> SHOW STABLES;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 meters                         | 2020-08-06 17:50:27.831 |       4 |      2 |           0 |
Query OK, 1 row(s) in set (0.001029s)

taos> SHOW TABLES;
Query OK, 0 row(s) in set (0.000946s)

taos> INSERT INTO d1001 USING meters TAGS('Beijing.Chaoyang', 2) VALUES('a');

DB error: invalid SQL: 'a' (invalid timestamp) (0.039494s)

taos> SHOW TABLES;
           table_name           |      created_time       | columns |          stable_name           |
======================================================================================================
 d1001                          | 2020-08-06 17:52:02.097 |       4 | meters                         |
Query OK, 1 row(s) in set (0.001091s)
```

## <a class="anchor" id="select"></a> Data Query

### Query Syntax:

```mysql
SELECT select_expr [, select_expr ...]
    FROM {tb_name_list}
    [WHERE where_condition]
    [INTERVAL (interval_val [, interval_offset])]
    [SLIDING sliding_val]
    [FILL fill_val]
    [GROUP BY col_list]
    [ORDER BY col_list { DESC | ASC }]
    [SLIMIT limit_val [SOFFSET offset_val]]
    [LIMIT limit_val [OFFSET offset_val]]
    [>> export_file];
```

#### SELECT Clause

A select clause can be a subquery of UNION and another query.

#### Wildcard character

The wildcard \* can be used to refer to all columns. For ordinary tables, there’re only ordinary columns in results.

```mysql
taos> SELECT * FROM d1001;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
Query OK, 3 row(s) in set (0.001165s)
```

For Stables, wildcards contain *tag columns*.

```mysql
taos> SELECT * FROM meters;
           ts            |       current        |   voltage   |        phase         |            location            |   groupid   |
=====================================================================================================================================
 2018-10-03 14:38:05.500 |             11.80000 |         221 |              0.28000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:16.600 |             13.40000 |         223 |              0.29000 | Beijing.Haidian                |           2 |
 2018-10-03 14:38:05.000 |             10.80000 |         223 |              0.29000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:06.500 |             11.50000 |         221 |              0.35000 | Beijing.Haidian                |           3 |
 2018-10-03 14:38:04.000 |             10.20000 |         220 |              0.23000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:16.650 |             10.30000 |         218 |              0.25000 | Beijing.Chaoyang               |           3 |
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 | Beijing.Chaoyang               |           2 |
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 | Beijing.Chaoyang               |           2 |
Query OK, 9 row(s) in set (0.002022s)
```

Wildcards support table name prefixes, the two following SQL statements will return all columns:

```mysql
SELECT * FROM d1001;
SELECT d1001.* FROM d1001;
```

In Join query, the results returned by \* with prefix and \* without prefix are different. \* returns all column data of all tables (excluding tags), while wildcards with prefix only return column data of the corresponding table.

```mysql
taos> SELECT * FROM d1001, d1003 WHERE d1001.ts=d1003.ts;
           ts            | current |   voltage   |    phase     |           ts            | current |   voltage   |    phase     |
==================================================================================================================================
 2018-10-03 14:38:05.000 | 10.30000|         219 |      0.31000 | 2018-10-03 14:38:05.000 | 10.80000|         223 |      0.29000 |
Query OK, 1 row(s) in set (0.017385s)
```
```mysql
taos> SELECT d1001.* FROM d1001,d1003 WHERE d1001.ts = d1003.ts;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |         219 |              0.31000 |
Query OK, 1 row(s) in set (0.020443s)
```

In the process of using SQL functions for query, some SQL functions support wildcard operation. The difference is that the `count(\*)` function returns only one column, but the `first`,`last`,`last_row` functions return all columns.

```mysql
taos> SELECT COUNT(*) FROM d1001;
       count(*)        |
========================
                     3 |
Query OK, 1 row(s) in set (0.001035s)
```

```mysql
taos> SELECT FIRST(*) FROM d1001;
        first(ts)        |    first(current)    | first(voltage) |     first(phase)     |
=========================================================================================
 2018-10-03 14:38:05.000 |             10.30000 |            219 |              0.31000 |
Query OK, 1 row(s) in set (0.000849s)
```

####  Tag Column

Since version 2.0. 14, it is supported to specify *tag column* in queries of ordinary tables, and the values of tag columns will be returned together with the data of other ordinary columns.

```mysql
taos> SELECT location, groupid, current FROM d1001 LIMIT 2;
            location            |   groupid   |       current        |
======================================================================
 Beijing.Chaoyang               |           2 |             10.30000 |
 Beijing.Chaoyang               |           2 |             12.60000 |
Query OK, 2 row(s) in set (0.003112s)
```

Note: The wildcard \* of ordinary tables does not contain *tag columns*.

#### Obtain the de-duplicated value of a tag column

Since version 2.0. 15, it is supported to specify `DISTINCT` keyword when querying tag columns in STables, which will return all non-duplicate values of given tag columns.

```mysql
SELECT DISTINCT tag_name FROM stb_name;
```

Note: At present, `DISTINCT` keyword only supports deduplication of tag columns of STables, and cannot be used for ordinary columns.

#### Column name in result set

In `SELECT` clause, if there’s no returning of column name in result set, the result set column name defaults to the expression name in `SELECT` clause as the column name. In addition, user can use `AS` to rename the columns in the returned result set. For example:

```mysql
taos> SELECT ts, ts AS primary_key_ts FROM d1001;
           ts            |     primary_key_ts      |
====================================================
 2018-10-03 14:38:05.000 | 2018-10-03 14:38:05.000 |
 2018-10-03 14:38:15.000 | 2018-10-03 14:38:15.000 |
 2018-10-03 14:38:16.800 | 2018-10-03 14:38:16.800 |
Query OK, 3 row(s) in set (0.001191s)
```

However, renaming for one single column is not supported for `first(*)`,`last(*)`,`last_row(*)`.

#### Implicit result column

`Select_exprs` can be the name of a column belongs to a table, or it can be a column-based functional expression or calculation formula, with an upper limit of 256. When user uses `interval` or `group by tags` clause, the timestamp column (the first column) and the tag column in `group by` clause are forced to be returned in the final returned result. Later versions can support turning off the output of implicit columns in `group by` clause, and the column output is completely controlled by select clause.

#### List of STable

The `FROM` keyword can be followed by a list of several tables (STables) or result of a subquery. 

If you do not specify user's current database, you can use the database name before the table name to specify the database to which the table belongs. For example: `power.d1001` to use tables across databases.

```mysql
SELECT * FROM power.d1001;
------------------------------
USE power;
SELECT * FROM d1001;
```

#### Special Functions

Some special query functions can be performed without using FROM clause. Obtain the current database database ():

```mysql
taos> SELECT DATABASE();
           database()           |
=================================
 power                          |
Query OK, 1 row(s) in set (0.000079s)
```

If no default database is specified when logging in, and `USE` command is not used to switch data, then `NULL` is returned.

```mysql
taos> SELECT DATABASE();
           database()           |
=================================
 NULL                           |
Query OK, 1 row(s) in set (0.000184s)
```

Get server and client version numbers:

```mysql
taos> SELECT CLIENT_VERSION();
 client_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000070s)

taos> SELECT SERVER_VERSION();
 server_version() |
===================
 2.0.0.0          |
Query OK, 1 row(s) in set (0.000077s)
```

A server state detection statement. If server is normal, return a number (for example, 1). If server is exceptional, return error code. The SQL syntax can be compatible with the check of TDengine status by connection pool and the check of database server status by third-party tools. And can avoid connection loss of connection pool caused by using a wrong heartbeat detection SQL statement.

```mysql
taos> SELECT SERVER_STATUS();
 server_status() |
==================
               1 |
Query OK, 1 row(s) in set (0.000074s)

taos> SELECT SERVER_STATUS() AS status;
   status    |
==============
           1 |
Query OK, 1 row(s) in set (0.000081s)
```

#### Special keywords in TAOS SQL

>  `TBNAME`: It can be regarded as a special tag in a STable query, representing the name of sub-table involved in the query
>
> _c0: Represents the first column of a table (STable)

#### Tips

Get all sub-table names and related tags information of a STable:

```mysql
SELECT TBNAME, location FROM meters;
```

Statistics of sub-tables number under a STable:

```mysql
SELECT COUNT(TBNAME) FROM meters;
```

The  two queries above only support adding filters for TAGS in Where conditional clause. For example:

```mysql
taos> SELECT TBNAME, location FROM meters;
             tbname             |            location            |
==================================================================
 d1004                          | Beijing.Haidian                |
 d1003                          | Beijing.Haidian                |
 d1002                          | Beijing.Chaoyang               |
 d1001                          | Beijing.Chaoyang               |
Query OK, 4 row(s) in set (0.000881s)

taos> SELECT COUNT(tbname) FROM meters WHERE groupId > 2;
     count(tbname)     |
========================
                     2 |
Query OK, 1 row(s) in set (0.001091s)
```

- You can use \* to return all columns, or given column names. Four operations can be performed on numeric columns, and column names can be given to output columns.
- `WHERE` statement can use various logical decisions to filter numeric values, or wildcards to filter strings
- The output is sorted by default in ascending order by timestamps in the first column, but you can specify descending order (\_c0 refers to the first column timestamp). It is illegal to use ORDER BY to sort other fields.
- Parameter LIMIT controls the number of outputs, and OFFSET specifies which output starts from. LIMIT/OFFSET executes the result set after ORDER BY.
- "> >" output can be exported to a specified file

#### Supported Filtering Operations

| **Operation** | **Note**                      | **Applicable Data Types**           |
| ------------- | ----------------------------- | ----------------------------------- |
| >             | larger than                   | **timestamp** and all numeric types |
| <             | smaller than                  | **timestamp** and all numeric types |
| >=            | larger than or equal to       | **timestamp** and all numeric types |
| <=            | smaller than or equal to      | **timestamp** and all numeric types |
| =             | equal to                      | all types                           |
| <>            | not equal to                  | all types                           |
| between and   | within a certain range        | **timestamp** and all numeric types |
| %             | match with any char sequences | **binary** **nchar**                |
| _             | match with a single char      | **binary** **nchar**                |

1. To filter the range of multiple fields at the same time, you need to use keyword AND to connect different query conditions. The query filtering between different columns connected by OR are not supported at the moment.
2. For filtering a single field, if it is a time filtering condition, only one condition in a statement can be set; however, for other (ordinary) columns or tag columns, OR keyword can be used for query filtering of combined conditions. For example: ((value > 20 AND value < 30) OR (value < 12)).
3. Since version 2.0. 17, condition filtering supports BETWEEN AND syntax. For example, WHERE col2 BETWEEN 1.5 AND 3.25 means that the query condition is "1.5 ≤ col2 ≤ 3.25".

### SQL Example

- For example, table tb1 is created with the following statement
  
    ```mysql
    CREATE TABLE tb1 (ts TIMESTAMP, col1 INT, col2 FLOAT, col3 BINARY(50));
    ```

- Query all records of the last hour of tb1

    ```mysql
    SELECT * FROM tb1 WHERE ts >= NOW - 1h;
    ```

- Look up table tb1 from 2018-06-01 08:00:00. 000 to 2018-06-02 08:00:00. 000, and col3 string is a record ending in'nny ', and the result is in descending order of timestamp:

    ```mysql
    SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC;
    ```

- Query the sum of col1 and col2, and name it complex. The time is greater than 2018-06-01 08:00:00. 000, and col2 is greater than 1.2. As a result, only 10 records are outputted, starting from item 5

    ```mysql
    SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND col2 > 1.2 LIMIT 10 OFFSET 5;
    ```

- Query the records of past 10 minutes, the value of col2 is greater than 3.14, and output the result to the file /home/testoutpu.csv.

    ```mysql
    SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutpu.csv;
    ```

<a class="anchor" id="functions"></a>

## SQL Functions

TDengine supports aggregations over data, they are listed below:

- **COUNT**

    ```mysql
    SELECT COUNT([*|field_name]) FROM tb_name [WHERE clause];
    ```
    Function: record the number of rows or non-null values in a column of statistics/STable.
    
    Returned result data type: long integer INT64.
    
    Applicable Fields: Applied to all fields.
    
    Applied to: **table, STable**.
    
    Note:
    1. You can use \* instead of specific fields, and use *() to return the total number of records.
    2. The query results for fields of the same table (excluding NULL values) are the same.
    3. If the statistic object is a specific column, return the number of records with non-NULL values in that column.
    
    Example:
    
    ```mysql
    taos> SELECT COUNT(*), COUNT(voltage) FROM meters;
        count(*)        |    count(voltage)     |
    ================================================
                        9 |                     9 |
    Query OK, 1 row(s) in set (0.004475s)
  
    taos> SELECT COUNT(*), COUNT(voltage) FROM d1001;
        count(*)        |    count(voltage)     |
    ================================================
                        3 |                     3 |
    Query OK, 1 row(s) in set (0.001075s)
    ```

- **AVG**

  ```mysql
  SELECT AVG(field_name) FROM tb_name [WHERE clause];
  ```
  Function: return the average value of a column in statistics/STable.
  
  Return Data Type: double.
  
  Applicable Fields: all types except timestamp, binary, nchar, bool.
  
  Applied to: **table,STable**.
  
  Example:
  
  ```mysql
  taos> SELECT AVG(current), AVG(voltage), AVG(phase) FROM meters;
      avg(current)        |       avg(voltage)        |        avg(phase)         |
  ====================================================================================
              11.466666751 |             220.444444444 |               0.293333333 |
  Query OK, 1 row(s) in set (0.004135s)
  
  taos> SELECT AVG(current), AVG(voltage), AVG(phase) FROM d1001;
      avg(current)        |       avg(voltage)        |        avg(phase)         |
  ====================================================================================
              11.733333588 |             219.333333333 |               0.316666673 |
  Query OK, 1 row(s) in set (0.000943s)
  ```

- **TWA**
   
  ```mysql
  SELECT TWA(field_name) FROM tb_name WHERE clause;
  ```
  
  Function: Time weighted average function. The time-weighted average of a column in a statistical table over a period of time.
  
  Return Data Type: double.
  
  Applicable Fields: all types except timestamp, binary, nchar, bool.
  
  Applied to: **table**.

- **SUM**

  ```mysql
  SELECT SUM(field_name) FROM tb_name [WHERE clause];
  ```
  
  Function: return the sum of a statistics/STable.
  
  Return Data Type: long integer INMT64 and Double.
  
  Applicable Fields: All types except timestamp, binary, nchar, bool.
  
  Applied to:  **table,STable**.
  
  Example:

  ```mysql
  taos> SELECT SUM(current), SUM(voltage), SUM(phase) FROM meters;
      sum(current)        |     sum(voltage)      |        sum(phase)         |
  ================================================================================
              103.200000763 |                  1984 |               2.640000001 |
  Query OK, 1 row(s) in set (0.001702s)
  
  taos> SELECT SUM(current), SUM(voltage), SUM(phase) FROM d1001;
      sum(current)        |     sum(voltage)      |        sum(phase)         |
  ================================================================================
              35.200000763 |                   658 |               0.950000018 |
  Query OK, 1 row(s) in set (0.000980s)
   ```

- **STDDEV**
  
  ```mysql
  SELECT STDDEV(field_name) FROM tb_name [WHERE clause];
  ```
  
  Function: Mean square deviation of a column in statistics table.
  
  Return Data Type: Double.
  
  Applicable Fields: All types except timestamp, binary, nchar, bool.
  
  Applied to: **table**. (also support **STable** since version 2.0.15.1)
  
  Example:
  
  ```mysql
  taos> SELECT STDDEV(current) FROM d1001;
      stddev(current)      |
  ============================
              1.020892909 |
  Query OK, 1 row(s) in set (0.000915s)
  ```

- **LEASTSQUARES**
    ```mysql
    SELECT LEASTSQUARES(field_name, start_val, step_val) FROM tb_name [WHERE clause];
    ```
    Function: Value of a column in statistical table is a fitting straight equation of primary key (timestamp). Start_val is the initial value of independent variable, and step_val is the step size value of independent variable.
    
    Return Data Type: String expression (slope, intercept).
    
    Applicable Fields: All types except timestamp, binary, nchar, bool.
    
    Note: Independent variable is the timestamp, and dependent variable is the value of the column.
    
    Applied to: **table**.
    
    Example:
    ```mysql
    taos> SELECT LEASTSQUARES(current, 1, 1) FROM d1001;
                leastsquares(current, 1, 1)             |
    =====================================================
    {slop:1.000000, intercept:9.733334}                 |
    Query OK, 1 row(s) in set (0.000921s)
    ```

### Selector Functions

- **MIN**
    ```mysql
    SELECT MIN(field_name) FROM {tb_name | stb_name} [WHERE clause];
    ```
    Function: return the minimum value of a specific column in statistics/STable.
    
    Return Data Type: Same as applicable fields.
    
    Applicable Fields: All types except timestamp, binary, nchar, bool.
    
    Example:

    ```mysql
    taos> SELECT MIN(current), MIN(voltage) FROM meters;
        min(current)     | min(voltage) |
    ======================================
                10.20000 |          218 |
    Query OK, 1 row(s) in set (0.001765s)
    
    taos> SELECT MIN(current), MIN(voltage) FROM d1001;
        min(current)     | min(voltage) |
    ======================================
                10.30000 |          218 |
    Query OK, 1 row(s) in set (0.000950s)
    ```

- **MAX**

   ```mysql
   SELECT MAX(field_name) FROM { tb_name | stb_name } [WHERE clause];
   ```
   
   Function: return the maximum value of a specific column in statistics/STable.
   
   Return Data Type: Same as applicable fields.
   
   Applicable Fields: All types except timestamp, binary, nchar, bool.
   
   Example:

   ```mysql
   taos> SELECT MAX(current), MAX(voltage) FROM meters;
       max(current)     | max(voltage) |
   ======================================
               13.40000 |          223 |
   Query OK, 1 row(s) in set (0.001123s)
  
   taos> SELECT MAX(current), MAX(voltage) FROM d1001;
       max(current)     | max(voltage) |
   ======================================
               12.60000 |          221 |
   Query OK, 1 row(s) in set (0.000987s)
   ```

- **FIRST**

   ```mysql
   SELECT FIRST(field_name) FROM { tb_name | stb_name } [WHERE clause];
   ```
   
   Function: The first non-NULL value written into a column in statistics/STable.
   
   Return Data Type: Same as applicable fields.
   
   Applicable Fields: All types.
   
   Note:
   1. To return the first (minimum timestamp) non-NULL value of each column, use FIRST (\*);
   2. if all columns in the result set are NULL values, the return result of the column is also NULL;
   3. If all columns in the result set are NULL values, no result is returned.
   
   Example:

   ```mysql
   taos> SELECT FIRST(*) FROM meters;
          first(ts)        |    first(current)    | first(voltage) |     first(phase)     |
   =========================================================================================
   2018-10-03 14:38:04.000 |             10.20000 |            220 |              0.23000 |
   Query OK, 1 row(s) in set (0.004767s)

   taos> SELECT FIRST(current) FROM d1002;
         first(current)    |
   =======================
                10.20000 |
   Query OK, 1 row(s) in set (0.001023s)
   ```

- 

- **LAST**

   ```mysql
   SELECT LAST(field_name) FROM { tb_name | stb_name } [WHERE clause];
   ```
   
   Function: The last non-NULL value written by the value of a column in statistics/STable.
   
   Return Data Type: Same as applicable fields.
   
   Applicable Fields: All types.
   
   Note:
   1. To return the last (maximum timestamp) non-NULL value of each column, use LAST (\*);
   2. If a column in the result set has a NULL value, the returned result of the column is also NULL; if all columns in the result set have NULL values, no result is returned.
   
   Example:

   ```mysql
    taos> SELECT LAST(*) FROM meters;
            last(ts)         |    last(current)     | last(voltage) |     last(phase)      |
    ========================================================================================
    2018-10-03 14:38:16.800 |             12.30000 |           221 |              0.31000 |
    Query OK, 1 row(s) in set (0.001452s)

    taos> SELECT LAST(current) FROM d1002;
        last(current)     |
    =======================
                10.30000 |
    Query OK, 1 row(s) in set (0.000843s)
   ```

- **TOP**
   
    ```mysql
    SELECT TOP(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
    ```
    Function: The top k non-NULL values of a column in statistics/STable. If there are more than k column values tied for the largest, the one with smaller timestamp is returned.
    
    Return Data Type: Same as applicable fields.
    
    Applicable Fields: All types except timestamp, binary, nchar, bool.
    
    Note:
    1. The range of *k* value is 1≤*k*≤100;
    2. System also returns the timestamp column associated with the record.
    
    Example:

    ```mysql
    taos> SELECT TOP(current, 3) FROM meters;
            ts            |   top(current, 3)    |
    =================================================
    2018-10-03 14:38:15.000 |             12.60000 |
    2018-10-03 14:38:16.600 |             13.40000 |
    2018-10-03 14:38:16.800 |             12.30000 |
    Query OK, 3 row(s) in set (0.001548s)
      
    taos> SELECT TOP(current, 2) FROM d1001;
            ts            |   top(current, 2)    |
    =================================================
    2018-10-03 14:38:15.000 |             12.60000 |
    2018-10-03 14:38:16.800 |             12.30000 |
    Query OK, 2 row(s) in set (0.000810s)
    ```

- **BOTTOM**

    ```mysql
    SELECT BOTTOM(field_name, K) FROM { tb_name | stb_name } [WHERE clause];
    ```
    Function: The last k non-NULL values of a column in statistics/STable. If there are more than k column values tied for the smallest, the one with smaller timestamp is returned.
    
    Return Data Type: Same as applicable fields.
    
    Applicable Fields: All types except timestamp, binary, nchar, bool.

    Note:
    1. The range of *k* value is 1≤*k*≤100;
    2. System also returns the timestamp column associated with the record.

    Example:

    ```mysql
    taos> SELECT BOTTOM(voltage, 2) FROM meters;
            ts            | bottom(voltage, 2) |
    ===============================================
    2018-10-03 14:38:15.000 |                218 |
    2018-10-03 14:38:16.650 |                218 |
    Query OK, 2 row(s) in set (0.001332s)
  
    taos> SELECT BOTTOM(current, 2) FROM d1001;
            ts            |  bottom(current, 2)  |
    =================================================
    2018-10-03 14:38:05.000 |             10.30000 |
    2018-10-03 14:38:16.800 |             12.30000 |
    Query OK, 2 row(s) in set (0.000793s)
    ```

- **PERCENTILE**
    ```mysql
    SELECT PERCENTILE(field_name, P) FROM { tb_name } [WHERE clause];
    ```
    Function: Percentile of the value of a column in statistical table.
    
    Return Data Type: Double.
    
    Applicable Fields: All types except timestamp, binary, nchar, bool.
    
    Note: The range of P value is 0 ≤ P ≤ 100. P equals to MIN when, and equals MAX when it’s 100.
    
    Example:

    ```mysql
    taos> SELECT PERCENTILE(current, 20) FROM d1001;
    percentile(current, 20)  |
    ============================
                11.100000191 |
    Query OK, 1 row(s) in set (0.000787s)
    ```

- **APERCENTILE**
    ```mysql
    SELECT APERCENTILE(field_name, P) FROM { tb_name | stb_name } [WHERE clause];
    ```
    Function: The value percentile of a column in statistical table is similar to the PERCENTILE function, but returns approximate results.
    
    Return Data Type: Double.
    
    Applicable Fields: All types except timestamp, binary, nchar, bool.
    
    Note: The range of *P* value is 0 ≤ *P* ≤ 100. *P* equals to MIN when, and equals MAX when it’s 100. APERCENTILE function is recommended, which performs far better than PERCENTILE function.

- **LAST_ROW**
    ```mysql
    SELECT LAST_ROW(field_name) FROM { tb_name | stb_name };
    ```
    Function: Return the last record of a table (STtable).
    
    Return Data Type: Double.
    
    Applicable Fields: All types.
    
    Note: Unlike last function, last_row does not support time range restriction and forces the last record to be returned.
    
    Example:

    ```mysql
    taos> SELECT LAST_ROW(current) FROM meters;
    last_row(current)   |
    =======================
                12.30000 |
    Query OK, 1 row(s) in set (0.001238s)
  
    taos> SELECT LAST_ROW(current) FROM d1002;
    last_row(current)   |
    =======================
                10.30000 |
      Query OK, 1 row(s) in set (0.001042s)
    ```

### Computing Functions

- **DIFF**
    ```mysql
    SELECT DIFF(field_name) FROM tb_name [WHERE clause];
    ```
    Function: Return the value difference between a column and the previous column.
    
    Return Data Type: Same as applicable fields.
    
    Applicable Fields: All types except timestamp, binary, nchar, bool.
    
    Note: The number of output result lines is the total number of lines in the range minus one, and there is no result output in the first line.
    
    Example:

    ```mysql
    taos> SELECT DIFF(current) FROM d1001;
            ts            |    diff(current)     |
    =================================================
    2018-10-03 14:38:15.000 |              2.30000 |
    2018-10-03 14:38:16.800 |             -0.30000 |
    Query OK, 2 row(s) in set (0.001162s)
    ```

- **SPREAD**

    ```mysql
    SELECT SPREAD(field_name) FROM { tb_name | stb_name } [WHERE clause];
    ```
    Function: Return the difference between the max value and the min value of a column in statistics /STable.
    
    Return Data Type: Double.
    
    Applicable Fields: All types except binary, nchar, bool.
    
    Note: Applicable for TIMESTAMP field, which indicates the time range of a record.
    
    Example:

    ```mysql
    taos> SELECT SPREAD(voltage) FROM meters;
        spread(voltage)      |
    ============================
                5.000000000 |
    Query OK, 1 row(s) in set (0.001792s)
  
    taos> SELECT SPREAD(voltage) FROM d1001;
        spread(voltage)      |
    ============================
                3.000000000 |
    Query OK, 1 row(s) in set (0.000836s)
    ```

- **Four Operations**

    ```mysql
    SELECT field_name [+|-|*|/|%][Value|field_name] FROM { tb_name | stb_name }  [WHERE clause];
    ```
    Function: Calculation results of addition, subtraction, multiplication, division and remainder of values in a column or among multiple columns in statistics/STable.
    
    Returned Data Type: Double.
    
    Applicable Fields: All types except timestamp, binary, nchar, bool.
    
    Note: 
    
    1. Calculation between two or more columns is supported, and the calculation priorities can be controlled by parentheses();
    2. The NULL field does not participate in the calculation. If a row involved in calculation contains NULL, the calculation result of the row is NULL.

## <a class="anchor" id="aggregation"></a> Time-dimension Aggregation

TDengine supports aggregating by intervals. Data in a table can partitioned by intervals and aggregated to generate results. For example, a temperature sensor collects data once per second, but the average temperature needs to be queried every 10 minutes. This aggregation is suitable for down sample operation, and the syntax is as follows:

```mysql
SELECT function_list FROM tb_name
  [WHERE where_condition]
  INTERVAL (interval [, offset])
  [SLIDING sliding]
  [FILL ({NONE | VALUE | PREV | NULL | LINEAR | NEXT})]

SELECT function_list FROM stb_name
  [WHERE where_condition]
  INTERVAL (interval [, offset])
  [SLIDING sliding]
  [FILL ({ VALUE | PREV | NULL | LINEAR | NEXT})]
  [GROUP BY tags]
```

- The length of aggregation interval is specified by keyword INTERVAL, the min time interval is 10 milliseconds (10a), and offset is supported (the offset must be less than interval). In aggregation queries, the aggregator and selector functions that can be executed simultaneously are limited to functions with one single output: count, avg, sum, stddev, leastsquares, percentile, min, max, first, last. Functions with multiple rows of output results (such as top, bottom, diff, and four operations) cannot be used.

- WHERE statement specifies the start and end time of a query and other filters

- FILL statement specifies a filling mode when data missed in a certain interval. Applicable filling modes include the following:
  
  1. Do not fill: NONE (default filingl mode).
  2. VALUE filling: Fixed value filling, where the filled value needs to be specified. For example: fill (VALUE, 1.23).
  3. NULL filling: Fill the data with NULL. For example: fill (NULL).
  4. PREV filling: Filling data with the previous non-NULL value. For example: fill (PREV).
  5. NEXT filling: Filling data with the next non-NULL value. For example: fill (NEXT).

Note:

  1. When using a FILL statement, a large number of filling outputs may be generated. Be sure to specify the time interval for the query. For each query, system can return no more than 10 million results with interpolation.
  2. In a time-dimension aggregation, the time-series in returned results increases strictly monotonously.
  3. If the query object is a STable, the aggregator function will act on the data of all tables under the STable that meet the value filters. If group by statement is not used in the query, the returned result increases strictly monotonously according to time-series; If group by statement is used to group in the query, each group in the returned result does not increase strictly monotonously according to time-series.

Example: The statement for building a database for smart meter is as follows:

```mysql
CREATE TABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
```

According to the data collected by the smart meter, the average value, maximum value, median current of current data in the past 24 hours are calculated in a phase of 10 minutes, and the current trend with time changes is fitted to a straight line. If there is no calculated value, fill it with the previous non-NULL value. The query statement used is as follows:

```mysql
SELECT AVG(current), MAX(current), LEASTSQUARES(current, start_val, step_val), PERCENTILE(current, 50) FROM meters
  WHERE ts>=NOW-1d
  INTERVAL(10m)
  FILL(PREV);
```

## <a class="anchor" id="limitation"></a> TAOS SQL Boundary Restrictions

- Max database name length is 32
- Max length of table name is 192, and max length of each data row is 16k characters
- Max length of column name is 64, max number of columns allowed is 1024, and min number of columns allowed is 2. The first column must be a timestamp
- Max number of tags allowed is 128, down to 1, and total length of tags does not exceed 16k characters
- Max length of SQL statement is 65480 characters, but it can be modified by system configuration parameter maxSQLLength, and max length can be configured to 1M
- Number of databases, STables and tables are not limited by system, but only limited by system resources

## Other TAOS SQL Conventions

**Restrictions on group by**

TAOS SQL supports group by operation on tags, tbnames and ordinary columns, required that only one column and whichhas less than 100,000 unique values.

**Restrictions on join operation**

TAOS SQL supports join columns of two tables by Primary Key timestamp between them, and does not support four operations after tables aggregated for the time being.

**Availability of is no null**

Is not null supports all types of columns. Non-null expression is < > "" and only applies to columns of non-numeric types.
