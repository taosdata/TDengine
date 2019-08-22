# STable: Super Table

"One Table for One Device" design can improve the insert/query performance significantly for a single device. But it has a side effect, the aggregation of multiple tables becomes hard. To reduce the complexity and improve the efficiency, TDengine introduced a new concept: STable (Super Table).  

## What is a Super Table

STable is an abstract and a template for a type of device. A STable contains a set of devices (tables) that have the same schema or data structure. Besides the shared schema, a STable has a set of tags, like the model, serial number and so on. Tags are used to record the static attributes for the devices and are used to group a set of devices (tables) for aggregation. Tags are metadata of a table and can be added, deleted or changed.   

TDengine does not save tags as a part of the data points collected. Instead, tags are saved as metadata. Each table has a set of tags. To improve query performance, tags are all cached and indexed. One table can only belong to one STable, but one STable may contain many tables. 

Like a table, you can create, show, delete and describe STables. Most query operations on tables can be applied to STable too, including the aggregation and selector functions. For queries on a STable, if no tags filter, the operations are applied to all the tables created via this STable. If there is a tag filter, the operations are applied only to a subset of the tables which satisfy the tag filter conditions. It will be very convenient to use tags to put devices into different groups for aggregation.

## Create a STable

Similiar to creating a standard table, syntax is: 

```mysql
CREATE TABLE <stable_name> (<field_name> TIMESTAMP, field_name1 field_type,…) TAGS(tag_name tag_type, …)
```

New keyword "tags" is introduced, where tag_name is the tag name, and tag_type is the associated data type. 

Note：

1. The bytes of all tags together shall be less than 512 
2. Tag's data type can not be time stamp or nchar
3. Tag name shall be different from the field name
4. Tag name shall not be the same as system keywords
5. Maximum number of tags is 6 

For example:

```mysql
create table thermometer (ts timestamp, degree float) 
tags (location binary(20), type int)
```

The above statement creates a STable thermometer with two tag "location" and "type"

## Create a Table via STable

To create a table for a device, you can use a STable as its template and assign the tag values. The syntax is:

```mysql
CREATE TABLE <tb_name> USING <stb_name> TAGS (tag_value1,...)
```

You can create any number of tables via a STable, and each table may have different tag values. For example, you create five tables via STable thermometer below:

```mysql
 create table t1 using thermometer tags (‘beijing’, 10);
 create table t2 using thermometer tags (‘beijing’, 20);
 create table t3 using thermometer tags (‘shanghai’, 10);
 create table t4 using thermometer tags (‘shanghai’, 20);
 create table t5 using thermometer tags (‘new york’, 10);
```

## Aggregate Tables via STable

You can group a set of tables together by specifying the tags filter condition, then apply the aggregation operations. The result set can be grouped and ordered based on tag value. Syntax is：

```mysql
SELECT function<field_name>,… 
 FROM <stable_name> 
 WHERE <tag_name> <[=|<=|>=|<>] values..> ([AND|OR] …)
 INTERVAL (<time range>)
 GROUP BY <tag_name>, <tag_name>…
 ORDER BY <tag_name> <asc|desc>
 SLIMIT <group_limit>
 SOFFSET <group_offset>
 LIMIT <record_limit>
 OFFSET <record_offset>
```

For the time being, STable supports only the following aggregation/selection functions: *sum, count, avg, first, last, min, max, top, bottom*, and the projection operations, the same syntax as a standard table.  Arithmetic operations are not supported, embedded queries not either. 

*INTERVAL* is used for the aggregation over a time range.

If *GROUP BY* is not used, the aggregation is applied to all the selected tables, and the result set is output in ascending order of the timestamp, but you can use "*ORDER BY _c0 ASC|DESC*" to specify the order you like. 

If *GROUP BY <tag_name>* is used, the aggregation is applied to groups based on tags. Each group is aggregated independently. Result set is a group of aggregation results. The group order is decided by *ORDER BY <tag_name>*. Inside each group, the result set is in the ascending order of the time stamp. 

*SLIMIT/SOFFSET* are used to limit the number of groups and starting group number.

*LIMIT/OFFSET* are used to limit the number of records in a group and the starting rows.

###Example 1:

Check the average, maximum, and minimum temperatures of Beijing and Shanghai, and group the result set by location and type. The SQL statement shall be:

```mysql
SELECT COUNT(*), AVG(degree), MAX(degree), MIN(degree)
FROM thermometer
WHERE location=’beijing’ or location=’tianjing’
GROUP BY location, type 
```

### Example 2:

List the number of records, average, maximum, and minimum temperature every 10 minutes for the past 24 hours for all the thermometers located in Beijing with type 10. The SQL statement shall be:

```mysql
SELECT COUNT(*), AVG(degree), MAX(degree), MIN(degree)
FROM thermometer
WHERE name=’beijing’ and type=10 and ts>=now-1d
INTERVAL(10M)
```

## Create Table Automatically

Insert operation will fail if the table is not created yet. But for STable, TDengine can create the table automatically if the application provides the STable name, table name and tags' value when inserting data points. The syntax is:

```mysql
INSERT INTO <tb_name> USING <stb_name> TAGS (<tag1_value>, ...) VALUES (field_value, ...) (field_value, ...) ... <tb_name2> USING <stb_name2> TAGS(<tag1_value2>, ...) VALUES (<field1_value1>, ...) ...;
```

When inserting data points into table tb_name, the system will check if table tb_name is created or not. If it is already created, the data points will be inserted as usual. But if the table is not created yet, the system will create the table tb_bame using STable stb_name as the template with the tags. Multiple tables can be specified in the SQL statement. 

## Management of STables

After you can create a STable, you can describe, delete, change STables. This section lists all the supported operations.

### Show STables in current DB

```mysql
show stables;
```

It lists all STables in current DB, including the name, created time, number of fileds, number of tags, and number of tables which are created via this STable. 

### Describe a STable

```mysql
DESCRIBE <stable_name>
```

It lists the STable's schema and tags

### Drop a STable

```mysql
DROP TABLE <stable_name>
```

To delete a STable, all the tables created via this STable shall be deleted first, otherwise, it will fail.

### List the Associated Tables of a STable

```mysql
SELECT TBNAME,[TAG_NAME,…] FROM <stable_name> WHERE <tag_name> <[=|=<|>=|<>] values..> ([AND|OR] …)
```

It will list all the tables which satisfy the tag filter conditions. The tables are all created from this specific STable. TBNAME is a new keyword introduced, it is the table name associated with the STable. 

```mysql
SELECT COUNT(TBNAME) FROM <stable_name> WHERE <tag_name> <[=|=<|>=|<>] values..> ([AND|OR] …)
```

The above SQL statement will list the number of tables in a STable, which satisfy the filter condition.

## Management of Tags

You can add, delete and change the tags for a STable, and you can change the tag value of a table. The SQL commands are listed below.  

### Add a Tag

```mysql
ALTER TABLE <stable_name> ADD TAG <new_tag_name> <TYPE>
```

It adds a new tag to the STable with a data type. The maximum number of tags is 6. 

### Drop a Tag

```mysql
ALTER TABLE <stable_name> DROP TAG <tag_name>
```

It drops a tag from a STable. The first tag could not be deleted, and there must be at least one tag.

### Change a Tag's Name

```mysql
ALTER TABLE <stable_name> CHANGE TAG <old_tag_name> <new_tag_name>
```

It changes the name of a tag from old to new. 

### Change the Tag's Value

```mysql
ALTER TABLE <table_name> SET TAG <tag_name>=<new_tag_value>
```

It changes a table's tag value to a new one. 
