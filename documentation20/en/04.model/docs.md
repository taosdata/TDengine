# Data Modeling

TDengine adopts a relational data model, so we need to build the "database" and "table". Therefore, for a specific application scenario, it is necessary to consider the design of the database, STable and ordinary table. This section does not discuss detailed syntax rules, but only concepts.

Please watch the [video tutorial](https://www.taosdata.com/blog/2020/11/11/1945.html) for data modeling.

## <a class="anchor" id="create-db"></a> Create a Database

Different types of data collection points often have different data characteristics, including frequency of data collecting, length of data retention time, number of replicas, size of data blocks, whether to update data or not, and so on. To ensure TDengine working with great efficiency in various scenarios, TDengine suggests creating tables with different data characteristics in different databases, because each database can be configured with different storage strategies. When creating a database, in addition to SQL standard options, the application can also specify a variety of parameters such as retention duration, number of replicas, number of memory blocks, time accuracy, max and min number of records in a file block, whether it is compressed or not, and number of days a data file will be overwritten. For example:

```mysql
CREATE DATABASE power KEEP 365 DAYS 10 BLOCKS 4 UPDATE 1;
```

The above statement will create a database named “power”. The data of this database will be kept for 365 days (it will be automatically deleted 365 days later), one data file created per 10 days, and the number of memory blocks is 4 for data updating. For detailed syntax and parameters, please refer to [Data Management section of TAOS SQL](https://www.taosdata.com/en/documentation/taos-sql#management).

After the database created, please use SQL command USE to switch to the new database, for example:

```mysql
USE power;	
```

Replace the database operating in the current connection with “power”, otherwise, before operating on a specific table, you need to use "database name. table name" to specify the name of database to use.

**Note:**

- Any table or STable belongs to a database. Before creating a table, a database must be created first.
- Tables in two different databases cannot be JOIN.

## <a class="anchor" id="create-stable"></a> Create a STable

An IoT system often has many types of devices, such as smart meters, transformers, buses, switches, etc. for power grids. In order to facilitate aggregation among multiple tables, using TDengine, it is necessary to create a STable for each type of data collection point. Taking the smart meter in Table 1 as an example, you can use the following SQL command to create a STable:

```mysql
CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupdId int);
```

**Note:** The STABLE keyword in this instruction needs to be written as TABLE in versions before 2.0.15.

Just like creating an ordinary table, you need to provide the table name (‘meters’ in the example) and the table structure Schema, that is, the definition of data columns. The first column must be a timestamp (‘ts’ in the example), the other columns are the physical metrics collected (current, volume, phase in the example), and the data types can be int, float, string, etc. In addition, you need to provide the schema of the tag (location, groupId in the example), and the data types of the tag can be int, float, string and so on. Static attributes of collection points can often be used as tags, such as geographic location of collection points, device model, device group ID, administrator ID, etc. The schema of the tag can be added, deleted and modified afterwards. Please refer to the [STable Management section of TAOS SQL](https://www.taosdata.com/cn/documentation/taos-sql#super-table) for specific definitions and details.

Each type of data collection point needs an established STable, so an IoT system often has multiple STables. For the power grid, we need to build a STable for smart meters, transformers, buses, switches, etc. For IoT, a device may have multiple data collection points (for example, a fan for wind-driven generator, some collection points capture parameters such as current and voltage, and some capture environmental parameters such as temperature, humidity and wind direction). In this case, multiple STables need to be established for corresponding types of devices. All collected physical metrics contained in one and the same STable must be collected at the same time (with a consistent timestamp).

A STable allows up to 1024 columns. If the number of physical metrics collected at a collection point exceeds 1024, multiple STables need to be built to process them. A system can have multiple DBs, and a DB can have one or more STables.

## <a class="anchor" id="create-table"></a> Create a Table

TDengine builds a table independently for each data collection point. Similar to standard relational data, one table has a table name, Schema, but in addition, it can also carry one or more tags. When creating, you need to use the STable as a template and specify the specific value of the tag. Taking the smart meter in Table 1 as an example, the following SQL command can be used to build the table:

```mysql
CREATE TABLE d1001 USING meters TAGS ("Beijing.Chaoyang", 2);
```

Where d1001 is the table name, meters is the name of the STable, followed by the specific tag value of tag Location as "Beijing.Chaoyang", and the specific tag value of tag groupId 2. Although the tag value needs to be specified when creating the table, it can be modified afterwards. Please refer to the [Table Management section of TAOS SQL](https://www.taosdata.com/en/documentation/taos-sql#table) for details.

**Note: ** At present, TDengine does not technically restrict the use of a STable of a database (dbA) as a template to create a sub-table of another database (dbB). This usage will be prohibited later, and it is not recommended to use this method to create a table.

TDengine suggests to use the globally unique ID of data collection point as a table name (such as device serial number). However, in some scenarios, there is no unique ID, and multiple IDs can be combined into a unique ID. It is not recommended to use a unique ID as tag value.

**Automatic table creating** : In some special scenarios, user is not sure whether the table of a certain data collection point exists when writing data. In this case, the non-existent table can be created by using automatic table building syntax when writing data. If the table already exists, no new table will be created. For example:

```mysql
INSERT INTO d1001 USING METERS TAGS ("Beijng.Chaoyang", 2) VALUES (now, 10.2, 219, 0.32);
```

The SQL statement above inserts records (now, 10.2, 219, 0.32) into table d1001. If table d1001 has not been created yet, the STable meters is used as the template to automatically create it, and the tag value "Beijing.Chaoyang", 2 is marked at the same time.

For detailed syntax of automatic table building, please refer to the "[Automatic Table Creation When Inserting Records](https://www.taosdata.com/en/documentation/taos-sql#auto_create_table)" section.

## Multi-column Model vs Single-column Model

TDengine supports multi-column model. As long as physical metrics are collected simultaneously by a data collection point (with a consistent timestamp), these metrics can be placed in a STable as different columns. However, there is also an extreme design, a single-column model, in which each collected physical metric is set up separately, so each type of physical metrics is set up separately with a STable. For example, create 3 Stables, one each for current, voltage and phase.

TDengine recommends using multi-column model as much as possible because of higher insertion and storage efficiency. However, for some scenarios, types of collected metrics often change. In this case, if multi-column model is adopted, the structure definition of STable needs to be frequently modified so make the application complicated. To avoid that, single-column model is recommended.
