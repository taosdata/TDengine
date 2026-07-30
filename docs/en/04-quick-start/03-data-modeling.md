---
sidebar_label: Data Modeling
title: Data Modeling
description: Use SQL to create databases, supertables, subtables, basic tables, and virtual tables
toc_max_heading_level: 4
---

This chapter builds on the smart meter example in [Basic Concepts](./02-basic-concepts.md) and shows how to use SQL in TDengine to create databases, supertables, subtables, basic tables, and virtual tables.

## Data Modeling

This section uses smart meters as an example to briefly introduce how to use SQL to create databases, supertables, and basic table operations in TDengine.

### Creating a Database

The SQL to create a database for storing meter data is as follows:

```sql
CREATE DATABASE power PRECISION 'ms' KEEP 3650 DURATION 10 BUFFER 16;
```

This SQL will create a database named `power`, with the following parameters explained:

- `PRECISION 'ms'`: This database uses millisecond (ms) precision timestamps for its time-series data
- `KEEP 3650`: The data in this database will be retained for 3650 days, and data older than 3650 days will be automatically deleted
- `DURATION 10`: Data for every 10 days is stored in one data file
- `BUFFER 16`: Writing uses a memory pool of size 16MB.

After creating the power database, you can execute the USE statement to switch databases.

```sql
use power;
```

This SQL switches the current database to `power`, indicating that subsequent insertions, queries, and other operations will be performed in the current `power` database.

### Creating a Supertable

The SQL to create a supertable named `meters` is as follows:

```sql
CREATE STABLE meters (
    ts timestamp, 
    current float, 
    voltage int, 
    phase float
) TAGS (
    location varchar(64), 
    group_id int
);
```

In TDengine, the SQL statement to create a supertable is similar to that in relational databases. For example, in the SQL above, `CREATE STABLE` is the keyword, indicating the creation of a supertable; then, `meters` is the name of the supertable; in the parentheses following the table name, the columns of the supertable are defined (column names, data types, etc.), with the following rules:

1. The first column must be a timestamp column. For example: `ts timestamp` indicates that the timestamp column name is `ts`, and its data type is `timestamp`;
2. Starting from the second column are the measurement columns. The data types of measurements can be integer, float, string, etc. For example: `current float` indicates that the measurement current `current`, data type is `float`;

Finally, TAGS is a keyword, indicating tags, and in the parentheses following TAGS, the tags of the supertable are defined (tag names, data types, etc.).

1. The data type of tags can be integer, float, string, etc. For example: `location varchar(64)` indicates that the tag region `location`, data type is `varchar(64)`;
2. The names of tags cannot be the same as the names of measurement columns.

### Creating a Table

The SQL to create a subtable `d1001` using the supertable is as follows:

```sql
CREATE TABLE d1001 
USING meters (
    location,
    group_id
) TAGS (
    "California.SanFrancisco", 
    2
);
```

In the SQL above, `CREATE TABLE` is a keyword indicating the creation of a table; `d1001` is the name of the subtable; `USING` is a keyword indicating the use of a supertable as a template; `meters` is the name of the supertable; in the parentheses following the supertable name, `location`, `group_id` are the names of the tag columns of the supertable; `TAGS` is a keyword, and the values of the tag columns for the subtable are specified in the following parentheses. `"California.SanFrancisco"` and `2` indicate that the location of subtable `d1001` is `California.SanFrancisco`, and the group ID is `2`.

When performing write or query operations on a supertable, users can use the pseudocolumn `tbname` to specify or output the name of the corresponding subtable.

### Automatic Table Creation

In TDengine, to simplify user operations and ensure smooth data entry, even if a subtable does not exist, users can use the automatic table creation SQL with the `using` keyword to write data. This mechanism allows the system to automatically create the subtable when it encounters a non-existent subtable, and then perform the data writing operation. If the subtable already exists, the system will write the data directly without any additional steps.

The SQL for writing data while automatically creating tables is as follows:

```sql
INSERT INTO d1002 
USING meters 
TAGS (
    "California.SanFrancisco", 
    2
) VALUES (
    NOW, 
    10.2, 
    219, 
    0.32
);
```

In the SQL above, `INSERT INTO d1002` indicates writing data into the subtable `d1002`; `USING meters` indicates using the supertable `meters` as a template; `TAGS ("California.SanFrancisco",  2)` indicates the tag values for subtable `d1002` are `California.SanFrancisco` and `2`; `VALUES (NOW, 10.2, 219, 0.32)` indicates inserting a record into subtable `d1002` with values NOW (current timestamp), 10.2 (current), 219 (voltage), 0.32 (phase). When TDengine executes this SQL, if subtable `d1002` already exists, it writes the data directly; if subtable `d1002` does not exist, it first automatically creates the subtable, then writes the data.

### Creating Basic Tables

In TDengine, apart from subtables with tags, there are also basic tables without any tags. These tables are similar to tables in traditional relational databases, and users can create them using SQL.

The differences between basic tables and subtables are:

1. Tag Extensibility: Subtables add static tags on top of basic tables, allowing them to carry more metadata. Additionally, the tags of subtables are mutable, and users can add, delete, or modify tags as needed.
2. Table Ownership: Subtables always belong to a supertable and are part of it. Basic tables, however, exist independently and do not belong to any supertable.
3. Conversion Restrictions: In TDengine, basic tables cannot be directly converted into subtables, and likewise, subtables cannot be converted into basic tables. These two types of tables determine their structure and properties at creation and cannot be changed later.

In summary, basic tables provide functionality similar to traditional relational database tables, while subtables introduce a tagging mechanism, offering richer descriptions and more flexible management for time-series data. Users can choose to create basic tables or subtables based on actual needs.

The SQL for creating an basic table without any tags is as follows:

```sql
CREATE TABLE d1003(
    ts timestamp,
    current float, 
    voltage int, 
    phase float,
    location varchar(64), 
    group_id int
);
```

The SQL above indicates the creation of the basic table `d1003`, with a structure including columns `ts`, `current`, `voltage`, `phase`, `location`, `group_id`, totaling 6 columns. This data model is completely consistent with relational databases.

Using basic tables as the data model means that static tag data (such as location and group_id) will be repeatedly stored in each row of the table. This approach not only increases storage space consumption but also significantly lowers query performance compared to using a supertable data model, as it cannot directly utilize tag data for filtering.

### Multi-Column Model vs. Single-Column Model

TDengine supports flexible data model designs, including multi-column and single-column models. The multi-column model allows multiple physical quantities collected simultaneously from the same data collection point with the same timestamp to be stored in different columns of the same supertable. However, in some extreme cases, a single-column model might be used, where each collected physical quantity is established in a separate table. For example, for the three physical quantities of current, voltage, and phase, three separate supertables might be established.

Although TDengine recommends using the multi-column model because it generally offers better writing and storage efficiency, the single-column model might be more suitable in certain specific scenarios. For example, if the types of quantities collected at a data collection point frequently change, using a multi-column model would require frequent modifications to the supertable's structural definition, increasing the complexity of the application. In such cases, using a single-column model can simplify the design and management of the application, as it allows independent management and expansion of each physical quantity's supertable.

Overall, TDengine offers flexible data model options, allowing users to choose the most suitable model based on actual needs and scenarios to optimize performance and manage complexity.

### Creating Virtual Tables

Whether using single-column or multi-column models, TDengine enables cross-table operations through virtual tables. Using smart meters as an example, here we introduce two typical use cases for virtual tables:

1. Single-Source Multi-Dimensional Time-Series Aggregation
2. Cross-Source Metric Comparative Analysis

---

#### 1. Single-Source Multi-Dimensional Time-Series Aggregation

In this scenario, "single-source" refers to multiple **single-column time-series tables** from the **same data collection point**. While these tables are physically split due to business requirements or constraints, they maintain logical consistency through device tags and timestamps. Virtual tables restore "vertically" split data into a complete "horizontal" view of the collection point.
For example, Suppose three supertables are created for current, voltage, and phase measurements using a single-column model. Virtual tables can aggregate these three measurements into one unified view.

The SQL statement for creating a supertable in the single-column model is as follows:

```sql

CREATE STABLE current_stb (
    ts timestamp, 
    current float
) TAGS (
    device_id varchar(64),
    location varchar(64), 
    group_id int
);

CREATE STABLE voltage_stb (
    ts timestamp, 
    voltage int
) TAGS (
    device_id varchar(64),
    location varchar(64), 
    group_id int
);
 
CREATE STABLE phase_stb (
    ts timestamp, 
    phase float
) TAGS (
    device_id varchar(64),
    location varchar(64), 
    group_id int
);
```

Assume there are four devices: d1001, d1002, d1003, and d1004. To create subtables for their current, voltage, and phase measurements, use the following SQL statements:

```sql
create table current_d1001 using current_stb(device_id, location, group_id) tags("d1001", "California.SanFrancisco", 2);
create table current_d1002 using current_stb(device_id, location, group_id) tags("d1002", "California.SanFrancisco", 3);
create table current_d1003 using current_stb(device_id, location, group_id) tags("d1003", "California.LosAngeles", 3);
create table current_d1004 using current_stb(device_id, location, group_id) tags("d1004", "California.LosAngeles", 2);

create table voltage_d1001 using voltage_stb(device_id, location, group_id) tags("d1001", "California.SanFrancisco", 2);
create table voltage_d1002 using voltage_stb(device_id, location, group_id) tags("d1002", "California.SanFrancisco", 3);
create table voltage_d1003 using voltage_stb(device_id, location, group_id) tags("d1003", "California.LosAngeles", 3);
create table voltage_d1004 using voltage_stb(device_id, location, group_id) tags("d1004", "California.LosAngeles", 2);

create table phase_d1001 using phase_stb(device_id, location, group_id) tags("d1001", "California.SanFrancisco", 2);
create table phase_d1002 using phase_stb(device_id, location, group_id) tags("d1002", "California.SanFrancisco", 3);
create table phase_d1003 using phase_stb(device_id, location, group_id) tags("d1003", "California.LosAngeles", 3);
create table phase_d1004 using phase_stb(device_id, location, group_id) tags("d1004", "California.LosAngeles", 2);
```

A virtual supertable can be used to aggregate these three types of measurements into a single table. The SQL statement to create the virtual supertable is as follows:

```sql
CREATE STABLE meters_v (
    ts timestamp, 
    current float, 
    voltage int, 
    phase float
) TAGS (
    location varchar(64), 
    group_id int
) VIRTUAL 1;
```

For the four devices d1001, d1002, d1003, and d1004, create virtual subtables with the following SQL statements:

```sql
CREATE VTABLE d1001_v (
    current from current_d1001.current,
    voltage from voltage_d1001.voltage, 
    phase from phase_d1001.phase
) 
USING meters_v 
TAGS (
    "California.SanFrancisco", 
    2
);
       
CREATE VTABLE d1002_v (
    current from current_d1002.current,
    voltage from voltage_d1002.voltage, 
    phase from phase_d1002.phase
) 
USING meters_v 
TAGS (
    "California.SanFrancisco", 
    3
);
       
CREATE VTABLE d1003_v (
    current from current_d1003.current,
    voltage from voltage_d1003.voltage, 
    phase from phase_d1003.phase
) 
USING meters_v 
TAGS (
    "California.LosAngeles", 
    3
);
       
CREATE VTABLE d1004_v (
    current from current_d1004.current,
    voltage from voltage_d1004.voltage, 
    phase from phase_d1004.phase
) 
USING meters_v 
TAGS (
    "California.LosAngeles", 
    2
);
```

Taking device d1001 as an example, assume that the current, voltage, and phase data of device d1001 are as follows:

<table>
    <tr>
        <th colspan="2" align="center">current_d1001</th>
        <th rowspan="7" align="center"></th>  
        <th colspan="2" align="center">voltage_d1001</th>
        <th rowspan="7" align="center"></th>  
        <th colspan="2" align="center">phase_d1001</th>
    </tr>
    <tr>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
        <td align="center">Timestamp</td>
        <td align="center">Voltage</td>
        <td align="center">Timestamp</td>
        <td align="center">Phase</td>
    </tr>
    <tr>
        <td align="center">1538548685000</td>
        <td align="center">10.3</td>
        <td align="center">1538548685000</td>
        <td align="center">219</td>
        <td align="center">1538548685000</td>
        <td align="center">0.31</td>
    </tr>
    <tr>
        <td align="center">1538548695000</td>
        <td align="center">12.6</td>
        <td align="center">1538548695000</td>
        <td align="center">218</td>
        <td align="center">1538548695000</td>
        <td align="center">0.33</td>
    </tr>
    <tr>
        <td align="center">1538548696800</td>
        <td align="center">12.3</td>
        <td align="center">1538548696800</td>
        <td align="center">221</td>
        <td align="center">1538548696800</td>
        <td align="center">0.31</td>
    </tr>
    <tr>
        <td align="center">1538548697100</td>
        <td align="center">12.1</td>
        <td align="center">1538548697100</td>
        <td align="center">220</td>
        <td align="center">1538548697200</td>
        <td align="center">0.32</td>
    </tr>
    <tr>
        <td align="center">1538548697700</td>
        <td align="center">11.8</td>
        <td align="center">1538548697800</td>
        <td align="center">222</td>
        <td align="center">1538548697800</td>
        <td align="center">0.33</td>
    </tr>
</table>

| Timestamp         | Current | Voltage | Phase |
|-------------------|---------|---------|-------|
| 1538548685000     | 10.3    | 219     | 0.31  |
| 1538548695000     | 12.6    | 218     | 0.33  |
| 1538548696800     | 12.3    | 221     | 0.31  |
| 1538548697100     | 12.1    | 220     | NULL  |
| 1538548697200     | NULL    | NULL    | 0.32  |
| 1538548697700     | 11.8    | NULL    | NULL  |
| 1538548697800     | NULL    | 222     | 0.33  |

---

#### 2. Cross-Source Metric Comparative Analysis

In this scenario, "cross-source" refers to data from **different data collection points**. Virtual tables align and merge semantically comparable measurements from multiple devices for comparative analysis.
For example, Compare current measurements across devices `d1001`, `d1002`, `d1003`, and `d1004`. The SQL statement to create the virtual table is as follows:

```sql
CREATE VTABLE current_v (
    ts TIMESTAMP,
    d1001_current FLOAT FROM current_d1001.current,
    d1002_current FLOAT FROM current_d1002.current, 
    d1003_current FLOAT FROM current_d1003.current,
    d1004_current FLOAT FROM current_d1004.current
);
```

Assume that the current data of devices d1001, d1002, d1003, and d1004 are as follows:

<table>
    <tr>
        <th colspan="2" align="center">d1001</th>
        <th rowspan="7" align="center"></th>  
        <th colspan="2" align="center">d1002</th>
        <th rowspan="7" align="center"></th>  
        <th colspan="2" align="center">d1003</th>
        <th rowspan="7" align="center"></th>  
        <th colspan="2" align="center">d1004</th>
    </tr>
    <tr>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
        <td align="center">Timestamp</td>
        <td align="center">Current</td>
    </tr>
    <tr>
        <td align="center">1538548685000</td>
        <td align="center">10.3</td>
        <td align="center">1538548685000</td>
        <td align="center">11.7</td>
        <td align="center">1538548685000</td>
        <td align="center">11.2</td>
        <td align="center">1538548685000</td>
        <td align="center">12.4</td>
    </tr>
    <tr>
        <td align="center">1538548695000</td>
        <td align="center">12.6</td>
        <td align="center">1538548695000</td>
        <td align="center">11.9</td>
        <td align="center">1538548695000</td>
        <td align="center">10.8</td>
        <td align="center">1538548695000</td>
        <td align="center">11.3</td>
    </tr>
    <tr>
        <td align="center">1538548696800</td>
        <td align="center">12.3</td>
        <td align="center">1538548696800</td>
        <td align="center">12.4</td>
        <td align="center">1538548696800</td>
        <td align="center">12.3</td>
        <td align="center">1538548696800</td>
        <td align="center">10.1</td>
    </tr>
    <tr>
        <td align="center">1538548697100</td>
        <td align="center">12.1</td>
        <td align="center">1538548697200</td>
        <td align="center">12.2</td>
        <td align="center">1538548697100</td>
        <td align="center">11.1</td>
        <td align="center">1538548697200</td>
        <td align="center">11.7</td>
    </tr>
    <tr>
        <td align="center">1538548697700</td>
        <td align="center">11.8</td>
        <td align="center">1538548697700</td>
        <td align="center">11.4</td>
        <td align="center">1538548697800</td>
        <td align="center">12.1</td>
        <td align="center">1538548697800</td>
        <td align="center">12.6</td>
    </tr>
</table>

The virtual table `current_v` aligns current data by timestamp:

| Timestamp         | d1001_current | d1002_current | d1003_current | d1004_current |
|-------------------|---------------|---------------|---------------|---------------|
| 1538548685000     | 10.3          | 11.7          | 11.2          | 12.4          |
| 1538548695000     | 12.6          | 11.9          | 10.8          | 11.3          |
| 1538548696800     | 12.3          | 12.4          | 12.3          | 10.1          |
| 1538548697100     | 12.1          | NULL          | 11.1          | NULL          |
| 1538548697200     | NULL          | 12.2          | NULL          | 11.7          |
| 1538548697700     | 11.8          | 11.4          | NULL          | NULL          |
| 1538548697800     | NULL          | NULL          | 12.1          | 12.6          |
