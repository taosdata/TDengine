---
sidebar_label: Virtual Tables
title: Virtual Tables
description: Various management operations for virtual tables
---

## Create Virtual Table

The `CREATE VTABLE` statement is used to create virtual basic tables and virtual subtables using virtual supertables as templates.

### Create Virtual Supertables

Refer to the `VIRTUAL` parameter in [Create Supertable](./04-stable.md#create-supertable).

### Create Virtual Basic Table

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name].vtb_name 
    ts_col_name timestamp, 
    (create_definition[ ,create_definition] ...) 
     
  create_definition:
    vtb_col_name column_definition
    
  column_definition:
    type_name [FROM [db_name.]table_name.col_name]
```

### Create Virtual Subtable

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name].vtb_name 
    (create_definition[ ,create_definition] ...) 
    USING [db_name.]stb_name 
    [(tag_name [, tag_name] ...)] 
    TAGS (tag_value [, tag_value] ...)
     
  create_definition:
    [stb_col_name FROM] [db_name.]table_name.col_name
  tag_value:
     const_value
```

Usage Notes:

1. Naming rules for virtual tables/columns follow [Name Rules](./19-limit.md#naming-rules).
2. Maximum table name length: 192 characters.
3. The first column must be TIMESTAMP and is automatically set as primary key.
4. Row length cannot exceed 64KB (Note: VARCHAR/NCHAR/GEOMETRY columns consume 2 extra bytes each).
5. Specify maximum length for VARCHAR/NCHAR/GEOMETRY types (e.g., VARCHAR(20)).
6. Use `FROM` to specify column data sources. Cross-database sources are supported via `db_name`.
7. The timestamp column (ts) values of virtual table are merged results from all involved tables' timestamp primary keys during queries.
8. Virtual supertables only support creating virtual subtables, virtual subtables can only use virtual supertables as template.
9. Ensure virtual tables' column/tag data types match their source columns/tags.
10. Virtual table names must be unique within a database and cannot conflict with table names, and it is recommended that view names do not duplicate virtual table names (not enforced). When a view and a virtual table have the same name, operations such as writing, querying, granting, and revoking permissions prioritize the virtual table with the same name. .
11. When creating virtual subtables/basic tables, `FROM` columns must originate from basic tables/subtables (not supertables, views, or other virtual tables).

## Query Virtual Tables

Virtual tables use the same query syntax as regular tables, but their dataset may vary between queries based on data alignment rules.

### Data Alignment Rules

1. Align data from multiple source tables by timestamp.
2. Combine columns with same timestamp into one row; missing values fill with NULL.
3. Virtual table timestamps are the union of all involved columns' origin tables' timestamps. Therefore, the number of rows in the result set may vary when different queries select different columns.
4. Users can combine any columns from multiple tables; unselected columns are excluded.

Example:

Given tables t1, t2, t3 with data:

<table>
    <tr>
        <th colspan="2" align="center">t1</th>
        <th rowspan="7" align="center"></th>  
        <th colspan="2" align="center">t2</th>
        <th rowspan="7" align="center"></th>  
        <th colspan="3" align="center">t3</th>
    </tr>
    <tr>
        <td align="center">ts</td>
        <td align="center">value</td>
        <td align="center">ts</td>
        <td align="center">value</td>
        <td align="center">ts</td>
        <td align="center">value1</td>
        <td align="center">value2</td>
    </tr>
    <tr>
        <td align="center">0:00:01</td>
        <td align="center">1</td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center">0:00:02</td>
        <td align="center">20</td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center">0:00:03</td>
        <td align="center">300</td>
        <td align="center">3000</td>
    </tr>
    <tr>
        <td align="center">0:00:04</td>
        <td align="center">4</td>
        <td align="center">0:00:04</td>
        <td align="center">40</td>
        <td align="center">0:00:03</td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center">0:00:05</td>
        <td align="center">50</td>
        <td align="center">0:00:05</td>
        <td align="center">500</td>
        <td align="center">5000</td>
    </tr>
</table>

Create a virtual table v1:

```sql
CREATE VTABLE v1 (
    ts timestamp,
    c1 int FROM t1.value,
    c2 int FROM t2.value,
    c3 int FROM t3.value1,
    c4 int FROM t3.value2);
```

Querying all columns:

```sql
SELECT * FROM v1;
```

Result:

<table>
    <tr>
        <th colspan="5" align="center">v1</th>
    </tr>
    <tr>
        <td align="center">ts</td>
        <td align="center">c1</td>
        <td align="center">c2</td>
        <td align="center">c3</td>
        <td align="center">c4</td>
    </tr>
    <tr>
        <td align="center">0:00:01</td>
        <td align="center">1</td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center">0:00:02</td>
        <td align="center"></td>
        <td align="center">20</td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center">0:00:03</td>
        <td align="center"></td>
        <td align="center"></td>
        <td align="center">300</td>
        <td align="center">3000</td>
    </tr>
    <tr>
        <td align="center">0:00:04</td>
        <td align="center">4</td>
        <td align="center">40</td>
        <td align="center"></td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center">0:00:05</td>
        <td align="center"></td>
        <td align="center">50</td>
        <td align="center">500</td>
        <td align="center">5000</td>
    </tr>
</table>

Partial column query:

```sql
SELECT c1, c2 FROM v1;
```

Result:

<table>
    <tr>
        <th colspan="5" align="center">v1</th>
    </tr>
    <tr>
        <td align="center">ts</td>
        <td align="center">c1</td>
        <td align="center">c2</td>
    </tr>
    <tr>
        <td align="center">0:00:01</td>
        <td align="center">1</td>
        <td align="center"></td>
    </tr>
    <tr>
        <td align="center">0:00:02</td>
        <td align="center"></td>
        <td align="center">20</td>
    </tr>
    <tr>
        <td align="center">0:00:04</td>
        <td align="center">4</td>
        <td align="center">40</td>
    </tr>
    <tr>
        <td align="center">0:00:05</td>
        <td align="center"></td>
        <td align="center">50</td>
    </tr>
</table>

Since the original tables t1 and t2 (corresponding to columns c1 and c2) lack the timestamp 0:00:03, this timestamp will not appear in the final result.

## Modify Virtual Basic Tables

```sql
ALTER VTABLE [db_name.]vtb_name alter_table_clause

alter_table_clause: {
  ADD COLUMN vtb_col_name vtb_column_type [FROM table_name.col_name]
  | DROP COLUMN vtb_col_name
  | ALTER COLUMN vtb_col_name SET {table_name.col_name | NULL }
  | MODIFY COLUMN col_name column_type
  | RENAME COLUMN old_col_name new_col_name
}
```

### Add Column

```sql
ALTER VTABLE vtb_name ADD COLUMN vtb_col_name vtb_col_type [FROM [db_name].table_name.col_name]
```

### Drop Column

```sql
ALTER VTABLE vtb_name DROP COLUMN vtb_col_name
```

### Modify Column Width

```sql
ALTER VTABLE vtb_name MODIFY COLUMN vtb_col_name data_type(length);
```

### Rename Column

```sql
ALTER VTABLE vtb_name RENAME COLUMN old_col_name new_col_name
```

### Change Column Source

```sql
ALTER VTABLE vtb_name ALTER COLUMN vtb_col_name SET {[db_name.]table_name.col_name | NULL}
```

## Modify Virtual Subtables

```sql
ALTER VTABLE [db_name.]vtb_name alter_table_clause

alter_table_clause: {
  ALTER COLUMN vtb_col_name SET table_name.col_name
  | SET TAG tag_name = new_tag_value
}
```

### Modify Subtable Tag Value

```sql
ALTER VTABLE tb_name SET TAG tag_name1=new_tag_value1, tag_name2=new_tag_value2 ...;
```

### Change Column Source

```sql
ALTER VTABLE vtb_name ALTER COLUMN vtb_col_name SET {[db_name.]table_name.col_name | NULL}
```

## Drop Virtual Tables

```sql
DROP VTABLE [IF EXISTS] [dbname].vtb_name;
```

## View Virtual Table Information

### List Virtual Tables

```sql
SHOW [NORMAL | CHILD] [db_name.]VTABLES [LIKE 'pattern'];
```

### Show Creation Statement

```sql
SHOW CREATE VTABLE [db_name.]vtable_name;
```

### Describe Structure

```sql
DESCRIBE [db_name.]vtb_name;
```

### Query All Virtual Tables' Information

```sql
SELECT ... FROM information_schema.ins_tables WHERE type = 'VIRTUAL_NORMAL_TABLE' OR type = 'VIRTUAL_CHILD_TABLE';
```

## Write to Virtual Tables

Writing or deleting data in virtual tables is **not supported**. Virtual tables are logical views computed from source tables.

## Virtual Tables vs. Views

| Property              | Virtual Table                     | View                          |
|-----------------------|-----------------------------------|-------------------------------|
| **Definition**        | Dynamic structure combining multiple tables by timestamp. | Saved SQL query definition. |
| **Data Source**       | Multiple tables with timestamp alignment. | Single/multiple table query results. |
| **Storage**           | No physical storage; dynamic generation. | No storage; query logic only. |
| **Timestamp Handling**| Aligns timestamps across tables.  | Follows query logic.          |
| **Update Mechanism**  | Real-time reflection of source changes. | Depends on query execution. |
| **Special Features**  | Supports NULL filling and interpolation (prev/next/linear). | No built-in interpolation. |
| **Use Case**          | Time series alignment, cross-table analysis. | Simplify complex queries, access control. |
| **Performance**       | Potentially higher complexity.    | Similar to underlying queries. |

Mutual conversion between virtual tables and views is not supported. For example, you cannot create a view based on a virtual table or create a virtual table from a view.

## Permissions

Virtual table permissions are categorized into READ and WRITE. Query operations require READ permission, while operations to delete or modify the virtual table itself require WRITE permission.

### Syntax

#### Grant

```sql
GRANT privileges ON [db_name.]vtable_name TO user_name
privileges: { ALL | READ | WRITE }
```

#### Revoke

```sql
REVOKE privileges ON [db_name.]vtable_name FROM user_name
    privileges: { ALL | READ | WRITE }
```

### Permission Rules

1. The creator of a virtual table and the root user have all permissions by default.
2. Users can grant or revoke read/write permissions for specific virtual tables (including virtual supertables and virtual regular tables) via `dbname.vtbname`. Direct permission operations on virtual subtables are not supported.
3. Virtual subtables and virtual supertables do not support tag-based authorization (table-level authorization). Virtual subtables inherit permissions from their virtual supertables.
4. Granting and revoking permissions for other users must be performed through `GRANT` and `REVOKE` statements, and only the root user can execute these operations.
5. The detailed permission control rules are summarized below:

| No. | Operation                | Permission Requirements                                                                                                                                                                                  |  
|-----|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|  
| 1   | CREATE VTABLE            | The user has **WRITE** permission on the database to which the virtual table belongs, and <br />the user has **READ** permission on the source tables corresponding to the virtual table's data sources. |  
| 2   | DROP/ALTER VTABLE        | The user has **WRITE** permission on the virtual table. If specifying a column's data source, the user must also have **READ** permission on the source table corresponding to that column.              |  
| 3   | SHOW VTABLES             | None                                                                                                                                                                                                     |  
| 4   | SHOW CREATE VTABLE       | None                                                                                                                                                                                                     |  
| 5   | DESCRIBE VTABLE          | None                                                                                                                                                                                                     |  
| 6   | Query System Tables      | None                                                                                                                                                                                                     |  
| 7   | SELECT FROM VTABLE       | The user has **READ** permission on the virtual table.                                                                                                                                                   |  
| 8   | GRANT/REVOKE             | Only the **root user** has permission.                                                                                                                                                                   |  

## Use Cases

| SQL Query | SQL Write | STMT Query | STMT Write | Subscribe | Stream Compute |
|----------|-----------|------------|------------|-----------|----------------|
| Supported | Not Supported | Not Supported | Not Supported | Not Supported | Supported |
