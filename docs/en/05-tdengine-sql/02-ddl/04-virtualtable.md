---
sidebar_label: Virtual Tables
title: Virtual Tables
description: Various management operations for virtual tables
---

## Virtual Table Overview

Virtual tables are logical tables that do not store data directly. When you query a virtual table, TDengine reads column data from one or more physical tables or existing virtual tables according to the virtual-table definition, aligns the data by timestamp, and generates the result set on demand.

Virtual tables include the following categories:

- Virtual basic tables: standalone virtual tables whose columns are defined directly with their source mappings.
- Virtual supertables: templates that define the shared columns and tags of virtual subtables and do not store data themselves.
- Virtual subtables: tables created from virtual supertables; their column data comes from other tables, and their tags can be either literals or references to tags in other tables. In addition, a virtual subtable can have owned columns and owned tags that are independent of the virtual supertable — see [Create Virtual Subtable](#create-virtual-subtable) and [Modify Virtual Subtables](#modify-virtual-subtables).

## Virtual Supertable Inheritance

A virtual supertable (VST) can inherit columns and tags from one or more parent VSTs using the `BASE ON` clause. This enables building hierarchical virtual table topologies — for example, a device-type VST that inherits common fields from a base device VST, then adds its own specialized columns.

### Create an Inherited Virtual Supertable

```sql
CREATE STABLE [IF NOT EXISTS] [db_name.]stb_name
    (col_name col_type [, ...])
    [TAGS (tag_name tag_type [, ...])]
    BASE ON [db_name.]parent_stb_name [, [db_name.]parent_stb_name] ...
    VIRTUAL 1
```

The `BASE ON` clause specifies one or more parent VSTs to inherit from. The child VST inherits all tags and all non-primary-timestamp columns from each parent. The child's own columns and tags are appended after the inherited ones.

Example:

```sql
-- Parent VST with common device fields
CREATE STABLE p_device (ts timestamp, status int) TAGS (region int) VIRTUAL 1;

-- Child VST inherits status + region, adds temperature
CREATE STABLE p_temp (ts timestamp, temp float) TAGS (sensor_id int)
    BASE ON test_db.p_device VIRTUAL 1;

-- Create a VCT under the child
CREATE VTABLE vct_t1 (status FROM src.c1, temp FROM src.c2)
    USING test_db.p_temp TAGS (100, 1);

-- Query the child VST (leaf)
SELECT * FROM test_db.p_temp;

-- Query the parent VST (non-leaf) — returns UNION ALL of all descendant VCTs
SELECT count(*) FROM test_db.p_device;
```

### Alter Inheritance

```sql
-- Add a parent
ALTER STABLE [db_name.]stb_name ADD BASE ON [db_name.]parent_stb_name;

-- Drop a parent (columns/tags contributed by that parent are removed)
ALTER STABLE [db_name.]stb_name DROP BASE ON [db_name.]parent_stb_name;
```

### Non-leaf VST Queries

When you query a non-leaf VST (one that has child VSTs inheriting from it), the engine automatically rewrites the query into a `UNION ALL` of all leaf-descendant VCTs. This means:

- `SELECT * FROM parent_vst` returns data from all descendant VCTs.
- `SELECT count(*) FROM parent_vst` aggregates across all descendants.
- Multi-level inheritance (grandparent → parent → leaf) is supported — querying the grandparent traverses the full descendant tree.

### View Inheritance Relationships

```sql
SHOW VTABLE INHERITS;
```

This displays the parent-child relationships between VSTs, including database name, child stable name, and parent stable names.

### Constraints

1. **Virtual only**: `BASE ON` requires both parent and child to be virtual supertables (`VIRTUAL 1`).
2. **Same database**: Parent and child VSTs must reside in the same database.
3. **Column/tag conflict**: The child's own column/tag names must not conflict with inherited names from any parent.
4. **Circular inheritance**: Circular dependency chains are detected and rejected.
5. **Max parents**: A VST can inherit from at most 10 parent VSTs.
6. **Non-leaf restrictions**: A non-leaf VST (one with children) cannot have VCTs created directly under it — VCTs must be created under leaf VSTs. A parent VST that already has VCTs cannot be used as a `BASE ON` target.
7. **Schema changes**: `ADD COLUMN`, `DROP COLUMN`, `ADD TAG`, `DROP TAG`, `RENAME TAG`, and tag-width changes on a parent VST that has children are rejected. The parent's schema is frozen once it becomes a non-leaf.

## Create Virtual Table

The `CREATE VTABLE` statement is used to create virtual basic tables and virtual subtables using virtual supertables as templates.

### Create Virtual Supertables

Refer to the `VIRTUAL` parameter in [Create Supertable](./03-stable.md#create-a-supertable).

### Create Virtual Basic Table

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name].vtb_name
    (
        ts_col_name timestamp,
        create_definition[ ,create_definition] ...
    )
    [TAGS (vtag_def [, vtag_def] ...)]

  create_definition:
    vtb_col_name column_definition

  column_definition:
    type_name [FROM column_reference]

  column_reference:
    [db_name.]table_name.col_name
    | ext_source_name.db_name.table_name.col_name [WHERE filter_condition]
    | ext_source_name.database_name.schema_name.table_name.col_name [WHERE filter_condition]
    | series_alias.col_name

  vtag_def:
    tag_name type_name = const_value
    | tag_name type_name FROM [db_name.]table_name.tag_name
```

For a TDengine table or an existing virtual table, `column_reference` is `[db_name.]table_name.col_name`, so the column definition remains `type_name [FROM [db_name.]table_name.col_name]`. `ext_source_name` names an existing external data source, and `series_alias` names an InfluxDB `SERIES` declaration. For the optional `WHERE filter_condition` on external references, see [External Column Reference Filters](#external-column-reference-filters). SQL Server external column references support three forms: the three-part `ext_source_name.table_name.col_name` (requires default `DATABASE` and `SCHEMA` on the source), the four-part `ext_source_name.schema.table_name.col_name` (requires a default `DATABASE`), and the five-part `ext_source_name.database.schema.table_name.col_name`; relative forms are expanded to the full five-part namespace when stored.

The same `column_reference` definition applies to virtual-subtable column reference lists and to `ALTER VTABLE` with `ADD COLUMN ... FROM` or `ALTER COLUMN ... SET`.

When creating a virtual basic table, you can declare tags with the `TAGS` clause; see the "Virtual Basic Table Tags" section below.

### Create Virtual Subtable

```sql
CREATE VTABLE [IF NOT EXISTS] [db_name].vtb_name
    (create_definition[ ,create_definition] ...)
    USING [db_name.]stb_name
    [(tag_definition [, tag_definition] ...)]
    TAGS (tag_value [, tag_value] ...)
    [SERIES series_alias AS ext_source_name.db_name.measurement_name
        (tag_name = 'tag_value' [, tag_name = 'tag_value'] ...) ...]

  create_definition:
     [stb_col_name FROM] column_reference
     | owned_col_name type_name FROM column_reference
  tag_definition:
     tag_name
     | owned_tag_name type_name
  tag_value:
     const_value
     | [db_name.]table_name.tag_name
     | FROM [db_name.]table_name.tag_name
     | tag_name FROM [db_name.]table_name.tag_name
```

#### Batch Creation of Virtual Subtables

A single `CREATE VTABLE` statement can contain multiple virtual-subtable clauses. Do not separate clauses with commas. Each clause can independently specify `IF NOT EXISTS`, a virtual supertable, column references, tag values, tag references, and `SERIES` declarations.

```sql
CREATE VTABLE
    IF NOT EXISTS meter_v1 (
        voltage FROM source_meter_1.voltage,
        current FROM source_meter_1.current
    ) USING meters_vst TAGS ('beijing', 1)
    IF NOT EXISTS meter_v2 (
        voltage FROM s2.voltage,
        current FROM s2.current
    ) USING meters_vst TAGS ('shanghai', 2)
    SERIES s2 AS influx_src.metrics.meters (site='shanghai');
```

The following rules apply to batch creation:

- `IF NOT EXISTS` applies only to the virtual subtable immediately following it.
- Each target virtual subtable must be in the same database as the virtual supertable named by its `USING` clause. Different clauses can use different databases, virtual supertables, or vgroups.
- TDengine validates every clause before sending create-table requests. If any clause has a syntax, metadata, permission, or reference error, no virtual subtable is created.
- After requests reach the server, the batch is not guaranteed to be atomic. If an error occurs after some virtual subtables have been created, those tables are not rolled back. Specify `IF NOT EXISTS` for every clause to make retries safe.
- A clause cannot reference another virtual subtable created by the same batch statement. Every reference source must exist before the statement is executed.
- Virtual basic tables and virtual supertables cannot be created in batches. A statement also cannot mix regular subtables, virtual basic tables, and virtual subtables.

**`tag_value` Syntax Notes**

- `const_value`: Use a literal constant as the tag value, matching the behavior of a regular subtable.
- `FROM [db_name.]table_name.tag_name`: A tag reference (tag-ref) to the specified source tag. When `tag_name` before `FROM` is omitted, the target tag name is the same as the source tag name. When `tag_name FROM ...` is used, the source tag can be mapped to a different target tag name.
- `[db_name.]table_name.tag_name`: A shorthand tag-ref form without `FROM`, commonly used with `ALTER VTABLE ... SET TAG`.

This is consistent with the column-reference form `[stb_col_name FROM] table_name.col_name`: the name before `FROM` is the virtual-table-side name, and the name after `FROM` is the source-side name.

**Owned columns and owned tags**

A virtual subtable can have members that are independent of the virtual supertable. Syntactically, the distinction is whether an explicit type is specified:

- In `create_definition`, an entry **without a type** (`[stb_col_name FROM] table_name.col_name`) is an aligned column (existing semantics, aligned by name or position with the virtual supertable's columns); an entry **with an explicit type** (`owned_col_name type_name FROM table_name.col_name`) declares an **owned column** — the column name is not in the virtual supertable's schema, and a `FROM` data-source reference is mandatory. Specifying a type for a column that already exists in the virtual supertable is rejected.
- In `tag_definition`, an entry **without a type** (`tag_name`) binds a virtual-supertable tag by name (existing semantics); an entry **with an explicit type** (`owned_tag_name type_name`) declares an **owned tag**, whose value is given positionally in `TAGS(...)` as a literal, `NULL`, or a tag-ref. Positional tag binding (omitting the tag list) does not support declaring owned tags.

**Example**

```sql
CREATE TABLE ev_log (ts timestamp, event bigint);

CREATE VTABLE v_d1 (
    switch   FROM p10.val,            -- aligned column: no type
    ext_ev   BIGINT FROM ev_log.event -- owned column: explicit type + reference
) USING v_devices (
    device,                           -- aligned tag: bound by name
    owner   VARCHAR(16)               -- owned tag: explicit type
) TAGS ('d1', 'alice');
```

Owned columns and owned tags exist only on that virtual subtable: they are visible when you query the subtable directly (in `SELECT *`, owned columns appear after the virtual supertable's columns), but they are **not visible** when you query the virtual supertable — the virtual supertable's schema is unaffected.

Usage Notes:

1. Naming rules for virtual tables/columns follow [Name Rules](../11-appendix/02-limit.md#naming-rules).
2. The maximum number of columns in a virtual table is 32767.
3. Maximum table name length: 192 characters.
4. The first column must be TIMESTAMP and is automatically set as primary key.
5. Row length cannot exceed 512KB (Note: VARCHAR/NCHAR/GEOMETRY columns consume 2 extra bytes each).
6. Specify maximum length for VARCHAR/NCHAR/GEOMETRY types (e.g., VARCHAR(20)).
7. Virtual tables do not support the `BLOB` or `MEDIUMBLOB` data types.
8. Use `FROM` to specify column data sources. Cross-database sources are supported via `db_name`. When `db_name` is omitted, TDengine uses the current database; if no current database is selected and `db_name` is not specified, the statement fails.
9. You cannot explicitly specify a source for the `ts` column. During queries, the virtual table's `ts` values are the union of the primary-key timestamps from the source tables of the selected columns.
10. Virtual supertables only support creating virtual subtables, and virtual subtables can only use virtual supertables as templates.
11. Ensure virtual tables' column/tag data types match their source columns/tags.
12. Virtual table names must be unique within a database and cannot conflict with table names, and it is recommended that view names do not duplicate virtual table names (not enforced). When a view and a virtual table have the same name, operations such as writing, querying, granting, and revoking permissions prioritize the virtual table with the same name.
13. When creating virtual subtables or virtual basic tables, `FROM` columns can come from regular tables, subtables, or existing virtual tables. TDengine supertables, views, and TDengine tables with composite primary keys are not supported as direct sources. For MySQL, PostgreSQL, and SQL Server external tables, see [External Column Reference Filters](#external-column-reference-filters).
14. In `TAGS (...)` for a virtual subtable, each tag can be a literal value or a tag-ref. Supported tag-ref forms are `table.tag`, `FROM table.tag`, and `tag_name FROM table.tag`. Use `db_name.table.tag` for cross-database references.
15. Reference-related limits and behaviors are described in the "Virtual-Table Reference Capabilities" section below.
16. Rules for virtual basic table tags (the `TAGS` clause) are described in the "Virtual Basic Table Tags" section below.
17. An owned column must carry a `FROM` data-source reference; purely local data columns are not supported. The reference target must be a data column (not a tag column), its type must match the declared type, and reference cycles are not allowed.
18. Owned tags do not support the `JSON` or `DECIMAL` types. If the virtual supertable has a single `JSON` tag, no owned tag can be declared on its virtual subtables.
19. Column names and tag names share a single namespace: owned column/tag names must not duplicate the virtual supertable's columns, tags, or existing owned members. In addition, if a new column/tag name introduced by `ALTER STABLE` on a virtual supertable is already used by an owned member of any virtual subtable, that `ALTER` fails.

### External Column Reference Filters

Virtual basic tables and virtual subtables can reference data columns in MySQL, PostgreSQL, or SQL Server external tables and append `WHERE` to each column reference. The condition is saved in the virtual-table definition. Each query filters the external rows before aligning the referenced columns by timestamp.

```sql
filter_condition:
    column_name comparison_operator literal
    | (filter_condition)
    | filter_condition AND filter_condition

comparison_operator: = | != | <> | < | <= | > | >=
```

**Filter constraints**

- `WHERE` is supported only for direct data-column references to MySQL, PostgreSQL, or SQL Server external tables. It cannot be appended to references to TDengine tables, other virtual tables, tag columns, or InfluxDB `SERIES`. Indirect references through other virtual tables retain filters already saved on the underlying references.
- The external table must have a primary key containing exactly one column that maps to TDengine `TIMESTAMP`. Every other primary-key column must be covered by `column = literal` in each reference's `WHERE`, ensuring at most one row per timestamp after filtering. `WHERE` is optional when the timestamp column is the entire primary key.
- Each comparison must have an unqualified column name from the current external table on the left and a numeric, string, or boolean literal on the right. The filter may use external columns that are not referenced by the virtual table. Literals must convert to the external column's data type; timestamp values must be string literals.
- Each external column may appear only once in a filter. MySQL and SQL Server column matching is case insensitive (for SQL Server, the database collation applies). PostgreSQL column matching is case sensitive: identifiers without backticks are converted to lowercase, so use backticks to preserve column names containing uppercase letters. For example, PostgreSQL columns named `device` and `Device` are distinct and may both appear in the same filter. Non-timestamp primary-key columns require `=`; other columns support the comparison operators listed above. Multiple comparisons must be joined by `AND`; parentheses are allowed.
- `OR`, `NOT`, `IN`, `LIKE`, `IS NULL`, `NULL` values, functions, arithmetic expressions, subqueries, and parameter placeholders are not supported. `BETWEEN` expands into two comparisons of the same column and is therefore also unsupported.
- A query's `WHERE` cannot replace the primary-key filter required in a column definition. Different columns can reference different filtered results from the same external table. Filters apply independently; timestamps are the union of the filtered results for references involved in the query, with missing values filled with `NULL`.

Assume an existing MySQL external data source named `mysql_src` contains `metrics.readings` with columns `ts`, `device`, `tenant`, `v`, and `w`. Its primary key is `(ts, device, tenant)`; `ts` maps to `TIMESTAMP`, and `v` and `w` map to `INT`:

```sql
CREATE VTABLE filtered_readings (
    ts TIMESTAMP,
    a INT FROM mysql_src.metrics.readings.v WHERE device = 'd1' AND tenant = 1,
    b INT FROM mysql_src.metrics.readings.w WHERE device = 'd2' AND tenant = 1
);

CREATE STABLE filtered_stable (ts TIMESTAMP, a INT) TAGS (site INT) VIRTUAL 1;
CREATE VTABLE filtered_child (
    a FROM mysql_src.metrics.readings.v WHERE device = 'd1' AND tenant = 1
) USING filtered_stable TAGS (1);

ALTER VTABLE filtered_readings ADD COLUMN c INT
    FROM mysql_src.metrics.readings.v WHERE device = 'd1' AND tenant = 1 AND v >= 10;
ALTER VTABLE filtered_readings ALTER COLUMN a SET
    mysql_src.metrics.readings.v WHERE device = 'd2' AND tenant = 1;
ALTER VTABLE filtered_readings ALTER COLUMN b SET NULL;
SHOW CREATE VTABLE filtered_readings;
```

`ALTER COLUMN ... SET` replaces the entire reference and its condition; there is no separate operation to edit only `WHERE`. Omitting `WHERE` when rebinding clears the old condition, but the new reference must still satisfy the primary-key requirements. In this example, the composite key requires equality conditions for both `device` and `tenant`. `SET NULL` clears both the reference and its filter. `SHOW CREATE VTABLE` includes the saved `WHERE` so that the definition can be recreated.

Queries revalidate saved conditions against the current external metadata. Removing a filter column, changing a type so that a literal is incompatible, or changing the primary key so that the filter no longer covers it causes affected queries to fail. Failure to execute the external filter also fails the query instead of returning unfiltered data.

### Virtual Basic Table Tags

Like its columns, a virtual basic table's tags come in two forms, which can be mixed in the same table:

- **Owned tag**: A tag owned by the table itself. Its value is specified inline at creation (`tag_name type_name = const_value`, where `= NULL` means a NULL value) or written later with `SET TAG`, and is stored in the table. An owned tag is a table-level constant: projecting it returns that constant for every row, and using it in a `WHERE` filter is evaluated with constant semantics.
- **Tag reference (tag-ref)**: `tag_name type_name FROM [db_name.]table_name.tag_name` references a tag of an underlying physical table and is resolved to the source tag's current value at query time. When used in a `WHERE` filter, the predicate is pushed down to the source table's tag index so that only matching subtables are scanned.

**Example**

In a smart-meter scenario, the subtables of the supertable `meters` serve as tag-ref sources:

```sql
CREATE STABLE meters (ts TIMESTAMP, v INT) TAGS (region VARCHAR(16), gid INT);
CREATE TABLE d0 USING meters TAGS ('us-east', 1);

-- Create a virtual basic table with both an owned tag and a tag-ref
CREATE VTABLE vntb (
    ts TIMESTAMP,
    v  INT FROM db.d0.v                    -- column reference
) TAGS (
    owner  VARCHAR(16) = 'alice',          -- owned tag
    level  INT = 0,                        -- owned tag
    region VARCHAR(16) FROM db.d0.region   -- tag-ref, value follows d0.region
);

SELECT owner, level FROM vntb;             -- owned tags, projected from the table itself
SELECT region FROM vntb;                   -- tag-ref, resolved from the source table
SELECT * FROM vntb WHERE region = 'us-east';  -- filter pushed down to the source tag index
```

Usage Notes:

1. Every tag in the `TAGS` clause must carry an explicit value: use `= const_value` for an owned tag (`= NULL` is a valid explicit value) or `FROM [db_name.]table_name.tag_name` for a tag-ref. A bare `tag_name type_name` (neither `=` nor `FROM`) is rejected.
2. A tag-ref must point to a tag column of a TDengine table (a subtable or virtual subtable), not a data column; the declared type must match the source tag type; external data sources are not supported.
3. Tag-refs follow the same permission rules as column references: creating or modifying a tag-ref requires `READ` permission on the source table.
4. The `DECIMAL` type is not supported for tags; tag count and total length limits are the same as for other tables — see [General Restrictions](../11-appendix/02-limit.md#general-restrictions).
5. A `JSON` tag can only be declared at table creation and must be the only tag of the table; `JSON` tags cannot be added later with `ALTER`.
6. Tag definitions do not support column options such as `PRIMARY KEY`, `ENCODE`, `COMPRESS`, or `COMMENT`.

### Virtual-Table Reference Capabilities

#### What references mean

In a virtual table, a referenced column or tag does not store copied data. Instead, TDengine resolves it dynamically from the source table at query time:

- **Tag reference (tag-ref)**: A virtual subtable's tag value references a tag column of another table and is resolved to the source tag's current value at query time. If the source tag is updated with `ALTER TABLE ... SET TAG`, query results on the virtual table reflect the new value immediately.
- **Column reference (col-ref)**: A virtual-table data column references a data column of another table, including another virtual table. New data written to the source becomes visible through the virtual table on subsequent queries.

#### Reference chains

- Virtual-table columns can reference columns from existing virtual tables, so multi-hop chains such as virtual table -> virtual table -> physical table are supported.
- Tag-ref and col-ref can be mixed across multiple hops.
- Same-database and cross-database reference chains are supported.

#### Behavior when referenced objects change

| Change operation | Impact on virtual tables |
| --- | --- |
| Source table tag updated with `ALTER TABLE ... SET TAG` | Queries on tag-ref virtual tables immediately reflect the new value |
| New data written to the source table | Queries on col-ref virtual tables can see the new data |
| Source table dropped with `DROP TABLE` | Virtual-table queries fail because the source no longer exists |
| Referenced source column dropped with `ALTER TABLE ... DROP COLUMN` | Virtual-table queries fail because the referenced column no longer exists |
| A referenced virtual table is dropped | Other virtual tables that reference it fail during query |
| Source tag-column type change | Rejected while dependent tag-refs exist |

#### Constraints

- A tag-ref must point to a tag column, not a data column, and the source tag type must match the target virtual tag type.
- A col-ref must point to a data column, not a tag column, and the source column type must match the target virtual column type.
- TDengine supertables, views, and TDengine tables with composite primary keys are not supported as reference sources. MySQL, PostgreSQL, and SQL Server external tables can use composite primary keys that meet the [filter requirements](#external-column-reference-filters).
- Reference cycles are not allowed and are rejected during validation.
- The total reference-chain depth cannot exceed 32 hops; validation or query execution returns `0x8000620C` when the chain exceeds that limit.

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
        <td align="center"></td>
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
  ADD COLUMN vtb_col_name vtb_column_type [FROM column_reference]
  | DROP COLUMN vtb_col_name
  | ALTER COLUMN vtb_col_name SET {column_reference | NULL}
  | MODIFY COLUMN col_name column_type
  | RENAME COLUMN old_col_name new_col_name
  | ADD TAG tag_name tag_type [FROM [db_name.]table_name.tag_name]
  | SET TAG tag_name = {new_tag_value | [db_name.]table_name.tag_name}
  | DROP TAG tag_name
  | RENAME TAG old_tag_name new_tag_name
}
```

### Usage Notes

For virtual basic tables, the following modifications are supported:

1. `ADD COLUMN`: Add a column.
2. `DROP COLUMN`: Drop a column.
3. `MODIFY COLUMN`: Modify the column definition. For variable-length data types, this can be used only to increase the width, not decrease it. If the virtual-table column already has a source column, widening the column fails because the new width no longer matches the source-column width. Clear the source first with `ALTER COLUMN ... SET NULL`, then modify the width.
4. `RENAME COLUMN`: Rename a column.
5. `ALTER COLUMN ... SET`: Change the source of a column. `SET NULL` clears the source of the virtual-table column.
6. `ADD TAG`: Add a tag. Without `FROM` it adds an owned tag (initial value `NULL`); with `FROM` it adds a tag-ref.
7. `SET TAG`: Modify a tag. Setting it to a literal (including `NULL`) clears any existing tag-ref and converts the tag to an owned tag; setting it to `[db_name.]table_name.tag_name` converts an owned tag to a tag-ref or repoints an existing tag-ref to another source tag. An error is returned if the source tag does not exist, the types do not match, or the target is a data column.
8. `DROP TAG`: Drop a tag; both owned tags and tag-refs can be dropped.
9. `RENAME TAG`: Change a tag's name; both owned tags and tag-refs can be renamed. Only the display name changes: an owned tag keeps its value and a tag-ref keeps pointing at its source tag. The new name must not clash with an existing column or tag of the table. Supported since v3.4.3.0.

### Add Column

```sql
ALTER VTABLE vtb_name ADD COLUMN vtb_col_name vtb_col_type [FROM column_reference]
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
ALTER VTABLE vtb_name ALTER COLUMN vtb_col_name SET {column_reference | NULL}
```

### Add Tag

```sql
-- Add an owned tag (initial value NULL)
ALTER VTABLE vtb_name ADD TAG tag_name tag_type;

-- Add a tag-ref
ALTER VTABLE vtb_name ADD TAG tag_name tag_type FROM [db_name.]table_name.tag_name;
```

### Modify Tag

```sql
-- Set an owned tag value; on a tag-ref this clears the reference and converts it to an owned tag
ALTER VTABLE vtb_name SET TAG tag_name = new_tag_value;

-- Convert an owned tag to a tag-ref, or repoint a tag-ref to another source tag
ALTER VTABLE vtb_name SET TAG tag_name = [db_name.]table_name.tag_name;
```

The conversion semantics of `SET TAG` are the same as for virtual subtables; see the "Modify Virtual Subtables" section below.

### Drop Tag

```sql
ALTER VTABLE vtb_name DROP TAG tag_name;
```

### Rename Tag

```sql
ALTER VTABLE vtb_name RENAME TAG old_tag_name new_tag_name;
```

## Modify Virtual Subtables

```sql
ALTER VTABLE [db_name.]vtb_name alter_table_clause

alter_table_clause: {
  ALTER COLUMN vtb_col_name SET {column_reference | NULL}
  | ADD COLUMN owned_col_name type_name FROM column_reference
  | ADD TAG owned_tag_name type_name
  | SET TAG tag_name = {new_tag_value | [db_name.]table_name.tag_name}
  | DROP COLUMN owned_col_name
  | DROP TAG owned_tag_name
}
```

**Usage Notes**

1. Changes to columns and tags that a virtual subtable inherits from its virtual supertable (cascaded members) must be performed through the virtual supertable.
2. For a virtual subtable's **owned columns** and **owned tags**, `ADD COLUMN`, `ADD TAG`, `SET TAG`, `DROP COLUMN`, and `DROP TAG` can be executed directly on the subtable.
3. `DROP COLUMN` / `DROP TAG` apply only to owned members; applying them to a cascaded member fails — use `ALTER STABLE` instead (which cascades to all virtual subtables under the virtual supertable).
4. After an owned member is dropped, its name is released immediately and can be reused by either a column or a tag. Dropping an owned tag that carries a tag-ref also clears that reference.
5. `MODIFY COLUMN`, `RENAME COLUMN`, and `RENAME TAG` are not supported for virtual subtables; the width and name of an owned member cannot be changed — drop and re-create it instead.

### Modify Subtable Tag Value

```sql
ALTER VTABLE tb_name SET TAG tag_name1=new_tag_value1, tag_name2=new_tag_value2 ...;
```

`SET TAG` can assign either a literal value or a tag-ref to a tag. This also applies to **owned tags**: assigning a literal clears any existing reference; assigning a tag-ref makes the value track the source tag dynamically.

#### Set a Tag to a Literal

```sql
ALTER VTABLE v0 SET TAG local_tag='local0_updated';
```

When a tag is set to a literal value, any existing tag-ref on that tag is cleared. The tag then becomes a static value that no longer tracks the source.

#### Set a Tag to a tag-ref (create or repoint a reference)

```sql
-- Same database: reference the city tag of table src0
ALTER VTABLE v0 SET TAG ref_city=src0.city;

-- Cross database: use the db_name.table.tag three-part form
ALTER VTABLE v0 SET TAG ref_city=db1.src1.city;
```

After a tag is set to a tag-ref, queries resolve it to the referenced tag's current value at query time. This operation can both add a reference to a tag that was previously a literal and repoint an existing tag-ref to a different source tag. The constraints match those for tag-refs at `CREATE VTABLE` time:

- The referenced object must be a tag column (of a child table or virtual child table), not a data column.
- The source tag and target tag must have the same data type.
- Reference cycles are not allowed (for example, pointing a tag of `v_a` at a virtual table that ultimately references `v_a` again); this is validated and rejected.
- The total reference chain depth must not exceed 32 hops; exceeding it returns error code `0x8000620C`.

#### Repointing within a multi-hop reference chain

When a virtual subtable's tag is one link of a multi-hop chain (for example, `v2_0.l2_ref_city -> v0.ref_city -> src0.city`), you can adjust the reference at any level with `SET TAG`, and the change propagates at query time following dynamic-binding rules:

- Repointing an intermediate link (e.g. `ALTER VTABLE v0 SET TAG ref_city=src1.city`) changes the result of upper-layer virtual tables that reference it.
- Repointing the top link (e.g. `ALTER VTABLE v2_0 SET TAG l2_ref_city=db.v1.ref_city`) can redirect it to a different chain, or even point it directly at a physical tag to "flatten" the chain.
- Setting an intermediate tag to a literal clears that link's reference, severing the upper layer's propagation from the original physical source; the upper layer then resolves to that literal value.

> **Note**: The batch form `ALTER VTABLE USING stb_name SET TAG ... WHERE ...` (modifying tags through the virtual super table) accepts literal values only and does not support setting a tag to a tag-ref. To set or repoint a tag-ref, use the single-subtable `ALTER VTABLE vtb_name SET TAG ...` syntax shown above.

### Change Column Source

```sql
ALTER VTABLE vtb_name ALTER COLUMN vtb_col_name SET {column_reference | NULL}
```

### Add an Owned Column

```sql
ALTER VTABLE vtb_name ADD COLUMN owned_col_name type_name FROM column_reference
```

Appends an owned column to a virtual subtable. The `FROM` data-source reference is mandatory — omitting it fails. The reference target must be a data column, its type must match the declared type, and validation follows the same rules as at creation time.

```sql
ALTER VTABLE v_d1 ADD COLUMN ext_v2 INT FROM p20.val;
```

### Add an Owned Tag

```sql
ALTER VTABLE vtb_name ADD TAG owned_tag_name type_name
```

Appends an owned tag to a virtual subtable with an initial value of `NULL`; literal values or tag-refs are attached with `SET TAG` (see "Modify Subtable Tag Value" above).

```sql
ALTER VTABLE v_d1 ADD TAG dept VARCHAR(16);
ALTER VTABLE v_d1 SET TAG dept = 'rd';
```

### Drop an Owned Column or Owned Tag

```sql
ALTER VTABLE vtb_name DROP COLUMN owned_col_name
ALTER VTABLE vtb_name DROP TAG owned_tag_name
```

Only owned members can be dropped. To remove a cascaded column or tag inherited from the virtual supertable, use `ALTER STABLE stb_name DROP COLUMN/DROP TAG` (this cascades to all virtual subtables and does not affect other owned members).

## Drop Virtual Tables

You can drop one or more virtual tables (virtual regular tables or virtual subtables) in a single SQL statement. The tables can belong to different databases.

```sql
DROP VTABLE [IF EXISTS] [dbname].vtb_name [, [IF EXISTS] [dbname].vtb_name] ...;
```

**Notes**

- When dropping multiple tables, all tables in the list are validated first (whether each table exists, is a virtual table, and is not a virtual supertable). If any table fails validation, the entire statement fails and no tables are dropped.
- `IF EXISTS` only applies to the single table it precedes. For example, in `DROP VTABLE IF EXISTS vtb1, vtb2`, a missing `vtb1` is skipped, but a missing `vtb2` still returns an error.
- If the same table name appears multiple times in the list, the first occurrence is dropped and subsequent occurrences return an error because the table no longer exists (consistent with `DROP TABLE`).
- A virtual supertable cannot be dropped with `DROP VTABLE`; use `DROP STABLE` instead.

## View Virtual Table Information

### List Virtual Tables

```sql
SHOW [NORMAL | CHILD] [db_name.]VTABLES [LIKE 'pattern'];
```

Usage Notes:

1. If `db_name` is omitted, `SHOW VTABLES` lists virtual basic tables and virtual subtables in the current database. If no current database is selected and `db_name` is not specified, the statement fails with `database not specified`. `LIKE` can be used for fuzzy matching. `NORMAL` lists only virtual basic tables, and `CHILD` lists only virtual subtables.
2. `SHOW TABLES` does not return the virtual basic tables and virtual subtables described here; use `SHOW VTABLES` instead.

### Show Creation Statement

```sql
SHOW CREATE VTABLE [db_name.]vtable_name;
```

Displays the creation statement for the specified virtual table. For virtual subtables created with tag-ref, the returned statement preserves the tag-ref definition.
For a virtual subtable with owned members, the returned statement also includes the full definition of owned columns (with their source references) and owned tags (with their values or references); `DECIMAL` columns are rendered with precision and scale as `DECIMAL(p,s)`. The output is a single, fully replayable DDL.
For a virtual basic table with tags, the returned statement includes the `TAGS(...)` clause: owned tags are inlined as `= value` (a NULL value is emitted as `= NULL`) and tag-refs as `FROM db_name.table_name.tag_name`, so a single statement fully recreates the virtual table.

### Describe Structure

```sql
DESCRIBE [db_name.]vtb_name;
```

`DESCRIBE` shows the virtual table's columns and tags. For tag-ref or col-ref entries, the result also shows the reference source. Tag rows are marked with `TAG` (both owned tags of virtual basic tables and owned tags of virtual subtables). For a virtual subtable with owned members, owned columns appear as regular data columns, after the virtual supertable's columns.

### Show Current Tag Values of a Virtual Child Table

```sql
SHOW TAGS FROM child_table_name [FROM db_name];
SHOW TAGS FROM [db_name.]child_table_name;
```

For tag-ref virtual subtables, `SHOW TAGS` returns the currently resolved tag values. `SHOW TAGS` also supports virtual basic tables: owned tags return the table's own value, and tag-refs return the resolved source value.

### Validate Virtual-Table References

```sql
SHOW VTABLE VALIDATE FOR [db_name.]vtb_name;
```

`SHOW VTABLE VALIDATE` checks column/tag references for a virtual basic table or virtual child table and returns the same validation metadata as `information_schema.ins_virtual_tables_referencing`, including `err_code` and `err_msg`.

### Query Virtual Basic Tables and Virtual Subtables

```sql
SELECT ... FROM information_schema.ins_tables WHERE type = 'VIRTUAL_NORMAL_TABLE' OR type = 'VIRTUAL_CHILD_TABLE';
```

```sql
SELECT ... FROM information_schema.ins_virtual_tables_referencing;
```

Use `ins_virtual_tables_referencing` to inspect source database, source table, source column, and validation status for virtual-table columns and tags.

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
privileges: {
    ALL,
  | priv_type [, priv_type] ...
}
priv_type: {
    READ
  | WRITE
}
```

#### Revoke

```sql
REVOKE privileges ON [db_name.]vtable_name FROM user_name
privileges: {
    ALL,
  | priv_type [, priv_type] ...
}
priv_type: {
    READ
  | WRITE
}
```

### Permission Rules

1. The creator of a virtual table and the root user have all permissions by default.
2. Users can grant or revoke read/write permissions for specific virtual tables (including virtual supertables and virtual regular tables) via `dbname.vtbname`. Direct permission operations on virtual subtables are not supported.
3. Virtual subtables and virtual supertables do not support tag-based authorization (table-level authorization). Virtual subtables inherit permissions from their virtual supertables.
4. Granting and revoking permissions for other users must be performed through `GRANT` and `REVOKE` statements, and only the root user can execute these operations.
5. The detailed permission control rules are summarized below:

| No. | Operation                | Permission Requirements                                                                                                                                                                                  |
|-----|--------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1   | CREATE VTABLE            | The user has **WRITE** permission on the database to which the virtual table belongs, and <br />the user has **READ** permission on the source tables or source virtual tables referenced by the virtual table. |
| 2   | DROP/ALTER VTABLE        | The user has **WRITE** permission on the virtual table. If specifying a source for either a column reference or a tag reference, the user must also have **READ** permission on the referenced source table or source virtual table. |
| 3   | SHOW VTABLES             | None                                                                                                                                                                                                     |
| 4   | SHOW CREATE VTABLE       | None                                                                                                                                                                                                     |
| 5   | DESCRIBE VTABLE          | None                                                                                                                                                                                                     |
| 6   | Query System Tables      | None                                                                                                                                                                                                     |
| 7   | SELECT FROM VTABLE       | The user has **READ** permission on the virtual table.                                                                                                                                                   |
| 8   | GRANT/REVOKE             | Only the **root user** has permission.                                                                                                                                                                   |

## Use Cases

| SQL Query | SQL Write | STMT Query | STMT Write |
|----------|-----------|------------|------------|
| Supported | Not Supported | Not Supported | Not Supported |
