# Insert Data

## Introduction

Application programs can execute `INSERT` statement through connectors to insert rows. The TAOS CLI can also be used to manually insert data.

### Insert Single Row

The below SQL statement is used to insert one row into table "d1001".

```sql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31);
```

### Insert Multiple Rows

Multiple rows can be inserted in a single SQL statement. The example below inserts 2 rows into table "d1001".

```sql
INSERT INTO d1001 VALUES (1538548684000, 10.2, 220, 0.23) (1538548696650, 10.3, 218, 0.25);
```

### Insert into Multiple Tables

Data can be inserted into multiple tables in the same SQL statement. The example below inserts 2 rows into table "d1001" and 1 row into table "d1002".

```sql
INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31) (1538548695000, 12.6, 218, 0.33) d1002 VALUES (1538548696800, 12.3, 221, 0.31);
```

For more details about `INSERT` please refer to [INSERT](/taos-sql/insert).

:::info

- Inserting in batches can improve performance. Normally, the higher the batch size, the better the performance. Please note that a single row can't exceed 48K bytes and each SQL statement can't exceed 1MB.
- Inserting with multiple threads can also improve performance. However, depending on the system resources on the application side and the server side, when the number of inserting threads grows beyond a specific point the performance may drop instead of improving. The proper number of threads needs to be tested in a specific environment to find the best number.

:::

:::warning

- If the timestamp for the row to be inserted already exists in the table, the behavior depends on the value of parameter `UPDATE`. If it's set to 0 (the default value), the row will be discarded. If it's set to 1, the new values will override the old values for the same row.
- The timestamp to be inserted must be newer than the timestamp of subtracting current time by the parameter `KEEP`. If `KEEP` is set to 3650 days, then the data older than 3650 days ago can't be inserted. The timestamp to be inserted can't be newer than the timestamp of current time plus parameter `DAYS`. If `DAYS` is set to 2, the data newer than 2 days later can't be inserted.

:::