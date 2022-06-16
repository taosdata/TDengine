# Insert Data

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## SQL Examples

Here are some brief examples for `INSET` statement. You can execute these statements manually by TDengine CLI or TDengine Cloud Explorer or programmatically by TDengine connectors. 

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


## Connector Examples

<Tabs>
<TabItem value="python" label="Python">

In this example, we use `execute` method to execute SQL and get affected rows. The variable `conn` is an instance of class  `taosrest.TaosRestConnection` we just created at [Connect Tutorial](./connect/python#connect).

```python
{{#include docs/examples/python/develop_tutorial.py:insert}}
```
</TabItem>
<TabItem value="java" label="Java">
</TabItem>
<TabItem value="go" label="Go">
</TabItem>
<TabItem value="rust" label="Rust">
</TabItem>
<TabItem value="node" label="Node.js">
</TabItem>
</Tabs>
