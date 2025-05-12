---
title: JSON Data Type
slug: /tdengine-reference/sql-manual/json-data-type
---

## Syntax Explanation

1. Create a json type tag

   ```sql
   create stable s1 (ts timestamp, v1 int) tags (info json)

   create table s1_1 using s1 tags ('{"k1": "v1"}')
   ```

2. json value operator ->

   ```sql
   select * from s1 where info->'k1' = 'v1'

   select info->'k1' from s1
   ```

3. json key existence operator contains

   ```sql
   select * from s1 where info contains 'k2'
   
   select * from s1 where info contains 'k1'
   ```

## Supported Operations

1. In where conditions, supports functions match/nmatch/between and/like/and/or/is null/is not null, does not support in

   ```sql
   select * from s1 where info->'k1' match 'v*';

   select * from s1 where info->'k1' like 'v%' and info contains 'k2';

   select * from s1 where info is null;

   select * from s1 where info->'k1' is not null
   ```

2. Supports json tag in group by, order by, join clauses, union all, and subqueries, e.g., group by json->'key'

3. Supports distinct operation.

   ```sql
   select distinct info->'k1' from s1
   ```

4. Tag operations

   Supports modifying json tag values (full coverage)

   Supports changing json tag names

   Does not support adding json tags, deleting json tags, or modifying json tag column width

## Other Constraints

1. Only label columns can use json type, if using json tags, there can only be one label column.

2. Length limit: json key length cannot exceed 256, and keys must be printable ASCII characters; total json string length cannot exceed 4096 bytes.

3. json format restrictions:

   1. json input string can be empty ("", "\t", " " or null) or object, cannot be a non-empty string, boolean, and array.
   2. object can be {}, if object is {}, then the entire json string is considered empty. key can be "", if key is "", then the k-v pair is ignored in the json string.
   3. value can be a number (int/double) or string or bool or null, currently cannot be an array. No nesting allowed.
   4. If a json string contains two identical keys, the first one is effective.
   5. json strings currently do not support escape characters.

4. When querying a non-existent key in json, returns NULL

5. When a json tag is used as a subquery result, the upper-level query no longer supports further parsing queries on the json string from the subquery.

   For example, not supported

   ```sql
   select jtag->'key' from (select jtag from stable)
   ```

   Not supported

   ```sql
   select jtag->'key' from (select jtag from stable) where jtag->'key'>0
   ```
