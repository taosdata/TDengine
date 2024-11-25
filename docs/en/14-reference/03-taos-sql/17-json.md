---
title: JSON Data Type
description: Detailed explanation of how to use JSON types
slug: /tdengine-reference/sql-manual/json-data-type
---

## Syntax Explanation

1. Create a JSON type tag

   ```sql
   create stable s1 (ts timestamp, v1 int) tags (info json)

   create table s1_1 using s1 tags ('{"k1": "v1"}')
   ```

2. JSON value operator `->`

   ```sql
   select * from s1 where info->'k1' = 'v1'

   select info->'k1' from s1
   ```

3. JSON key existence operator `contains`

   ```sql
   select * from s1 where info contains 'k2'
   
   select * from s1 where info contains 'k1'
   ```

## Supported Operations

1. In the `WHERE` condition, supports functions like `match`, `nmatch`, `between and`, `like`, `and`, `or`, `is null`, `is not null`, but does not support `in`.

   ```sql
   select * from s1 where info->'k1' match 'v*';

   select * from s1 where info->'k1' like 'v%' and info contains 'k2';

   select * from s1 where info is null;

   select * from s1 where info->'k1' is not null
   ```

2. Supports using JSON tags in `GROUP BY`, `ORDER BY`, `JOIN` clauses, `UNION ALL`, and subqueries, such as `group by json->'key'`.

3. Supports `DISTINCT` operation.

   ```sql
   select distinct info->'k1' from s1
   ```

4. Tag Operations

   Supports modifying JSON tag values (full overwrite).

   Supports modifying JSON tag names.

   Does not support adding JSON tags, deleting JSON tags, or modifying the width of JSON tag columns.

## Other Constraints

1. Only tag columns can use JSON types; if using JSON tags, there can only be one tag column.

2. Length Limitations: The length of keys in JSON cannot exceed 256, and keys must be printable ASCII characters; the total length of the JSON string cannot exceed 4096 bytes.

3. JSON Format Restrictions:

   1. The input string for JSON can be empty (`""`, `"\t"`, `" "`, or `null`) or an object, but cannot be a non-empty string, boolean, or array.
   2. An object can be `{}`; if the object is `{}`, the entire JSON string is considered empty. Keys can be `""`; if a key is `""`, that k-v pair is ignored in the JSON string.
   3. Values can be numbers (int/double), strings, booleans, or null; arrays are currently not allowed. Nested structures are not permitted.
   4. If the JSON string contains two identical keys, the first one takes effect.
   5. JSON strings currently do not support escaping.

4. When querying a key that does not exist in JSON, NULL is returned.

5. When a JSON tag is used as a subquery result, the upper query can no longer parse the JSON string within the subquery.

   For example, this is not supported:

   ```sql
   select jtag->'key' from (select jtag from stable)
   ```

   This is also not supported:

   ```sql
   select jtag->'key' from (select jtag from stable) where jtag->'key'>0
   ```
