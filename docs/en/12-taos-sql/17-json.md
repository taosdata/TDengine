---
title: JSON Type
sidebar_label: JSON Type
description: This document describes the JSON data type in TDengine.
---


## Syntax

1. Tag of type JSON

   ```
   create stable s1 (ts timestamp, v1 int) tags (info json)

   create table s1_1 using s1 tags ('{"k1": "v1"}')
   ```

2. "->" Operator of JSON

   ```
   select * from s1 where info->'k1' = 'v1'

   select info->'k1' from s1
   ```

3. "contains" Operator of JSON

   ```
   select * from s1 where info contains 'k2'

   select * from s1 where info contains 'k1'
   ```

## Applicable Operations

1. When a JSON data type is used in `where`, `match/nmatch/between and/like/and/or/is null/is no null` can be used but `in` can't be used.

   ```
   select * from s1 where info->'k1' match 'v*';

   select * from s1 where info->'k1' like 'v%' and info contains 'k2';

   select * from s1 where info is null;

   select * from s1 where info->'k1' is not null
   ```

2. A tag of JSON type can be used in `group by`, `order by`, `join`, `union all` and sub query; for example `group by json->'key'`

3. `Distinct` can be used with a tag of type JSON

   ```
   select distinct info->'k1' from s1
   ```

4. Tag Operations

   The value of a JSON tag can be altered. Please note that the full JSON will be overridden when doing this.

   The name of a JSON tag can be altered.

   A tag of JSON type can't be added or removed. The column length of a JSON tag can't be changed.

## Other Restrictions

- JSON type can only be used for a tag. There can be only one tag of JSON type, and it's exclusive to any other types of tags.

- The maximum length of keys in JSON is 256 bytes, and key must be printable ASCII characters. The maximum total length of a JSON is 4,096 bytes.

- JSON format:

   - The input string for JSON can be empty, i.e. "", "\t", or NULL, but it can't be non-NULL string, bool or array.
   - object can be {}, and the entire JSON is empty if so. Key can be "", and it's ignored if so.
   - value can be int, double, string, bool or NULL, and it can't be an array. Nesting is not allowed which means that the value of a key can't be JSON.
   - If one key occurs twice in JSON, only the first one is valid.
   - Escape characters are not allowed in JSON.

- NULL is returned when querying a key that doesn't exist in JSON.

- If a tag of JSON is the result of inner query, it can't be parsed and queried in the outer query.

   For example, the SQL statements below are not supported.

   ```
   select jtag->'key' from (select jtag from stable)
   ```

   and

   ```
   select jtag->'key' from (select jtag from stable) where jtag->'key'>0
   ```
