---
sidebar_label: JSON 类型
title: JSON 类型
description: 对 JSON 类型如何使用的详细说明
---


## 语法说明

1. 创建 json 类型 tag

   ```
   create stable s1 (ts timestamp, v1 int) tags (info json)

   create table s1_1 using s1 tags ('{"k1": "v1"}')
   ```

2. json 取值操作符 ->

   ```
   select * from s1 where info->'k1' = 'v1'

   select info->'k1' from s1
   ```

3. json key 是否存在操作符 contains

   ```
   select * from s1 where info contains 'k2'

   select * from s1 where info contains 'k1'
   ```

## 支持的操作

1. 在 where 条件中时，支持函数 match/nmatch/between and/like/and/or/is null/is no null，不支持 in

   ```
   select * from s1 where info->'k1' match 'v*';

   select * from s1 where info->'k1' like 'v%' and info contains 'k2';

   select * from s1 where info is null;

   select * from s1 where info->'k1' is not null
   ```

2. 支持 json tag 放在 group by、order by、join 子句、union all 以及子查询中，比如 group by json->'key'

3. 支持 distinct 操作.

   ```
   select distinct info->'k1' from s1
   ```

4. 标签操作

   支持修改 json 标签值（全量覆盖）

   支持修改 json 标签名

   不支持添加 json 标签、删除 json 标签、修改 json 标签列宽

## 其他约束条件

1. 只有标签列可以使用 json 类型，如果用 json 标签，标签列只能有一个。

2. 长度限制：json 中 key 的长度不能超过 256，并且 key 必须为可打印 ascii 字符；json 字符串总长度不超过 4096 个字节。

3. json 格式限制：

   1. json 输入字符串可以为空（"","\t"," "或 null）或 object，不能为非空的字符串，布尔型和数组。
   2. object 可为{}，如果 object 为{}，则整个 json 串记为空。key 可为""，若 key 为""，则 json 串中忽略该 k-v 对。
   3. value 可以为数字(int/double)或字符串或 bool 或 null，暂不可以为数组。不允许嵌套。
   4. 若 json 字符串中出现两个相同的 key，则第一个生效。
   5. json 字符串里暂不支持转义。

4. 当查询 json 中不存在的 key 时，返回 NULL

5. 当 json tag 作为子查询结果时，不再支持上层查询继续对子查询中的 json 串做解析查询。

   比如暂不支持

   ```
   select jtag->'key' from (select jtag from stable)
   ```

   不支持

   ```
   select jtag->'key' from (select jtag from stable) where jtag->'key'>0
   ```
