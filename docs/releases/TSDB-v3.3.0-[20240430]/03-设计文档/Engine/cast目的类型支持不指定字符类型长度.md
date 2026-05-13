# cast目的类型支持不指定字符类型长度

## 1. 背景

对比 mysql ，对于像 select cast(c1 as binary) from tbname  这种 sql 是支持的，而我们则必须指定binary 长度才可以，如 select cast(c1 as binary(24)) from tbname 才可以。本次任务支持不指定长度的 cast 语法。 
[TD-29091](https://jira.taosdata.com:18080/browse/TD-29091)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/04/11 | 0.1 | 任新胜 |  |
|  |  |  |  |

## 3. 定义

cast 不指定长度语法， 需支持以下类型
1. BINARY
2. NCHAR
3. VARCHAR
4. VARBINARY

## 4. 行为说明

1. 当使用 cast 时，BINARY/NCHAR/VARCHAR/VARBINARY 四种类型可以不指定长度。语法示例：
   - select cast(c1 as BINARY) from tbname
   - select cast(c1 as NCHAR) from tbname
   - select cast(c1 as VARCHAR) from tbname
   - select cast(c2 as VARBINARY) from tbname
2. 默认长度：当不指定长度时，程序将会自动指定最大长度作为默认长度；原因是需要提前申请指定长度的内存，因此选择最大长度可以保证能取到完整数据。
  注意：默认长度为 65517  bytes, 对于 NCHAR， 会根据每个 NCHAR 长度调节可以容纳的  NCHAR 个数；65517 / 4 = 16379
  示例：
  ```json
  taos> select v1, cast(v1 as nchar(34343)) from st11;
  
  DB error: CAST function converted length should be in range (0, 16379] NCHARS (0.000608s)
  ```

1. cast 也会出现在 where 条件或者 order by，逻辑一致，同样支持不指定长度，示例：
   - select v1, cast(v1 as nchar) from st1 where cast(v1 as nchar) like "11" order by cast(v1 as nchar);

## 5. 性能

当不指定长度时，因为使用了默认长度 65517 ，相比之前会有内存方面的影响
1. 当用户按照之前的用法，指定长度时，完全不受影响
2. 在 taos shell 中或者少量执行的语句，影响可以忽略不计
3. 当大量执行的 sql 中包含没有指定长度的 cast 时，即时占用内存会因为数据量上升，有一定影响，不推荐客户如此使用，最好能根据业务指定长度；这是新增功能，符合旧用户使用习惯，做好新用户的沟通，应该不是问题。

## 6. 兼容性

    无影响

## 7. 运维

   无影响

## 8. 使用场景

  基础场景

## 9. 约束和限制

   无

## 10. 常见错误和排查

   cast 不同类型，长度不符合要求时候报错会根据类型调整，增加可读性。

## 11. 可观测性

  taos shell 直接执行 sql 确认结果

## 12. 安装和卸载

   无

## 13. 文档

  不需要

## 14. 参考文档

  无
