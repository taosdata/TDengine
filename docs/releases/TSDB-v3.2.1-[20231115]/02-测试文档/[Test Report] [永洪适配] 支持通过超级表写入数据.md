# [Test Report] [永洪适配] 支持通过超级表写入数据

## 1. 需求来源

在九月七日沟通 BI 软件适配进展时， Jeff 明确要求支持通过超级表写入数据，这里把此需求文档化。参考文档 [[永洪适配] 在超级表查询结果中增加子表名称](https://taosdata.feishu.cn/wiki/LRw5wmVTGiM6ack0u4IcKbinnmg)，在 BI 模式下，超级表的 schema 已经包括 tbname 字段。使用 BI 软件的用户，并不了解 TDengine 有关超级表、子表的概念，以官网的智能电表场景为例，用户向名为 d1001 的电表写入数据时，一定是通过超级表进行的，会执行如下的 SQL 语句。
```sql {wrap}
INSERT INTO smeters (ts, current, voltage, phase, tbname) VALUES (now, 10.27, 0.31, 3.16, 'd1001');
```

再扩展一下，用户可能还会执行批量写入语句，例如
```sql {wrap}
INSERT INTO smeters (ts, current, voltage, phase, tbname) VALUES (now, 10.27, 0.31, 3.16, 'd1001')(now, 32.27, 8.25, 1.22, 'd1002');

```

## 2. 语法说明

扩展数据写入语法，包括批量写入、导入 CSV写入、自动建表等所有操作。
```sql
INSERT INTO
    stb1_name [(field1_name, ...)]       
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    [stb2_name [(field1_name, ...)]  
        VALUES (field1_value, ...) [(field1_value2, ...) ...] | FILE csv_file_path
    ...];

INSERT INTO stb_name [(field1_name, ...)] subquery
```

## 3. 测试用例及报告

| 测试场景 | 描述 | 测试结论 | 备注 |
| --- | --- | --- | --- |
| 场景1 | 旧的数据方式插入不受影响 | 通过 |  |
| 场景2 | 进行单超级表，tbname只有单子表，单列的插入 | 通过 |  |
| 场景3 | 进行单超级表，tbname有多子表，单列的插入 | 通过 |  |
| 场景4 | 进行单超级表，tbname只有单子表，部分列的插入 | 通过 |  |
| 场景5 | 进行单超级表，tbname有多子表，部分列的插入 | 通过 |  |
| 场景6 | 进行单超级表，tbname只有单子表，所有列的插入 | 通过 |  |
| 场景7 | 进行单超级表，tbname有多子表，所有列的插入 | 通过 |  |
| 场景8 | 进行单超级表，tbname有多子表，所有列的csv插入 | 通过 | taosadepter不支持 |
| 场景9 | 进行多超级表，每个超级表tbname只有单子表，单列的插入 | 通过 |  |
| 场景10 | 进行多超级表，每个超级表tbname有多子表，单列的插入 | 通过 |  |
| 场景11 | 进行多超级表，每个超级表tbname只有单子表，部分列的插入 | 通过 |  |
| 场景12 | 进行多超级表，每个超级表tbname有多子表，部分列的插入 | 通过 |  |
| 场景13 | 进行多超级表，每个超级表tbname只有单子表，所有列的插入 | 通过 |  |
| 场景14 | 进行多超级表，每个超级表tbname有多子表，所有列的插入 | 通过 |  |
| 场景15 | 进行多超级表，每个超级表tbname有多子表，所有列的csv插入 | 通过 | taosadepter不支持 |
