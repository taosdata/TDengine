# 支持 ALL/ANY/SOME/[NOT]EXISTS 运算符与子查询 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-03 | 2026-03-06 | 1.0 | 潘魏 | 定义ALL/ANY/SOME/EXISTS/NOT EXISTS 功能与语法 |

## 2. 背景

ALL/ANY/SOME/EXISTS/NOT EXISTS 作为 SQL 标准中常用的运算符，常与子查询结合实现复杂的条件筛选逻辑，在实际业务查询中使用频率高。为提升 TDengine 对 SQL 标准的兼容性，满足用户复杂查询需求，开发该功能，支持上述运算符与子查询的结合使用。

## 3. 定义

1. **非相关子查询**：子查询中不会引用父查询的列，本次功能支持该类子查询与 ALL/ANY/SOME/EXISTS/NOT EXISTS 结合使用。
2. **ALL 运算符**：用于判断表达式是否满足子查询返回的**所有**结果，需与比较运算符（=、>、<、>=、<=、<>）结合使用。
3. **ANY/SOME 运算符**：二者功能等价，用于判断表达式是否满足子查询返回的**任意一个**结果，需与比较运算符结合使用。
4. **EXISTS 运算符**：判断子查询是否**返回至少一行数据**，子查询结果为非空时返回 TRUE，否则返回 FALSE。
5. **NOT EXISTS 运算符**：与 EXISTS 逻辑相反，判断子查询是否**无数据返回**，子查询结果为空时返回 TRUE，否则返回 FALSE。

## 4. 行为说明

### 4.1 语法

支持 SQL 标准语法格式，具体如下：

#### 4.1.1 ALL/ANY/SOME 语法

```plaintext
expr [=|>|<|>=|<=|<>] [ALL|ANY|SOME] (subquery)
```

#### 4.1.2 EXISTS/NOT EXISTS 语法

```plaintext
EXISTS (subquery)
NOT EXISTS (subquery)
```

### 4.2 功能

1. 支持 ALL/ANY/SOME 与非相关子查询结合使用，子查询仅能输出**单列数据**，子查询可为任意满足输出要求的查询语句，每个非相关子查询仅执行一次。
2. 支持 EXISTS/NOT EXISTS 与非相关子查询结合使用，子查询无列数限制，仅判断结果集是否为空，每个非相关子查询仅执行一次。
3. ALL/ANY/SOME/EXISTS/NOT EXISTS 与子查询的组合，可在查询语句的**任意子句、函数、表达式**中使用，只要语法定义支持即可。
4. 对于目前产品中未支持上述运算符+子查询的使用但有需求的场景，可后续修改支持，当前版本适用范围不扩大。
5. 非相关子查询可**嵌套使用**，嵌套的子查询同样遵循本文档的约束与限制。
6. 运算符遵循 SQL 标准逻辑：
  - ALL：表达式与子查询所有结果的比较均为 TRUE 时，整体结果为 TRUE；
  - ANY/SOME：表达式与子查询任意一个结果的比较为 TRUE 时，整体结果为 TRUE；
  - EXISTS：子查询返回至少一行，结果为 TRUE，否则为 FALSE；
  - NOT EXISTS：子查询无返回行，结果为 TRUE，否则为 FALSE。

### 4.3 范围

1. 第一个版本仅在**查询语句**中支持，可在普通查询、嵌套查询、UNION/UNION ALL、INSERT INTO SELECT、STMT 查询、视图中使用上述运算符+非相关子查询的组合。
2. 流计算、订阅、DDL、DML 语句中暂不支持。

## 5. 性能

1. 对 TDengine 既有功能的性能无影响。
2. 上述运算符结合非相关子查询的执行性能，与同等逻辑的嵌套查询性能相当，部分组合进行了性能优化与标准执行流程相比性能更优。
3. EXISTS/NOT EXISTS 采用**短路求值**逻辑，子查询返回第一行数据后立即终止执行，无额外性能开销。

## 6. 安全

不涉及

## 7. 兼容性

不涉及

## 8. 运维

可通过 explain 命令查看执行计划与性能。

## 9. 使用场景

以下为符合语法规范的使用示例语句，覆盖不同查询子句与使用场景：
```sql
-- ALL 结合子查询，WHERE 子句中使用
select col1, col2 from tb1 where col1 > ALL (select f1 from tb2 where f2 > 10);
-- ANY 结合子查询，JOIN ON 条件中使用
select a.ts, b.val from tb1 a join tb2 b on a.ts = b.ts and a.f1 < ANY (select col1 from tb3 union select col1 from tb4);
-- SOME 与 ANY 等价，HAVING 子句中使用
select avg(f1) from tb1 group by f1 having sum(f2) >= SOME (select f3 from tb2 interval(1s));
-- EXISTS 结合子查询，CASE 表达式中使用
select case when exists (select 1 from tb2 where tb2.col1 = tb1.col1) then 'exist' else 'not exist' end from tb1;
-- NOT EXISTS 结合子查询，SELECT 列表中使用
select col1, not exists (select f1 from tb3 where f1 = tb1.col1) as flag from tb1;
-- 嵌套非相关子查询，ALL 中使用
select col1 from tb1 where col1 <> ALL (select avg(f1) from tb2 where f2 in (select col2 from tb3) group by f2);
-- UNION 中使用 EXISTS
select col1 from tb1 where exists (select 1 from tb4) union select col2 from tb2 where not exists (select 1 from tb5);
-- INSERT INTO SELECT 中使用 ANY
insert into tb6 select ts, f1 from tb1 where f1 = ANY (select col1 from tb7 where ts > '2026-01-01 00:00:00');
```

## 10. 约束和限制

1. 仅在**查询语句**中支持上述运算符+非相关子查询的组合，流计算、订阅、DDL、DML 语句中暂不支持；
2. 不带 FROM 的查询暂不支持使用上述运算符+非相关子查询；
3. Query policy 4 暂不支持上述运算符+非相关子查询；
4. **相关子查询暂不支持**，即子查询中不能引用父查询的列；
5. ALL/ANY/SOME 对应的子查询**必须返回单列数据**，否则报语法错误；
6. 子查询的结果集数据类型，需与运算符左侧的表达式数据类型**兼容**，否则报类型不匹配错误。

## 11. 常见错误和排查

1. 可根据 TDengine 抛出的错误提示信息直接判断出错原因；
2. 常见错误场景及排查方向：
  - 语法错误：检查运算符与子查询的组合格式是否符合 4.1 节定义，是否存在括号缺失等问题；
  - 列数错误：ALL/ANY/SOME 子查询返回多列时，需确认子查询仅保留单列输出；
  - 类型不匹配：检查表达式与子查询结果集的数据类型是否兼容，需做类型转换时手动处理；
  - 场景不支持：检查是否在流计算、DDL/DML 等暂不支持的语句中使用该功能，需调整为查询语句实现；
  - Query policy 4 报错：需切换为其他支持的 query policy 执行查询。

## 12. 可观测性

可通过 explain 命令查看执行计划与性能。

## 13. 安装和卸载

无

## 14. 文档

1. 不需要修改企业版文档；
2. 需要修改官网文档，补充 ALL/ANY/SOME/EXISTS/NOT EXISTS 运算符与子查询结合的语法、使用示例及约束限制。

## 15. 参考文档

1. SQL 标准官方文档；
2. [需求说明：支持更多子查询](https://taosdata.feishu.cn/wiki/Gi6HwAWcIimFpjkpAhXcMdNYn3N)
3. TDengine 现有子查询功能相关文档。

## 16. 附录

无
