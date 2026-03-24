# Interval 窗口支持插值时间范围 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-07 | - | 0.1 | 张天毅 | 设计基本语法 |
| 2025-01-09 | - | 1.0 | 张天毅 | 完善行为说明，提交review |
| 2025-01-13 | - | 1.1 | 张天毅 | 增加4.4节，描述伪列行为 |
| 2025-01-15 | - | 1.2 | 张天毅 | 更新语法，更新参数取值范围 |
| 2025-01-28 | - | 1.3 | 张天毅 | 更新surrounding time interval支持的时间单位 |
| 2025-02-03 | 2025-02-03 | 1.4 | 张天毅 | 更新流计算中的具体行为 |

## 2. 背景

Inteval 窗口需要支持插值时间范围，限制插值时的数据探索范围。

## 3. 定义

- `fill`：名词指`fill`子句，动词指“使用有效值填充无效值”，其功能为：通过在其模式参数指定的数据探索范围中寻找有效值，对目标时间点的none/null等无效数据进行填充
- 数据探索范围：指对某个时间点进行填充时，包含该点在内的一段时间范围，只可以在该范围内寻找有效值进行填充
- **限制**数据探索范围：对数据探索范围进行约束的功能，即该文档所要实现的功能

## 4. 行为说明

#### 4.0.1 现状

目前，interp 算子可以通过在`range`子句中指定时间长度形成时间范围，配合`fill`中指定`value`的方式实现：对于插值时间点，仅在生成的时间范围中寻找有效值，未找到则使用`value`填充。例如，
`select _irowts, interp(v) from tt range("2026-01-01", 1d) fill(prev, 100)`这个语句，在对 "2026-01-01" 时间点进行插值时，只会在 ["2025-12-31", "2026-01-01"] 这个区间内寻找有效值，未找到则使用100填充。
总结来说，一方面 interp 算子已经拥有了这个功能，另一方面已经存在`range(..., time_interval) fill(prev/next/near, fill_value)`这种语法。在为 interval 窗口设计这个功能时，需要考虑与现存语法和功能的兼容和统一。

#### 4.0.2 `fill`子句新参数和新行为

```sql {wrap}
interp_clause:
  RANGE(ts_val [, ts_val] [, surrounding_time_val]) EVERY(every_val) FILL(fill_mod_and_val) SURROUND(surrounding_time_interval, fill_values)

fill_mod_and_val:
   NONE
 | {NULL|NULL_F}
 | {VALUE|VALUE_F}, fill_values
 | {PREV|NEXT|NEAR|LINEAR} [, surrounding_time_interval, fill_values]
 | LINEAR
 
 
```

修改后，interp 算子中~~和 interval 窗口中，~~`fill`子句使用`prev/next/``near/``~~linear~~`模式进行插值时，以及 interval 窗口中`fill`子句使用`prev/next`模式进行插值时——~~可以填写~~`~~surrounding_time_interval~~`~~和~~`~~fill_values~~`~~参数~~配合`surround`子句实现**限制**数据探索范围的功能，缺省则不对探索范围进行限制。
`range`子句保留`surrounding_time_interval`参数，`fill`子句也保留`fill_values`参数以保证兼容性；但官网不会再提及这一用法。

#### 4.0.3 对新参数的特殊说明

##### 4.0.3.1 surrounding_time_interval（简称 sti）

- 数值需要大于 0，interval 子句中数值需大于等于 interval。
- 理论上可以支持任意长度的时间。不支持年（y）和月（n）作为时间单位。
- 在 interp 算子中，`fill`直接探索表的**原始**数据，如果要对时间点 x 进行`prev`模式插值，那么可以探索 [ x - sti, x ] 时间范围内的数据；如果进行`near`模式插值，那将探索 [ x - sti, x + sti] 时间范围内的数据。这符合用户的直观理解。
- 在 interval 窗口中，`fill` 的对象为窗口聚合数据，相比原始数据更加离散，此时 sti 值的作用受到 interval 窗口大小（简称 w）的影响，实际能向前或向后探索的窗口数据条数符合公式 <equation>\left\lfloor\frac{sti}{w}\right\rfloor
</equation>。例如，interval 窗口大小为 1h，sti 为 80min，此时公式结果为 1，即会探索一条数据；若 sti 小于 1h，如 3599s，此时公式结果为 0，即不进行任何探索。这与对原始数据进行扩展探索有所区别，这点会在文档中举例说明，帮助客户正确理解。
- Sti 不会破坏 `where` 条件，探索数据将同时满足这两个限制。

##### 4.0.3.2 fill_values

与`fill(value)`填充模式类似，默认值参数的数目必须与`select_list`元素数目一致，且必须能正确解析为对应列数据类型。

#### 4.0.4 `_isfilled` 和 `_irowts_origin` 伪列的行为

使用 interp 算子时可以使用`_isfilled`和`_irowts_origin`伪列，分别表示结果值是否来源于插值和插值所引用的原始数据的时间戳。当因为`surrounding_time_interval`的限制而使用`fill_values`作为结果时，`_isfilled`为 true，`_irowts_origin`为 NULL。

#### 4.0.5 流计算中的行为

在流计算中，触发窗口中的每次计算都是独立的。也就是说，尽管前一触发窗口中有有效数据，后一个触发窗口中的时间窗口无法通过`fill(prev)`的方式去引用这些数据来填充。每个触发窗口，仅仅使用计算子句约束下能够读取到的数据进行计算，也只能使用这些数据进行填充或插值操作。

## 5. 性能

预期衰减程度小于 5%

## 6. 安全

不涉及

## 7. 兼容性

新增语法扩展了 Fill 字句的关键字，不影响已有语法的使用，不涉及兼容性问题。

## 8. 运维

不涉及

## 9. 使用场景

不涉及

## 10. 约束和限制

不涉及

## 11. 常见错误和排查

不涉及

## 12. 可观测性

不涉及

## 13. 安装和卸载

描述对安装和卸载脚本有什么要求才能够使用户能够顺畅地使用本特性/优化

## 14. 文档

需要修改官网文档

## 15. 参考文档

[Interp 函数支持插值时间范围 FS](https://taosdata.feishu.cn/wiki/Vojlwl1KJirmE0kR55oc2FeLnnJ)
[Interval 窗口支持插值时间范围 RS](https://taosdata.feishu.cn/wiki/DXl3wgGuqiqI0okPyRbcGjAOn1f)

## 16. 附录
