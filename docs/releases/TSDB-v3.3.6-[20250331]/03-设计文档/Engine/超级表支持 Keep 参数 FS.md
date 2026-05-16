# 超级表支持 Keep 参数 FS

## 1. 背景

TDengine当前支持数据库级别的数据保留策略（KEEP参数），但在某些场景下，不同超级表可能需要独立的数据保留策略。本特性允许在超级表级别设置KEEP参数，并在Compact操作时生效，实现更细粒度的数据生命周期管理。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/3/3 | 0.1 | 鲍之骁 | 初稿 |
| 2025/3/13 | 0.2 | 鲍之骁 | 修改产品行为超级表的 keep 参数不会对查询结果立即产生影响 |

## 3. 定义

- **KEEP参数**：表示数据文件保存的天数，缺省值为 3650，取值范围 [1, 365000]，且必须大于或等于 3 倍的 DURATION 参数值。数据库会自动删除保存时间超过 KEEP 值的数据从而释放存储空间。KEEP 可以使用加单位的表示形式，如 KEEP 100h、KEEP 10d 等，支持 m（分钟）、h（小时）和 d（天）三个单位。也可以不写单位，如 KEEP 50，此时默认单位为天。

## 4. 行为说明

### 4.1 **创建超级表**

**语法扩展**：
```sql
CREATE STABLE meters (ts TIMESTAMP, current FLOAT) 
TAGS (location BINARY(50)) 
KEEP 365d;
```

**约束**：
1. 子表继承超级表的`KEEP`，无法单独设置
---

### 4.2 **修改超级表**

**语法示例**：
```sql
ALTER STABLE meters KEEP 730d;
```

---

### 4.3 **Compact**** ****操作**

**行为变更**：
Compact 自动删除超级表内超`KEEP`的时序数据，语法不变
**示例**：
```sql
COMPACT DATABASE power;  -- 触发清理meters超级表的过期数据
```

## 5. 性能

~~超级表支持 keep 参数前，查询从 vnode 中获取keep；超级表支持 keep 参数后，查询需要从meta 中获取对应对应超级表的 keep ，增加了一个拉取meta的操作。拉取 meta 的操作在某些场景下可能会影响~~~~ TSBS 的查询性能。~~
修改了产品行为，目前的实现理论上不会影响 tsbs 性能。
~~TSBS  测试报告：~~
[超级表支持 Keep 参数查询性能对比](https://taosdata.feishu.cn/docx/VTHPdZ52Qo8LxsxGTVMcsansn4g)

## 6. 兼容性

不产生兼容性问题，通过代码实现规避。

## 7. 运维

1. 需注意，超级表的 keep 参数并不会直接影响数据实际存储，只有在 compact 时，才会真正做数据清理。
2. compact 前需要 flush ,否则可能不生效。

## 8. 使用场景

需要对超级表单独设置 keep 时间。

## 9. 约束和限制

1. 超级表 keep 参数不会立即影响查询结果，只有在 compact 完成后，数据才会被清理，并对查询不可见。
2. 允许插入超过超级表 keep 时间的数据,具体规则见以下示例：
定义三个时间，dbKeep2 对于数据库过期的时间，stableKeep 对于超级表过期的时间，keyTs 数据的时间。
keyTs < dbKeep2 :插入失败
dbKeep2  < keyTs < stableKeep :插入成功,compact后删除
keyTs>stableKeep:插入成功
1. compact 前必须进行 flush 否则可能不生效。
2. compact 之后，alter stable 的 keep 再 compact ,部分数据有可能无法被正确清理，这取决于对应的文件在上次 compact 之后是否有新的数据写入。

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

1. **官网文档**：
   - 更新CREATE STABLE及ALTER STABLE语法说明。
   - 新增KEEP参数说明章节，对比数据库与超级表的差异。

## 14. 参考文档

[超级表支持 Keep 参数 RS](https://taosdata.feishu.cn/wiki/R6BNwYURNi1y7AkEn5DcK2FQnDe)

## 15. 附录
