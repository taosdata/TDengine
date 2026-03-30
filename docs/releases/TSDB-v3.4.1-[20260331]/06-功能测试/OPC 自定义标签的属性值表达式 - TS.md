# OPC 自定义标签的属性值表达式 - TS

## 1. 测试目标

本文档定义 OPC 自定义标签 `{Attr#XY}` 属性值字符替换功能的测试策略、测试用例和验收标准。

## 2. 变更历史

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026/3/17 | 2026/3/17 | 1.0 | @杨志宇 | 初始版本 |

## 3. 测试范围

### 3.1 **在范围内**

- `replace_attr_with_transform()` 核心替换函数
- `extra_custom_tags()` 端到端替换流程
- 四个支持属性：BrowseName、DisplayName、Description、Path
- DSN 参数配置解析
- 空值 / 边界条件处理
- 与普通 `{Attr}` 占位符的组合使用

### 3.2 **不在范围内**

- `{id#XY}` 点位 ID 占位符（已有独立测试覆盖）
- 子表名 / 超级表名占位符替换
- 前端 UI 渲染（需人工验证）
- OPC UA / DA 协议层的数据采集

## 4. 参考文档

- 需求规格说明书 (RS): 
- 概要设计说明书 (FS): 

## 5. 测试结论

测试通过✅ 。功能、易用性等符合预期。

## 6. 测试环境

- **操作系统**: Ubuntu 22.04 LTS
- **Rust 版本**: 1.75+
- **TDengine 版本**: 3.3.x
- **测试工具**: cargo test, cargo check

## 7. 功能测试

### 7.1 **基本字符替换**

#### 7.1.1 **测试要点**

验证 `{Attr#XY}` 对四个支持属性的基本替换行为，包括字符替换和首尾修剪。

#### 7.1.2 **用例列表**

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
| F-01 | DisplayName 下划线转点号 | Pattern=`{DisplayName#_.}`，DisplayName=`zs_p1_unit1_float`，期望结果 `zs.p1.unit1.float` | 通过✅ |
| F-02 | BrowseName 连字符转点号 | Pattern=`{BrowseName#-.}`，BrowseName=`zs-p1-unit1`，期望结果 `zs.p1.unit1` | 通过✅ |
| F-03 | Path 斜杠转下划线 | Pattern=`{Path#/_}`，Path=`/Objects/Plant/Area1/`，期望结果 `Objects_Plant_Area1` | 通过✅ |
| F-04 | DisplayName 点号转斜杠 | Pattern=`{DisplayName#./}`，DisplayName=`.Device.Type.Tag.`，期望结果 `Device/Type/Tag` | 通过✅ |
| F-05 | Description 替换 | Pattern=`{Description#-_}`，Description=`一号-车间-温度`，期望结果 `一号_车间_温度` | 通过✅ |

### 7.2 **组合使用**

#### 7.2.1 **测试要点**

验证 `{Attr#XY}` 与静态文本、普通 `{Attr}` 占位符的组合使用。

#### 7.2.2 **用例列表**

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
| C-01 | 前后缀静态文本 | Pattern=`prefix_{DisplayName#_.}_suffix`，DisplayName=`a_b_c`，期望结果 `prefix_a.b.c_suffix` | 通过✅ |
| C-02 | 混合 `{Attr#XY}` 和 `{Attr}` | Pattern=`{BrowseName#-.}({Description})`，BrowseName=`a-b`，Description=`desc`，期望结果 `a.b(desc)` | 通过✅ |
| C-03 | 多标签同时替换 | 同一 PointConfig 包含多个 tag_values，各自独立替换，互不影响 | 通过✅ |
| C-04 | 静态值不受影响 | tag_value=`fixed_value`（无占位符），期望结果 `fixed_value` 不变 | 通过✅ |

### 7.3 **替换优先级**

#### 7.3.1 **测试要点**

验证 `{Attr#XY}` 优先于 `{Attr}` 处理。

#### 7.3.2 **用例列表**

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
| P-01 | 同属性同时使用两种占位符 | Pattern=`{DisplayName#_.}-{DisplayName}`，DisplayName=`a_b`，期望结果 `a.b-a_b`（`#XY` 先替换，普通占位符保留原值） | 通过✅ |

### 7.4 **非法占位符**

#### 7.4.1 **测试要点**

验证不符合 `{Attr#XY}` 语法的占位符保留原文不替换。

#### 7.4.2 **用例列表**

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
| N-01 | 缺少 XY | Pattern=`{DisplayName#}`，期望保留原文 `{DisplayName#}` | 通过✅ |
| N-02 | 不支持的属性 | Pattern=`{NodeClass#_.}`，期望保留原文 `{NodeClass#_.}` | 通过✅ |

## 8. 易用性测试

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
| U-01 | OPC UA 中文界面 description | 自定义标签字段的 description 中包含 `{Attr#XY}` 语法说明和示例 | 通过✅ |
| U-02 | OPC UA 英文界面 description | 同上，英文版本 | 通过✅ |
| U-03 | KingHistorian 中文界面 description | 同上，KingHistorian 中文版本 | 通过✅ |
| U-04 | KingHistorian 英文界面 description | 同上，KingHistorian 英文版本 | 通过✅ |
| U-05 | 语法说明准确性 | description 中的示例与实际替换行为一致 | 通过✅ |

## 9. 长期稳定性测试

无。本功能为无状态的字符串替换操作，不涉及长期运行场景。

## 10. 性能测试

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
| PF-01 | 单次替换耗时 | 单个 `{Attr#XY}` 替换操作耗时应在微秒级别 | 通过✅ |
| PF-02 | 批量点位替换 | 1000 个点位各含 5 个自定义标签，全部使用 `{Attr#XY}`，总耗时应在毫秒级别 | 通过✅ |

## 11. 安全测试

无。本功能仅涉及内存中的字符串替换，不涉及权限、敏感信息或外部输入执行。

## 12. 兼容性测试

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
| CP-01 | 现有 `{Attr}` 占位符不受影响 | 升级后，仅使用 `{BrowseName}` 等普通占位符的任务行为不变 | 通过✅ |
| CP-02 | 现有 `{id#XY}` 占位符不受影响 | 升级后，使用 `{id#/.}` 等点位 ID 占位符的任务行为不变 | 通过✅ |
| CP-03 | 子表名/超级表名占位符不受影响 | 升级后，tbname_expression 和 stable_expression 中的占位符行为不变 | 通过✅ |
| CP-04 | 旧版本任务继续执行 | 升级安装后，旧版本创建的不含 `{Attr#XY}` 的任务能继续正常执行 | 通过✅ |

## 13. 代码质量测试

### 13.1 编译测试

```bash

## 14. 单独编译 source-parquet 模块

cargo check -p source-pspace

## 15. 结果: ✅ 通过

## 16. 编译 task 模块（包含集成）

cargo check -p taosx-task

## 17. 结果: ✅ 通过

## 18. 构建 source-parquet 模块

cargo build -p source-pspace

## 19. 结果: ✅ 通过

```

### 19.1 代码规范

- ✅ 遵循 Rust 代码规范
- ✅ 使用 workspace lints 配置
- ✅ 与现有代码风格保持一致
- ✅ 错误处理使用 anyhow/Result
- ✅ 日志使用 tracing 框架

## 20. 已知问题和限制

- 仅支持单字符替换，不支持多字符或正则表达式替换
- 仅支持 BrowseName、DisplayName、Description、Path 四个属性，不支持 NodeClass、ParentId
- 不支持链式替换（同一属性值先替换 X→Y 再替换 A→B），但可通过配置多个标签分别实现

### 20.1 已知 Bug

无已知 Bug。

## 21. 测试总结

### 21.1 测试完成度

- 代码实现: ✅ 100% 完成
- 编译测试: ✅ 100% 通过
- 功能测试: ✅ 100% 通过
- 兼容性测试: ✅ 100% 通过

### 21.2 后续工作

无

### 21.3 风险评估

无
