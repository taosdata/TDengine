# 概要设计说明书（Functional Spec）- TDengine Grafana Plugin v4.0.0

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-26 | 2026-03-26 | 4.0.0 | 佘彦杰 | 初始版本 - v4.0.0 重大变更发布 |

# 背景

TDengine Grafana Plugin 经过多个版本的演进，积累了一些历史遗留的功能和字段。随着 Grafana 生态的发展，部分功能已被 Grafana 原生能力覆盖，继续维护这些废弃功能增加了代码复杂度和维护成本。同时，现有架构从 `DataSourceApi` 到 `DataSourceWithBackend` 的迁移可以提供更好的性能和安全性。

本次 v4.0.0 版本是一个重大变更版本（Breaking Changes Release），旨在：
1. **清理技术债务**：移除废弃的查询字段和功能
2. **架构升级**：从 `DataSourceApi` 迁移到 `DataSourceWithBackend`
3. **简化用户体验**：优化 QueryEditor UI，聚焦 SQL-First 工作流
4. **提升代码质量**：增强测试覆盖率，修复已知 bug
5. **安全合规**：修复已知的安全漏洞（CVE）

# 定义

- **Breaking Changes**: 不向后兼容的变更，升级后需要用户手动调整现有配置或查询
- **DataSourceApi**: Grafana 数据源的基础抽象类
- **DataSourceWithBackend**: Grafana 数据源的高级抽象类，支持后端代理模式
- **SQL Macro**: Grafana 模板变量系统中用于 SQL 查询的宏替换（如 `$__timeFilter`）
- **TDinsight**: TDengine 官方监控仪表板
- **DataFrame**: Grafana 的数据表示格式
- **LongToWide Conversion**: 长格式到宽格式的数据转换

# 行为说明

## 1. 移除废弃的查询字段

### 1.1 移除的字段列表

以下字段已从 `Query` 接口中移除：

| 字段名 | 原功能 | 替代方案 |
|--------|--------|----------|
| `alias` | 查询结果别名 | 使用 SQL `AS` 子句或 Grafana 字段显示名称配置 |
| `colNameFormatStr` | 列名格式化字符串 | 使用 Grafana 字段显示名称功能 (`displayName`) |
| `colNameToGroup` | 分组列名 | 使用 SQL `GROUP BY` 搭配适当的列别名 |
| `timeShift` | 时间偏移（布尔值） | 使用 Grafana 面板选项中的时间范围覆盖 |
| `timeShiftPeriod` | 时间偏移周期数 | 使用 Grafana 面板选项中的时间范围覆盖 |
| `timeShiftUnit` | 时间偏移单位 | 使用 Grafana 面板选项中的时间范围覆盖 |

### 1.2 迁移示例

**旧版本（v3.x）查询配置**:
```json
{
  "refId": "A",
  "alias": "My Metric",
  "colNameFormatStr": "location_{{value}}",
  "colNameToGroup": "location",
  "formatType": "Time series",
  "sql": "SELECT value FROM sensors"
}
```

**新版本（v4.0.0）查询配置**:
```json
{
  "refId": "A",
  "formatType": "Time series",
  "sql": "SELECT value AS my_metric FROM sensors"
}
```

对于列名格式化，使用 Grafana 的字段配置：
```json
{
  "fieldConfig": {
    "defaults": {
      "displayName": "location_${__field.labels.location}"
    }
  }
}
```

## 2. QueryEditor UI 简化

### 2.1 移除的 UI 输入

以下 UI 输入已从 QueryEditor 中移除：
- "Alias By" 输入框
- "Group By Column(s)" 输入框
- "Group By Format" 输入框

### 2.2 新的 SQL-First 工作流

QueryEditor 现在聚焦于 SQL 查询编写，用户需要直接在 SQL 中实现：
- 使用 `AS` 子句定义别名
- 使用 `GROUP BY` 或 `PARTITION BY` 子句进行分组
- 使用 Grafana 原生字段配置进行显示名称定制

## 3. 增强的 SQL 宏支持

### 3.1 新增宏

| 宏名称 | 替换内容 | 示例 |
|--------|----------|------|
| `$__timeFrom` | 时间范围起始点（RFC3339 格式，带引号） | `'2026-03-01T00:00:00Z'` |
| `$__timeTo` | 时间范围结束点（RFC3339 格式，带引号） | `'2026-03-26T23:59:59Z'` |
| `$__timeFilter(column)` | 时间范围过滤条件（SQL WHERE 子句） | `ts >= '2026-03-01T00:00:00Z' AND ts < '2026-03-26T23:59:59Z'` |

### 3.2 SQL 宏使用示例

```sql
-- 使用 $__timeFilter 宏
SELECT tbname, AVG(current) AS avg_current
FROM sensors
WHERE $__timeFilter(ts)
GROUP BY tbname;

-- 使用 $__timeFrom 和 $__timeTo 宏
SELECT COUNT(*) AS total_records
FROM sensors
WHERE ts >= $__timeFrom AND ts < $__timeTo;

-- 结合 Grafana 变量
SELECT *
FROM ${table_name}
WHERE $__timeFilter(ts)
  AND location = '$location';
```

## 4. 架构迁移：DataSourceWithBackend

### 4.1 变更内容

- **前端**: `datasource.ts` 从 `DataSourceApi` 迁移到 `DataSourceWithBackend`
- **后端**: 增强 Go 后端的数据处理逻辑，支持 LongToWide 转换
- **通信**: 前端查询通过后端代理访问 TDengine REST API

### 4.2 数据流变化

**旧版本（v3.x）**:
```
QueryEditor → datasource.query() → TDengine REST API → convertToDataFrame()
```

**新版本（v4.0.0）**:
```
QueryEditor → datasource.query() → Backend Plugin → TDengine REST API
→ Backend Data Processing → Frontend DataFrame Processing
```

## 5. 移除遗留仪表板

### 5.1 移除的仪表板

- **TDinsightV2.json**: 已被 TDinsightV3 替代
- **15146-tdengine-monitor-dashboard.json**: 功能已整合到 TDinsightV3 和 taosX 仪表板

### 5.2 推荐使用的仪表板

| 仪表板名称 | Grafana ID | 用途 |
|------------|------------|------|
| TDinsightV3 | 18180 | TDengine 数据库监控 |
| TDsmeters | 19910 | 智能电表示例 |
| taosX | 20631 | taosX 数据同步监控 |

## 6. 最低 Grafana 版本要求

- **旧版本**: Grafana >= 7.5.0
- **新版本**: Grafana >= 8.0.0

从 Grafana 8.0 开始支持更完善的后端插件 SDK 和字段配置能力。

## 7. 出错处理

### 7.1 升级后查询失败

**错误现象**: 升级到 v4.0.0 后，现有查询面板显示错误

**原因**: 查询配置包含已移除的字段

**解决方案**:
1. 编辑受影响的面板
2. 移除废弃字段，改用 SQL 或 Grafana 原生配置
3. 保存面板

### 7.2 Alert 规则失效

**错误现象**: Alert 规则触发异常

**原因**: Alert 规则查询使用了废弃字段

**解决方案**:
1. 导出现有 Alert 规则（Grafana UI → Alerting → Export）
2. 编辑 JSON 配置，移除废弃字段
3. 使用 Grafana Provisioning API 重新导入

# 性能

## 性能提升

1. **查询性能**: 通过后端代理模式，减少前端数据处理开销，预计查询响应时间降低 10-20%
2. **启动性能**: 移除废弃代码和仪表板，插件包体积减小约 15%，加载时间相应缩短
3. **内存占用**: 简化数据处理流程，预计降低内存占用 5-10%

## 无性能影响的场景

对于简单的时间序列查询（不使用废弃字段），性能表现与 v3.x 基本一致。

# 安全

## 安全修复

1. **CVE-2026-33186**: 升级 `google.golang.org/grpc` 至 v1.79.3，修复 gRPC 安全漏洞
2. **CVE-2026-24051**: 添加 `go.opentelemetry.io/otel/sdk` v1.40.0 的 replace 指令，修复 OpenTelemetry SDK 漏洞

## 安全加固

- 后端代理模式增强了数据源凭证的安全性，敏感信息（如密码、Token）仅存储在 Grafana 后端，不暴露给前端

# 兼容性

## Breaking Changes

| 变更类型 | 影响范围 | 必须这么做的理由 |
|----------|----------|------------------|
| 移除废弃查询字段 | 使用这些字段的仪表板和 Alert 规则 | 减少代码复杂度，与 Grafana 原生能力对齐 |
| QueryEditor 简化 | 使用 UI 配置分组/别名的用户 | 简化用户体验，避免重复配置入口 |
| 移除遗留仪表板 | 仍在使用 TDinsightV2 的用户 | TDinsightV2 已废弃多年，TDinsightV3 功能更全面 |
| 提升最低 Grafana 版本 | Grafana 7.5-7.x 用户 | Grafana 8.0 提供更好的插件 SDK 支持 |

## 向前兼容

v4.0.0 与 TDengine 2.x 和 3.x 保持兼容，无需升级 TDengine 服务端。

## 回滚方案

如果升级后遇到严重问题：
1. 卸载 v4.0.0 插件
2. 安装 v3.8.0 插件
3. 现有仪表板恢复正常（但仍需计划未来迁移）

# 运维

## 部署影响

无

## 客户支持

1. **迁移支持**: 提供详细的迁移指南（README.md）
2. **常见问题**: 在 GitHub Issues 收集和解答升级相关问题
3. **回滚指导**: 提供清晰的回滚步骤

# 使用场景

## Use Case 1: 时间序列监控查询

**场景描述**: 用户需要查询传感器数据，按设备分组显示

**旧版本（v3.x）**:
- 在 QueryEditor UI 填写 "Group By Column(s)" 为 `tbname`
- 在 "Alias By" 填写 `sensor_$tag_tbname`

**新版本（v4.0.0）**:
```sql
SELECT tbname, AVG(current) AS avg_current
FROM sensors
WHERE $__timeFilter(ts)
GROUP BY tbname;
```
配合 Grafana 字段配置：
```json
{
  "fieldConfig": {
    "defaults": {
      "displayName": "sensor_${__field.labels.tbname}"
    }
  }
}
```

## Use Case 2: 告警规则配置

**场景描述**: 配置 CPU 使用率超过 80% 的告警

**查询 SQL**:
```sql
SELECT AVG(cpu_usage) AS avg_cpu
FROM system_metrics
WHERE $__timeFilter(ts)
  AND host = '$host'
GROUP BY host;
```

**Alert 条件**:
- Reduce function: Last
- Math expression: `$A > 80`

## Use Case 3: 数据探索和调试

**场景描述**: 开发者需要快速验证查询结果

**操作步骤**:
1. 创建新面板
2. 选择 TDengine 数据源
3. 在 SQL 编辑器输入查询
4. 使用 `$__timeFilter(ts)` 自动应用时间范围
5. 点击 Run Query 查看结果

# 约束和限制

## 约束

1. **Grafana 版本**: 必须使用 Grafana 8.0 或更高版本
2. **TDengine 版本**: 建议使用 TDengine 3.x（2.x 也支持但部分功能受限）
3. **浏览器要求**: 现代浏览器（Chrome 90+, Firefox 88+, Safari 14+）

## 限制

1. **迁移工作量**: 对于大量使用废弃字段的仪表板，迁移工作量较大
2. **Grafana 7.x 支持**: v4.0.0 不再支持 Grafana 7.x，仍使用旧版 Grafana 的用户需继续使用 v3.8.0
3. **Alert 迁移**: Alert 规则无法自动迁移，需手动调整
4. **向后兼容**: v4.0.0 查询配置无法在 v3.x 中正确解析（如果需要回滚，需手动调整查询）

# 常见错误和排查

## 错误 1: 查询面板显示空白或错误

**错误信息**: `undefined field in query configuration`

**原因**: 查询配置包含已移除的字段（如 `alias`, `colNameToGroup`）

**排查步骤**:
1. 编辑面板 → Query 选项卡
2. 检查 JSON 模式（点击 "Query Inspector" → "JSON"）
3. 移除 `alias`, `colNameFormatStr`, `colNameToGroup`, `timeShift*` 等字段
4. 保存并重新运行查询

## 错误 2: Alert 规则不再触发

**错误信息**: Alert evaluation failed

**原因**: Alert 查询使用了废弃字段

**排查步骤**:
1. 进入 Alerting → Alert rules
2. 编辑受影响的规则
3. 检查查询配置，移除废弃字段
4. 测试规则并保存

## 错误 3: 时间宏替换不生效

**错误信息**: SQL syntax error near `$__timeFilter`

**原因**: TDengine REST API 不支持直接执行包含宏的 SQL

**解决方案**: 检查后端日志，确认后端插件正常运行。宏替换在后端完成，如果后端未启动或配置错误，宏不会被替换。

# 可观测性

## taos shell

无影响。taos shell 不依赖 Grafana 插件。

## taos Explorer

无影响。taos Explorer 是独立的 Web UI。

## TDinsight

**影响**: TDinsight V2 被移除，用户需迁移到 TDinsight V3

**行为变化**:
- 插件包中不再包含 TDinsightV2.json
- Grafana 数据源配置中不再显示 "Import TDinsightV2" 按钮
- 用户需手动从 Grafana 官网导入 TDinsightV3 (Dashboard ID: 18180)

**迁移步骤**:
1. 访问 https://grafana.com/grafana/dashboards/18180
2. 点击 "Copy ID to Clipboard"
3. 在 Grafana UI: Dashboards → New → Import
4. 粘贴 Dashboard ID 18180
5. 选择 TDengine 数据源
6. 点击 Import

# 安装和卸载

请参考 [官网文档](https://docs.taosdata.com/third-party/visual/grafana/#%E5%AE%89%E8%A3%85-grafana-plugin-%E5%B9%B6%E9%85%8D%E7%BD%AE%E6%95%B0%E6%8D%AE%E6%BA%90)

# 文档

## 企业版文档

**是否需要修改**: 是

**修改内容**:
1. 更新插件版本号至 v4.0.0
2. 移除 TDinsightV2 相关文档
3. 添加迁移指南章节
4. 更新 SQL 宏示例
5. 更新截图（QueryEditor UI 变化）

## 官网文档

**是否需要修改**: 是

**修改内容**:
1. 发布 v4.0.0 Breaking Changes 公告
2. 更新安装指南
3. 添加 FAQ: "如何从 v3.x 迁移到 v4.0.0"
4. 更新 TDinsight 仪表板推荐（v3 instead of v2）

**文档 PR 截止日期**: 2026-03-25（产品发布前）

# 参考文档

1. [Grafana Plugin SDK - DataSourceWithBackend](https://grafana.com/docs/grafana/latest/developers/plugins/backend/)
2. [Grafana Time Range Variables](https://grafana.com/docs/grafana/latest/variables/variable-types/time-range/)
3. [TDengine Grafana Plugin GitHub](https://github.com/taosdata/grafanaplugin)
4. [CHANGELOG.md](../CHANGELOG.md) - 详细变更记录

# 附录

## 附录 A: Query 接口定义变化

**旧版本（v3.x）**:
```typescript
export interface Query extends DataQuery {
    alias?: string
    colNameFormatStr: string
    colNameToGroup: string
    formatType: string
    queryType: string
    sql: string
    timeShiftPeriod: number
    timeShiftUnit: string
    expression: string
}
```

**新版本（v4.0.0）**:
```typescript
export interface Query extends DataQuery {
    queryType?: string
    sql: string
    timeShiftPeriod?: number | string
    timeShiftUnit?: string
    expression?: string
}
```

## 附录 B: 后端数据处理流程

```
1. QueryDataHandler.HandleQuery()
   ↓
2. 解析查询参数（SQL, time range）
   ↓
3. 替换 SQL 宏（$__timeFrom, $__timeTo, $__timeFilter）
   ↓
4. 发送 REST API 请求到 TDengine
   ↓
5. 接收 JSON 响应
   ↓
6. convertRow(): 将 TDengine 数据格式转换为 DataFrame
   ↓
7. LongToWide 转换（如果需要）
   ↓
8. 返回 DataFrame 到前端
```

## 附录 C: 测试覆盖率提升

| 模块 | v3.x 覆盖率 | v4.0.0 覆盖率 | 新增测试用例数 |
|------|-------------|---------------|----------------|
| datasource.ts | ~30% | ~70% | 40+ |
| pkg/plugin/datasource.go | ~40% | ~75% | 60+ |
| SQL 宏替换 | 0% | 100% | 15+ |
| 数据转换（convertRow） | ~20% | ~90% | 30+ |

## 附录 D: 关键 Bug 修复

### Bug 1: Alert Tag 提取失败

**问题描述**: 当查询包含分组维度时，Alert 规则无法正确提取标签

**根因**: `convertRow()` 函数未正确处理分组列的标签提取逻辑

**修复方案**: 重构标签解析逻辑，支持带空格和特殊字符的维度值

**测试用例**:
```go
// pkg/plugin/datasource_test.go
func TestParseLabelWithSpaces(t *testing.T) {
    input := "label1=value with spaces,label2=value2"
    labels := parseLabels(input)
    assert.Equal(t, "value with spaces", labels["label1"])
    assert.Equal(t, "value2", labels["label2"])
}
```

### Bug 2: 时间宏替换精度问题

**问题描述**: `$__timeFrom` 和 `$__timeTo` 在某些时区下替换结果不一致

**根因**: 时间格式化使用了本地时区而非 UTC

**修复方案**: 统一使用 RFC3339 格式，强制 UTC 时区

**验证步骤**:
1. 设置 Grafana 时区为 Asia/Shanghai
2. 创建查询使用 `$__timeFrom` 宏
3. 验证替换后的时间戳为 UTC（Z 后缀）

---
