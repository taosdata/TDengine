# pSpace Points 模块设计

## 点位列表获取规则

获取要查询的点位 `List<Long>` 遵循以下优先级规则：

### 1. 用户通过 CSV 配置点位（优先）

- **条件**：`PointsConfig.pointIds`（TOML 字段 `point_ids`）不为 `null` 且不为空
- **行为**：直接使用 `PointsConfig.pointIds` 作为点位列表
- **场景**：用户通过 CSV 文件自行配置了需要采集的点位 ID

### 2. 通过节点查询点位（回退）

- **条件**：`PointsConfig.pointIds` 未设置（`null`）或为空列表
- **行为**：调用 `Points.getPoints(root, nameFilter, includeDataType, client)` 查询点位列表
- **参数来源**：
  - `root`：来自 `NodesConfig.root`（必填，根节点 ID）
  - `nameFilter`：来自 `PointsConfig.nameFilter`（TOML 字段 `name_filter`，可选，用于按名称过滤）
  - `includeDataType`：来自 `PointsConfig.includeDataType`（TOML 字段 `include_data_type`，默认 `false`）
- **场景**：用户通过 UI 选择数据节点，系统自动查询该节点下所有子点位

### 伪代码

```java
List<Long> tagIds;
if (pointsConfig != null && pointsConfig.getPointIds() != null && !pointsConfig.getPointIds().isEmpty()) {
    // 规则 1：用户通过 CSV 配置了点位
    tagIds = pointsConfig.getPointIds();
} else {
    // 规则 2：通过节点查询点位
    List<Point> points = Points.getPoints(root, nameFilter, includeDataType, client);
    tagIds = points.stream().map(Point::getId).collect(Collectors.toList());
}
```

## 相关代码

- 配置类：[PointsConfig.java](../../../src/main/java/com/taosdata/taosx/pspace/config/PointsConfig.java)
- 节点配置：[NodesConfig.java](../../../src/main/java/com/taosdata/taosx/pspace/config/NodesConfig.java)
- 点位查询：[Points.java](../../../src/main/java/com/taosdata/taosx/pspace/Points.java)
- 主配置：[Configuration.java](../../../src/main/java/com/taosdata/taosx/pspace/config/Configuration.java)

## TOML 配置示例

```toml
[nodes]
root = 12345

[points]
name_filter = "温度*"        # 可选，按名称过滤
include_data_type = true      # 可选，是否包含数据类型信息
point_ids = [100, 200, 300]   # 可选，如果设置则直接使用，忽略 name_filter 查询
```
