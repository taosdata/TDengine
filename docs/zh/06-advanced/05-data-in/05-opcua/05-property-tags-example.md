---
title: "OPC UA Property 到 TAG 示例（以 limit 为例）"
sidebar_label: "Property 到 TAG 示例"
---

本文说明如何让 OPC UA 点位上的 Property（属性）随采集任务写入 TDengine 子表对应的超级表 TAG。
文中以 `limit` 为例，但流程同样适用于其他 Property 名称。

## 适用场景

- OPC UA 服务器在 Variable 节点下定义了业务属性（如 `limit`、`EngineeringUnits`、`upperBound` 等）
- 希望 taosX 在导入时把这些属性自动映射为 TAG 列

## 关键原则

1. 属性必须是目标 Variable 的 **Property**（`HasProperty` 关系），而不是独立数据点位。
2. Explorer 任务建议使用：
   - **节点类型**：`all`
   - **根节点 ID（可选）**：可设置到包含目标 Variable 的父层级（用于收敛扫描范围）；不填写时通常从服务端默认根开始浏览
3. 不要让 `custom_tags` 中的标签名与 Property 名重名（重名会冲突）。

## 示例：`limit` 属性导入为 TAG

### OPC UA 侧示例结构

```text
Objects
└── Plant
    └── Area1
        └── Sensor01
            └── Temperature                              (Variable)
                ├── limit = "0~100"                      (Property)
                └── EngineeringUnits = "degree Celsius"  (Property)
```

### Explorer 任务配置建议

在“选择数据点位”模式下，建议：

- 根节点 ID（可选）：例如 `ns=2;i=3`（按实际地址空间填写；不同服务端可能不同）
- 节点类型：`all`
- 超级表名：`opc_limit_{type}`（仅示例）
- 子表名：`t_{ns}_{id#/_}`
- 时间戳列：`received_ts`

其余连接与认证按你的 OPC UA 实际环境填写。

## 验证是否生效

```sql
USE <目标数据库>;
SHOW STABLES;
DESCRIBE <超级表名>;
```

预期 `DESCRIBE` 中能看到与你服务端属性名一致的 TAG，例如：

- `limit` `VARCHAR(1024)` `TAG`
- `EngineeringUnits` `VARCHAR(1024)` `TAG`

并可进一步验证值：

```sql
SELECT tbname, `limit`, `EngineeringUnits`
FROM <超级表名>
LIMIT 5;
```

## 关于“limit 只是示例名”

`limit` 不是内置保留名，也不是唯一支持的名称。
只要 OPC UA 服务器按规范把属性建成 Variable 的 Property，其他名称（如 `high_limit`、`alarmLevel`、`unit`）也可以按同流程映射为 TAG。

## 常见问题

### 1) 只有 `custom_tags`，没有 Property TAG

优先检查：

- 是否设置了合适的根节点 ID（可选，但建议在点位较多时配置）
- 节点类型是否为 `all`
- OPC UA 端该属性是否真的是 Property（不是普通 Variable）

### 2) 修改配置后仍看不到新 TAG

如果超级表已按旧结构创建，旧表结构不会自动补新 TAG。
请使用新库/新表名重建任务，或清理旧表后重新运行。
