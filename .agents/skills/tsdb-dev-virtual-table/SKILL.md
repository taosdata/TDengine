---
name: tsdb-dev-virtual-table
description: "TDengine 虚拟表（Virtual Table）开发指南。覆盖虚拟表从 SQL 解析、计划生成、执行器到 DDL 的完整代码链路、核心数据结构和已有优化。适用于开发虚拟表新功能、调试虚拟表查询问题、理解虚拟表架构。触发关键词: virtual table, 虚拟表, vtable, virtualScan, 虚拟表开发, vtable scan, 虚拟表查询"
metadata:
  author: Jing Sima
  version: 1.0.0
  owner_team: engine
---

# TDengine 虚拟表（Virtual Table）开发指南

## When to Use

- 开发虚拟表相关新功能（新增列引用类型、扫描优化等）
- 调试虚拟表查询链路问题（解析错误、计划生成异常、执行器结果错误）
- 理解虚拟表与 Stream 的集成关系
- 修改虚拟表 DDL（CREATE/DROP VIRTUAL TABLE）逻辑
- 为虚拟表添加新的优化规则

## Prerequisites

- 已 clone TDinternal 仓库并可编译
- 熟悉 TDengine 查询引擎基本架构（Parser → Planner → Executor）
- 了解 TDengine 的超级表/子表/普通表模型

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-virtual-table version=1.0.0 author=Jing Sima`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
