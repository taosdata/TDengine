# TSDB v3.4.1 SCA 扫描报告

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-15 | 2026-04-15 | 1.0 | 霍琳贺 | 初始版本 |

## 2. 报告概述

### 2.1 目的

记录 TDengine TSDB v3.4.1 第三方组件成分分析（SCA）的扫描结果和处置情况，符合《工具扫描指南》和《软件供应链安全管理制度》要求。

### 2.2 依据

- 《工具扫描指南》
- 《软件供应链安全管理制度》
- 《TSDB v3.4.1 安全管理计划》第 5.3.3 节

### 2.3 扫描工具

| 工具 | 版本 | 扫描对象 | 扫描方式 |
| --- | --- | --- | --- |
| 棱镜七彩 SCA | <!-- TODO --> | 全部组件第三方依赖 | CI/CD 集成 |
| Trivy | <!-- TODO --> | 容器镜像 | CI/CD 集成 |
| cargo-deny | <!-- TODO --> | Rust 依赖 | CI/CD 集成 |
| govulncheck | <!-- TODO --> | Go 依赖 | CI/CD 集成 |
| pip-audit | <!-- TODO --> | Python 依赖 | CI/CD 集成 |
| audit-ci | <!-- TODO --> | Node.js 依赖 | CI/CD 集成 |

## 3. 扫描范围

### 3.1 扫描组件清单

| 组件 | 语言 | 依赖管理 | 直接依赖数 | 传递依赖数 |
| --- | --- | --- | --- | --- |
| TDengine Server | C/C++ | Conan | <!-- TODO --> | <!-- TODO --> |
| taosAdapter | Go | go.mod | <!-- TODO --> | <!-- TODO --> |
| taosX | Rust | Cargo.toml | <!-- TODO --> | <!-- TODO --> |
| Explorer | Go/Node.js | go.mod/package.json | <!-- TODO --> | <!-- TODO --> |
| taos-connector-jdbc | Java | Maven | <!-- TODO --> | <!-- TODO --> |
| taos-connector-python | Python | requirements.txt | <!-- TODO --> | <!-- TODO --> |
| taos-connector-node | Node.js | package.json | <!-- TODO --> | <!-- TODO --> |
| taos-connector-dotnet | C# | NuGet | <!-- TODO --> | <!-- TODO --> |
| taos-connector-rust | Rust | Cargo.toml | <!-- TODO --> | <!-- TODO --> |
| taos-connector-go | Go | go.mod | <!-- TODO --> | <!-- TODO --> |

## 4. 漏洞扫描结果

### 4.1 结果汇总

| 严重等级 | 发现数量 | 已修复 | 不受影响 | 待修复 | 接受风险 |
| --- | --- | --- | --- | --- | --- |
| 严重 (CVSS ≥ 9.0) | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> |
| 高危 (CVSS 7.0-8.9) | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> |
| 中危 (CVSS 4.0-6.9) | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> |
| 低危 (CVSS < 4.0) | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> |

### 4.2 质量门禁达标情况

| 门禁标准 | 目标 | 实际 | 达标 |
| --- | --- | --- | --- |
| 高危漏洞 | 0 个 | <!-- TODO --> | <!-- TODO --> |
| 中危漏洞 | 有修复计划 | <!-- TODO --> | <!-- TODO --> |

### 4.3 按组件分布

| 组件 | 严重 | 高危 | 中危 | 低危 | 合计 |
| --- | --- | --- | --- | --- | --- |
| <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> |

## 5. 许可证分析

### 5.1 许可证分布

| 许可证类型 | 组件数 | 风险等级 | 说明 |
| --- | --- | --- | --- |
| MIT | <!-- TODO --> | 低 | 宽松许可 |
| Apache-2.0 | <!-- TODO --> | 低 | 宽松许可 |
| BSD-3-Clause | <!-- TODO --> | 低 | 宽松许可 |
| GPL-2.0/3.0 | <!-- TODO --> | 高 | 传染性许可，需评估 |
| LGPL-2.1/3.0 | <!-- TODO --> | 中 | 条件性传染 |
| 其他/未知 | <!-- TODO --> | 需评估 | 人工确认 |

### 5.2 许可证合规结论

<!-- TODO: 填写许可证合规性结论 -->

## 6. 组件升级修复计划

| 组件 | 当前版本 | 漏洞 CVE | 目标版本 | 责任人 | 计划日期 | 状态 |
| --- | --- | --- | --- | --- | --- | --- |
| <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> | <!-- TODO --> |

## 7. 结论与建议

<!-- TODO: 填写整体 SCA 扫描结论和改进建议 -->
