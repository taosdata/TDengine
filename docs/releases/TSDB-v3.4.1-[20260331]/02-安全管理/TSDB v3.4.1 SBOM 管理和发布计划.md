# TSDB v3.4.1 SBOM 管理和发布计划

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-21 | 2026-01-21 | 1.0 | 霍琳贺 | 初始版本 |

## 2. 文档概述

### 2.1 目的

本文档旨在明确 TDengine TSDB v3.4.1 版本的软件物料清单（SBOM）管理策略、生成计划、发布方式及维护流程，确保产品软件成分的透明化和可追溯性，满足客户供应链安全审计与合规要求。

### 2.2 适用范围

本计划适用于 TDengine TSDB v3.4.1 所有正式发布组件，包括核心引擎、客户端驱动、管理工具及容器镜像。

### 2.3 依据文件

1. 《[软件物料清单（SBOM）管理规范](../../../security-docs/14-软件物料清单（SBOM）管理规范.md)》
2. 《[SBOM 集成指南](../../../security-docs/15-SBOM-集成指南.md)》
3. 《[软件供应链安全管理制度](../../../security-docs/04-软件供应链安全管理制度.md)》

## 3. 组件清单与 SBOM 覆盖范围

### 3.1 核心组件

| 组件 | 仓库 | 主要语言 | SBOM 工具 | SBOM 文件名 |
| --- | --- | --- | --- | --- |
| TDengine Server | taosdata/TDengine | C/C++ | syft, conan | `tsdb.cdx.json` / `tsdb.spdx.json` |
| taosAdapter | taosdata/taosadapter | Go | syft | `taosadapter.cdx.json` |
| taosKeeper | taosdata/taoskeeper | Go | syft | `taoskeeper.cdx.json` |
| taosX | taosdata/taosx | Rust | cargo-sbom | `taosx.cdx.json` |
| taosExplorer | taosdata/explorer | Node.js/Go | cyclonedx-npm, syft | `taosexplorer.cdx.json` |

### 3.2 客户端连接器

| 组件 | 主要语言 | SBOM 工具 | SBOM 文件名 |
| --- | --- | --- | --- |
| taos-connector-rust | Rust | cargo-sbom | `taos-connector-rust.cdx.json` |
| taos-connector-python | Python | cyclonedx-python | `taos-connector-python.cdx.json` |
| taos-connector-jdbc | Java | cyclonedx-maven-plugin | `taos-connector-jdbc.cdx.json` |
| taos-connector-node | Node.js | cyclonedx-npm | `taos-connector-node.cdx.json` |
| taos-connector-dotnet | C# | cyclonedx-dotnet | `taos-connector-dotnet.cdx.json` |
| taos-connector-go | Go | syft | `taos-connector-go.cdx.json` |

## 4. SBOM 格式与标准

### 4.1 输出格式

- **主推格式**：CycloneDX 1.5 JSON（`.cdx.json`）
- **兼容格式**：SPDX 2.3 JSON（`.spdx.json`），符合 ISO/IEC 5962:2021

### 4.2 NTIA 最低要素

所有 SBOM 必须包含以下要素：

| 要素 | 说明 |
| --- | --- |
| 供应商名称 | `Organization: Taos Data Inc.` |
| 组件名称 | 使用规范名称（如 `TDengine TSDB Server`） |
| 版本标识符 | 语义化版本（如 `3.4.1.0`） |
| 唯一标识符 | PURL 格式（如 `pkg:github/taosdata/TDengine@3.4.1.0`） |
| 依赖关系 | 直接依赖和传递依赖 |
| SBOM 创建者 | `Tool: <工具名-版本>, Organization: Taos Data Inc.` |
| 创建时间戳 | ISO 8601 格式，UTC 时区 |
| 哈希值 | SHA256 或 SHA512 |

### 4.3 文件命名规范

- 单组件：`<component>.<format>.json`（如 `tsdb.cdx.json`）
- 版本化：`<component>-<version>.<format>.json`（如 `tsdb-3.4.1.0.cdx.json`）
- 聚合 SBOM：`tdengine-tsdb-<edition>-<version>-<arch>.<format>.json`

## 5. 生成计划

### 5.1 生成时机

| 阶段 | 时间 | SBOM 活动 | 责任人 |
| --- | --- | --- | --- |
| 开发阶段 | 2026-02-01 ~ 03-31 | 各组件 CI/CD 集成 SBOM 自动生成 | 平台部 |
| 测试阶段 | 2026-04-01 ~ 04-20 | 验证 SBOM 准确性，与实际发布包对比 | 安全团队 |
| 发布阶段 | 2026-04-20 ~ 04-25 | 生成正式版 SBOM，合并聚合 SBOM | 安全团队 + 平台部 |

### 5.2 CI/CD 集成方案

1. **构建阶段自动生成**：在各组件发布流水线的打包步骤后，自动调用 SBOM 生成工具
2. **质量门禁**：SBOM 生成失败或不符合 NTIA 最低要素将阻断发布流程
3. **制品库存储**：SBOM 文件随发布包一同上传至制品库

### 5.3 各语言生成方式

#### Rust 项目（taosx, taos-connector-rust）

```bash
cargo sbom --output-format cyclone_dx_json_1_4 > <component>.cdx.json
```

#### Go 项目（taosAdapter, taosKeeper）

```bash
syft dir:. -o cyclonedx-json=<component>.cdx.json
syft dir:. -o spdx-json=<component>.spdx.json
```

#### C/C++ 项目（TDengine Server）

```bash
syft dir:. -o cyclonedx-json=tsdb.cdx.json
```

#### Java 项目（taos-connector-jdbc）

```xml
<!-- pom.xml 中添加 cyclonedx-maven-plugin -->
mvn org.cyclonedx:cyclonedx-maven-plugin:makeAggregateBom
```

#### Python 项目（taos-connector-python）

```bash
cyclonedx-py requirements -o <component>.cdx.json --format json
```

#### Node.js 项目（taos-connector-node, Explorer 前端）

```bash
cyclonedx-npm --output-format json --output-file <component>.cdx.json
```

#### C# 项目（taos-connector-dotnet）

```bash
dotnet CycloneDX <project>.csproj -o . -f <component>.cdx.json -j
```

## 6. 发布与交付计划

### 6.1 内部发布

- SBOM 文件存储于制品库，路径：`/TDengine/3.4/3.4.1.0/<edition>/<package-name>.<format>.json`
- 与发布包哈希值关联存储，确保可验证性

### 6.2 对外交付

| 客户类型 | 交付方式 | 格式 |
| --- | --- | --- |
| 社区版用户 | GitHub Releases 页面下载 | CycloneDX + SPDX |
| 企业版客户 | TDengine 下载中心 | CycloneDX + SPDX |
| VIP 客户 | 随交付物一同提供 | 按合同要求 |

### 6.3 聚合 SBOM

在正式发布前，将各组件 SBOM 合并生成聚合 SBOM：

```bash
cyclonedx merge \
  --input-files tsdb.cdx.json taosadapter.cdx.json taosx.cdx.json taosexplorer.cdx.json \
  --output-file tdengine-tsdb-release-3.4.1.0-linux-x64.cdx.json
```

## 7. 漏洞响应与 VEX

### 7.1 漏洞监控

- 安全团队通过 Dependency Track 持续监控 SBOM 中组件的已知漏洞
- 新漏洞发现后 2 小时内定位受影响的产品版本

### 7.2 VEX 声明

对于影响 SBOM 中组件的漏洞，根据实际可利用性发布 VEX 声明：

- `not_affected`：组件存在但不受影响
- `affected`：受影响且已提供修复版本
- `under_investigation`：正在分析中

## 8. 质量保证

### 8.1 SBOM 验证检查项

| 检查项 | 标准 | 验证方法 |
| --- | --- | --- |
| 格式合规 | 通过 CycloneDX/SPDX 校验 | `cyclonedx validate --input-file <file>` |
| 要素完整 | 包含所有 NTIA 最低要素 | 自动化脚本检查 |
| 内容准确 | SBOM 内容与实际发布包成分一致 | 人工抽查 + 自动对比 |
| 版本一致 | SBOM 版本号与发布版本一致 | 自动化检查 |

### 8.2 审计频率

- 每个正式版本发布前进行 SBOM 合规性审查
- 每季度进行 SBOM 内容准确性抽查审计

## 9. 里程碑

| 里程碑 | 目标日期 | 交付物 | 负责人 |
| --- | --- | --- | --- |
| CI/CD SBOM 集成完成 | 2026-02-28 | 各组件 CI 配置 | 平台部 |
| 组件 SBOM 首次生成 | 2026-03-15 | 各组件 `.cdx.json` 文件 | 各组件负责人 |
| SBOM 准确性验证 | 2026-04-15 | 验证报告 | 安全团队 |
| 聚合 SBOM 生成 | 2026-04-22 | 聚合 `.cdx.json` + `.spdx.json` | 安全团队 |
| SBOM 随版本发布 | 2026-04-25 | 制品库 + GitHub/下载中心 | 平台部 |
