# TDengine TSDB Monorepo

An open-source time-series and IoT data platform monorepo covering the TDengine core engine, internal and enterprise sources, adapters, tooling, and official language connectors.

- [Overview](#overview)
- [Monorepo Structure](#monorepo-structure)
- [Documentation](#documentation)
- [Key Subprojects](#key-subprojects)
- [Starting Point](#starting-point)

## Overview

This repository brings together the main TDengine TSDB codebase and the supporting components that are built, packaged, tested, and documented alongside it.

At the top level you will find:

- the **core engine** and community sources
- **internal / enterprise** sources used by the unified build
- **runtime components** such as taosAdapter, taosKeeper, taosX, and Explorer
- **official connectors** for .NET, Go, JDBC, Node.js, ODBC, Python, and Rust
- the product, release, design, and test documentation tree under `docs/`

This root README is intentionally brief. It is meant to help you understand the monorepo and find the right next document quickly.

## Monorepo Structure

| Path | What it contains |
| --- | --- |
| `source/taos-community` | Core engine, community code, packaging, examples, and engine-adjacent tools |
| `source/taos-internal` | Internal / enterprise source tree used by the top-level build |
| `source/taos-adapter` | `taosAdapter`, the bridge used by ingestion tools and WebSocket-based connectors |
| `source/taos-community/tools/keeper` | `taosKeeper`, the monitoring metrics export component |
| `source/taos-gen` | Data generation and related tooling |
| `source/taos-xservice` | taosX services, Explorer, plugins, Rust crates, and supporting packaging |
| `source/taos-insight` | Insight and monitoring-related assets |
| `source/taos-connector-*` | Official language connectors and their own contributor docs |
| `docs/` | Product, release, roadmap, design, test, and process documentation |
| `tests/` | Top-level test assets and helpers |
| `packaging/` | Shared packaging assets |
| `tools/` | Repository tooling and scripts |

## Documentation

Use the root README as a map, then jump to the document closest to your task:

| Need | Go here |
| --- | --- |
| Product, release, roadmap, design, and test docs in this repo | [`docs/README.md`](docs/README.md) |
| TDengine documentation (English) | [docs.tdengine.com](https://docs.tdengine.com/) |
| TDengine documentation (Chinese) | [docs.taosdata.com](https://docs.taosdata.com/) |
| Component-specific contributor workflows | The README inside the relevant `source/...` directory |

## Key Subprojects

| Component | Entry point |
| --- | --- |
| Core engine and community code | [`source/taos-community/README.md`](source/taos-community/README.md) |
| taosAdapter | [`source/taos-adapter/README.md`](source/taos-adapter/README.md) |
| taosKeeper | [`source/taos-community/tools/keeper/README.md`](source/taos-community/tools/keeper/README.md) |
| taosX and Explorer | [`source/taos-xservice/README.md`](source/taos-xservice/README.md) |
| .NET connector | [`source/taos-connector-dotnet/README.md`](source/taos-connector-dotnet/README.md) |
| Go connector | [`source/taos-connector-go/README.md`](source/taos-connector-go/README.md) |
| JDBC connector | [`source/taos-connector-jdbc/README.md`](source/taos-connector-jdbc/README.md) |
| Node.js connector | [`source/taos-connector-node/README.md`](source/taos-connector-node/README.md) |
| ODBC connector | [`source/taos-connector-odbc/README.md`](source/taos-connector-odbc/README.md) |
| Python connector | [`source/taos-connector-python/README.md`](source/taos-connector-python/README.md) |
| Rust connector | [`source/taos-connector-rust/README.md`](source/taos-connector-rust/README.md) |
| Data generation tooling | [`source/taos-gen/README.md`](source/taos-gen/README.md) |

## Starting Point

- **Evaluating the product**: start with the official docs site, then use this repo to inspect the implementation and component layout.
- **Working on the engine or shared build**: start at the root `CMakeLists.txt`, `cmake/`, and `source/taos-community/`.
- **Working on a specific component or connector**: jump directly to that component's README and local build instructions.
- **Looking for product or release process material**: start with `docs/README.md`.
