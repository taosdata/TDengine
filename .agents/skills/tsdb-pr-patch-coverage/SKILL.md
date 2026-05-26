---
name: tsdb-pr-patch-coverage
description: "给定 tsdb (TDengine) 仓库的一个 MR (GitLab) 链接或源/目标分支对，只跑该 MR 引入的测试（pytest + gtest），输出 MR 新增代码的 patch coverage（行级），并生成与 HTML 同口径的报告（注释/`{}`/空行 excluded 不计入分母）。Trigger keywords: MR 覆盖率, patch coverage, 增量覆盖率, mr coverage, gitlab mr 覆盖, tdengine 覆盖率, 新增代码覆盖率。"
metadata:
  author: mmwang
  version: 1.0.0
  owner_team: engine
compatibility: "Linux only; requires Conan2 / lcov 1.14+ / gcovr 8.x / Python 3.9+; tsdb already built with --coverage flags in debug/"
---

# tsdb-pr-patch-coverage

## When to Use

- 用户给一个 GitLab MR 链接（`https://git.tdengine.net/.../merge_requests/<id>`），让你"算一下这个 MR 的覆盖率 / patch coverage / 增量覆盖率"
- 用户直接告诉你 source/target 分支（"算下 feat/xxx 合到 enh/yyy 的覆盖率"）
- 用户给一对 commit (BASE..HEAD)，希望统计**只这个改动范围里新增/修改的行**的覆盖率
- 用户要求**只跑 MR 自己带的测试**，不跑全量回归

不适用于：
- 跑全量测试看整仓覆盖率（不是 patch coverage，用 lcov + genhtml 直接做）
- 非 TDengine 仓库（其它项目的测试入口、构建路径不同）

## Prerequisites

- 仓库：TDengine (`tsdb`)，已用 `--coverage` 编译进 `debug/` 目录
  - 验证：`find <repo>/source/taos-community/debug -name '*.gcno' | head -1` 应有输出
- 系统：Linux（pytest 框架依赖 `screen`、`LD_PRELOAD` libasan 等）
- 已安装：`git`、`lcov` ≥ 1.14、`gcovr` 8.x、`python3` ≥ 3.9
  - `pip install --quiet gcovr lcov_cobertura`
- 当前用户对仓库目录有写权限（要清空 `.gcda`、`sim/dnode*`）
- 网络可访问 `git.tdengine.net`（如果通过 MR 链接拉分支信息）
- 依赖 Skill：`skill-telemetry`

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-pr-patch-coverage version=1.0.0 author=mmwang`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
