# TDengine CI 指南

> **适用对象**: 所有使用 TSDB 仓库的研发人员  
> **CI 系统**: GitLab CI/CD（`.gitlab/.gitlab-ci.yml`）  
> **Runner 集群**: 2 台 builder (tsdb-builder-0 / tsdb-builder-1) + 14 台 test worker

---

## 目录

1. [流水线概览](#1-流水线概览)
2. [提交 MR 与触发 CI](#2-提交-mr-与触发-ci)
3. [Pipeline 触发规则](#3-pipeline-触发规则)
4. [流水线各阶段说明](#4-流水线各阶段说明)
   - [4.8 CI 调度与性能优化说明](#48-ci-调度与性能优化说明)
5. [查看 Pipeline 状态与日志](#5-查看-pipeline-状态与日志)
6. [失败用例诊断](#6-失败用例诊断)
7. [Core Dump 文件查找](#7-core-dump-文件查找)
8. [本地复现与调试单个用例](#8-本地复现与调试单个用例)
9. [cases.task 用例配置](#9-casestask-用例配置)
10. [常见问题 FAQ](#10-常见问题-faq)
11. [GitHub PR 迁移到 GitLab MR](#11-github-pr-迁移到-gitlab-mr)
12. [磁盘清理机制](#12-磁盘清理机制)
    - [12.1 总体结构](#121-总体结构)
    - [12.2 Coordinator 节点清理](#122-coordinator-节点清理)
    - [12.3 Worker 节点清理](#123-worker-节点清理)
    - [12.4 清理触发时机](#124-清理触发时机)
    - [12.5 保留参数速查与调整方法](#125-保留参数速查与调整方法)
- [附录 A — Runner 机器列表](#a-runner-机器列表)
- [附录 B — 关键路径速查](#b-关键路径速查)
- [附录 C — 环境变量速查](#c-环境变量速查)
- [附录 B — 关键路径速查](#b-关键路径速查)
- [附录 C — 环境变量速查](#c-环境变量速查)

---

## 1. 流水线概览

每次触发的 CI Pipeline 包含 6 个主要阶段，按顺序执行：

```
prepare → build → quality → verify → upload → test
                                                  │
                                                  ├── coordinator (调度器，运行于 builder)
                                                  ├── test-linux-1  (u3-141)
                                                  ├── test-linux-2  (u3-142)
                                                  ├── test-linux-3  (u3-143)
                                                  ├── test-linux-4  (u0-210)
                                                  ├── test-linux-5  (u1-63, large-mem)
                                                  ├── test-linux-6  (u1-59, large-mem)
                                                  ├── test-linux-7  (u0-212)
                                                  ├── test-linux-8  (u1-47, large-mem)
                                                  ├── test-linux-9  (u3-145)
                                                  ├── test-linux-10 (u3-146)
                                                  ├── test-linux-11 (u3-147)
                                                  ├── test-linux-12 (u2-205)
                                                  ├── test-linux-13 (u2-210)
                                                  └── test-linux-14 (u2-217)
```

| 阶段 | Job | 说明 |
|------|-----|------|
| prepare | prepare-workspace | 克隆源码到 builder 机器 |
| check | check-void + check-enum | 代码检查（并行，任意失败立即终止后续构建） |
| build | build-externals → build-noasan + build-asan | 构建 externals → 并行编译 TDengine (noasan + asan) |
| quality | check-assert | 代码质量检查（assert 使用规范）|
| verify | unit-test | CTest 单元测试 |
| upload | upload-nexus | 打包上传产物到 Nexus |
| test | coordinator + test-linux-1~14 | 系统集成测试（14 台 worker）|

---

## 2. 提交 MR 与触发 CI

### 2.1 提交 MR

```bash
# 1. 基于 3.3.6 创建分支
git checkout 3.3.6 && git pull
git checkout -b feat/my-feature

# 2. 开发 & 提交
git add -A && git commit -m "feat: add xxx"

# 3. 推送到远端
git push origin feat/my-feature

# 4. 在 GitLab Web UI 上创建 Merge Request
#    Source: feat/my-feature → Target: 3.3.6
```

### 2.2 Pipeline 自动触发

创建目标为 `main`、`3.0` 或 `3.3.6` 系列分支的 MR 后，Pipeline **自动触发**。你不需要做任何额外操作。

> **注意 1**: 当前接受**目标分支为 `main`、`3.0*`（如 `3.0`）、`3.3.6*`（如 `3.3.6`、`3.3.6.x`）**的 MR 触发 CI。  
> **注意 2**: Pipeline 触发后，build/test 阶段只有在以下文件发生变更时才会实际执行（否则 job 会被跳过）：
> ```
> **/*.{c,h,cpp,cmake}                    CMakeLists.txt
> source/**                               scripts/**
> .gitlab/.gitlab-ci.yml
> source/**/test/ci/cases.task            （用例列表变更）
> ```
> 仅修改文档或非代码文件时，build/test 阶段会跳过，流水线快速结束。

### 2.3 推送新 commit

向已有 MR 推送新 commit 时：
- **旧 pipeline 自动取消**（`auto_cancel: on_new_commit: interruptible`）
- **新 pipeline 自动启动**
- 无需手动操作

### 2.4 手动触发（Web）

在 GitLab 仓库页面 → **Build → Pipelines → Run Pipeline**：
- 选择分支（如 `main`、`3.0`、`3.3.6`）
- 可选设置变量（如 `RELEASE_VERSION`）
- 点击 "Run Pipeline"

> **注意**: `3.0` 和 `3.3.6` 分支的 CI 同时支持 MR 触发、Push 触发、定时调度和手动触发四种方式；`main` 分支 CI 仅支持 MR 触发。

---

## 3. Pipeline 触发规则

| 触发方式 | 条件 | 说明 |
|----------|------|------|
| **MR 事件** | 创建/更新 MR，**目标分支匹配 `main`、`3.0*` 或 `3.3.6*`** | 最常用触发方式 |
| **Push** | 推送到受保护的 `3.0`、`3.3.6` 系列分支 | 直接推送触发 |
| **定时调度** | Schedule | daily build 使用 |
| **手动** | Web UI | 临时测试 / 发布版本 |

> **注意**: Pipeline 内各 build/test job 还有第二层过滤（`.rules-code-change`）：仅当以下文件有变更时才会执行：
> - 代码文件：`**/*.{c,h,cpp,cmake}`、`CMakeLists.txt`
> - 源码目录：`source/**`
> - CI 脚本：`scripts/**`（注意：3.3.6 分支 CI 脚本在根目录 `scripts/` 下，而非 `tests/ci/scripts/`）
> - CI 配置：`.gitlab/.gitlab-ci.yml`
> - 用例列表：`source/**/test/ci/cases.task`
>
> 如果你只修改了文档或测试 Python 文件，build/test 阶段会被跳过，流水线快速结束。

---

## 4. 流水线各阶段说明

### 4.1 prepare — 源码克隆

- **prepare-workspace** 在 builder 机器上克隆两份源码（NoSan 用和 ASAN 用），保证后续并行编译互不干扰
- **输出**: `build.env` 包含 `WORKSPACE`、`TSDB_SRC`、`TSDB_SRC_SAN` 路径
- **workspace 路径规则**:
  - MR: `/data1/tdengine-ci/mr<IID>/`
  - 定时: `/data1/tdengine-ci/daily-<branch>-<date>/`
  - 手动: `/data1/tdengine-ci/web-<pipeline_id>/`

### 4.2 check — 代码检查（并行）

在这个阶段，两个检查 job 并行执行：

- **check-void**: 运行 `check_void.sh`，检查返回值被忽略的函数调用（严格规范）。**失败会阻塞后续所有阶段**，`build-externals` 在其 `needs` 中依赖它。
- **check-enum**: 运行 `check_enum.sh`，检查 enum 定义规范。设置了 `allow_failure: true`，**失败仅作警告（pipeline 标黄），不阻塞 build 阶段**。

> **说明**：`check-void` 失败 → pipeline 立即终止，不会进入 build；`check-enum` 失败 → build/test 阶段照常继续，job 显示为警告状态。

### 4.3 build — 三步构建

```
check-void → build-externals  (哨兵快速通道 → 独占 flock)
                   ↓ needs
         build-noasan ──并行── build-asan
           (共享 flock 持有整个 make 过程，读写锁语义)
```

> **依赖链说明**：`build-externals` 的 `needs` 为 `[prepare-workspace, check-void, check-enum]`。`check-enum` 设有 `allow_failure: true`，即使失败 `build-externals` 也会正常启动，不会阻塞构建流程。

- **build-externals**: 构建 RocksDB、libuv、curl、zlib、Arrow/Parquet 等第三方库。externals stamp 写入 `cache/externals-others-amd64/`（与 debug 目录无关，`--clean` 不会清除）。使用独占 flock 防止多 MR 并发写入。

  **哨兵快速通道**: build-externals 在进入 flock 之前先检查 `.externals-ready.<branch>.md5` 哨兵文件——若 `external.cmake` 的 MD5 与哨兵一致，则立即 `exit 0`（约 5 秒），彻底跳过 cmake configure 和排队等锁。只有 `external.cmake` 内容变化（新增/升级依赖）或缓存损坏时才真正进锁重建。不同分支（main/3.0/3.3.6）各维护独立哨兵文件，避免分支间误判。

- **build-noasan**: 编译 TDengine 主体（不含 ASAN），启用 `BUILD_TEST=ON`，输出到 `debug-others/`。产出全部测试二进制。
- **build-asan**: 编译 TDengine 主体（内置 ASAN，`-DBUILD_SANITIZER=ON`），`BUILD_TEST=OFF`（测试二进制在 upload 阶段从 noasan 补充）。输出同样在 `debug-others/`。

> **并行执行**: build-noasan 和 build-asan 使用**共享锁**（`flock -s`）而非独占锁。共享锁仅在验证 externals 文件完整性时持有（< 1s），验证通过后立即释放，两者真正并行编译。`TD_EXTERNALS_USE_ONLY=ON` 确保 cmake configure + make 只读 externals 缓存，临时文件写入各自独立的源码目录（`TSDB_SRC/debug-others/` vs `TSDB_SRC_SAN/debug-others/`），无写冲突。相比旧的独占串行方案，节省约 25 分钟/pipeline。
>
> **构建镜像**: 三个 build job 统一使用 `harbor.tdengine.net/tsdb-builder/others:${BUILDER_IMAGE_TAG}-amd64`（当前版本 0.21，GCC 14 / libasan.so.8），与测试容器 `tdengine-ci:0.1` 版本对齐，消除 libasan 版本不匹配问题。

### 4.4 quality — 代码质量检查

- **check-assert**: 运行 `count_assert.py`，检查代码中的 assert 使用规范

### 4.5 verify — 单元测试

- 在 builder 上的 Docker 容器内运行 `ctest`
- 执行 `ctest`（排除若干已知不稳定项）、`clientTest`、`connectOptionsTest`
- 超时限制: 单个 ctest 1200 秒，整体 job 40 分钟
- 产出 `junit.xml` 和 `ctest.log`

### 4.6 upload — 产物打包上传

将 NoSan 和 ASAN 两份构建产物分别打包为 tar.gz，上传到 Nexus：

```
Nexus 路径（MR 触发）:
  tsdb/ci/mr<IID>/linux/x64/noasan/linux-x64-noasan.tar.gz  ← build-noasan 产物
  tsdb/ci/mr<IID>/linux/x64/asan/linux-x64-asan.tar.gz      ← build-asan 产物

其他触发方式（当前已注释，备查）:
  Push:     tsdb/ci/branch-<branch>/linux/x64/noasan | asan
  定时:     tsdb/daily/<YYYYMMDD>/<branch>/linux/x64/noasan | asan
  手动:     tsdb/release/<version>/linux/x64/noasan | asan
```

> **说明**: ASAN 包（`asan/`）中的测试二进制（`tmq_sim`、`tmq_taosx_ci` 等）由 `build-noasan` 补充，因为 ASAN 构建时 `BUILD_TEST=OFF`。

### 4.7 test — 分布式集成测试

这是耗时最长的阶段，由 **1 个 coordinator + 14 个 worker** 组成：

- **coordinator** 运行在 builder 上，同时解析两个 cases.task 文件并通过 HTTP API 动态分发用例：
  - `source/taos-community/tests/parallel_test/cases.task`（legacy 测试框架）
  - `source/taos-community/test/ci/cases.task`（新测试框架 newfw）
- **test-linux-1~14** 运行在各 worker 上，从 coordinator 拉取用例并执行
- 每个 worker 内部支持多容器并发（`TEST_CONCURRENCY` 自适应计算）
- Prometheus 监控各 worker 负载（采样窗口 `PROM_RATE_WINDOW=1m`，30s 更新间隔），空闲机器多分配用例
- u1-63、u1-59、u1-47 为大内存机器，`WORKER_CAPS=large-mem`，优先分配优先级为 `100` 的大内存用例
- 调度激进程度由 `CI_SCHED_AGGR`（0/1/2）控制，默认 1（中等）

> **目录映射说明**: 当前仓库目录结构为 `tsdb/`，容器内测试框架由于历史原因依赖 `TDinternal/community` 目录结构。为了避免迁移期间过多路径冲突，实现方式是通过 Docker volume 挂载，将 tsdb 仓库目录映射到容器内的 `TDinternal/community` 路径，容器内的测试框架依然按原架构运行。待 GitHub PR 全部迁移到 GitLab 后，再统一正为 `tsdb/` 目录结构。

### 4.8 CI 调度与性能优化说明

本节说明 CI 系统中已实施的几项调度和性能优化，对日常使用透明，了解后有助于排查异常行为。

#### build 阶段优化

**build-externals 哨兵快速通道**

`build-externals` 在进入 flock 之前先检查哨兵文件（`.externals-ready.<branch>.md5`）：
- 若本分支的 `external.cmake` MD5 未变化，立即 `exit 0`（~5 秒），无需等锁、无需 cmake configure
- 哨兵按分支独立维护（main / 3.0 / 3.3.6 各有一个），避免分支间误判
- 只有 `external.cmake` 内容变化（新增/升级依赖）或缓存损坏时才真正进锁重建

**build-noasan 和 build-asan 真并行（读写锁模式）**

使用 `flock` 的**读写锁语义**（`flock -s` = 共享/读锁，`flock -x` = 独占/写锁）：

| Job | 锁类型 | 持有时间 | 说明 |
|-----|--------|---------|------|
| build-externals | 独占（`-x`） | 整个 externals 构建 | 多 MR 排队，防止并发写 |
| build-noasan | 共享（`-s`） | 整个 make 过程 | 多个共享锁可并存 |
| build-asan | 共享（`-s`） | 整个 make 过程 | 多个共享锁可并存 |

核心保证：
- **同一 MR**：noasan 和 asan 同时持共享锁 → 真正并行
- **多 MR 并发**：所有 MR 的 noasan/asan 全部可以同时持共享锁 → 互不阻塞
- **build-externals 隔离**：独占锁必须等所有共享锁释放后才能获取（等 noasan/asan 都跑完），反之 noasan/asan 等 externals 重建完成后才能开始 → externals 缓存始终一致
- **实际效益**：总 build 时间从串行的 noasan + asan ≈ 60 分钟缩短至 max(noasan, asan) ≈ 35 分钟（节省 ~25 分钟）

#### test 阶段调度优化

**`CI_SCHED_AGGR` — 一键调度激进程度**

通过 GitLab CI 变量 `CI_SCHED_AGGR` 控制分发批次大小和 worker 负载上限：

| 值 | 名称 | CPU 分配上限 | 适用场景 |
|----|------|------------|---------|
| `0` | conservative | 60% | worker 有其他任务、防过载 |
| `1` | moderate（**默认**） | 70% | 常规使用 |
| `2` | aggressive | 80% | CI 专机，希望尽快出结果 |

通过 `.gitlab-ci.yml` 顶部 `variables` 或 GitLab UI → Pipeline Settings 调整。

**`PROM_RATE_WINDOW` — Prometheus 采样窗口**

`PROM_RATE_WINDOW=1m`（默认，原为 `5m`）：coordinator 的 Prometheus 查询使用 1 分钟滑动窗口，负载变化在 1 分钟内反映，避免旧的 5 分钟窗口导致 worker 空转等待数据刷新。

**`_ci_register_job` PID 文件精确计数**

worker 并发数 `TEST_CONCURRENCY` 基于 PID 文件计数（而非 pgrep subshell），避免 subshell 自身被计入导致并发数被低估一半。

---

## 5. 查看 Pipeline 状态与日志

### 5.1 进入 Pipeline 页面

1. 打开 MR 页面
2. 找到 **Pipelines** tab，点击 pipeline ID 或状态图标
3. 或者: 仓库页面 → **Build → Pipelines**

### 5.2 Pipeline 视图

Pipeline 页面展示所有 Job 的状态。所有 builder job（coordinator / build / check）和 worker job（test-linux-N）的标题均含主机名，无需点进去才能知道在哪台机器执行：

```
✅ prepare-workspace
✅ check-void [tsdb-builder-0]   ⚠ check-enum [tsdb-builder-0]
✅ build-externals [tsdb-builder-0]
✅ build-noasan [tsdb-builder-0]    ✅ build-asan [tsdb-builder-0]
✅ check-assert [tsdb-builder-0]
✅ unit-test [tsdb-builder-0]
✅ upload-nexus [tsdb-builder-0]
🔄 coordinator [tsdb-builder-0]
🔄 test-linux-1 [u3-141]   ❌ test-linux-2 [u3-142]   ✅ test-linux-3 [u3-143] ...
```

> **注**：`coordinator [$TSDB_BUILDER_TAG]` 中的 `$TSDB_BUILDER_TAG` 由 `workflow:rules:variables` 动态设置， MR IID 末位 0–4 → `tsdb-builder-0`（u1-47），5–9 → `tsdb-builder-1`（u2-104）。

| 图标 | 含义 |
|------|------|
| ✅ | 成功 |
| ❌ | 失败 |
| ⚠ | 失败但 allow_failure=true（仅警告） |
| 🔄 | 运行中 |
| ⏸ | 等待依赖 |
| 🚫 | 已取消 |

### 5.3 查看 Job 日志

1. 点击任意 Job 名称（如 `test-linux-2 [u3-142]`）
2. 进入 Job 详情页，展示实时日志
3. 右侧面板显示 **Duration**（耗时）和 **Runner**（执行机器）

### 5.4 查看 JUnit 测试报告

在 **MR 页面 → Pipeline → Tests** tab，GitLab 自动聚合所有 JUnit XML 报告：
- 显示 Pass / Fail / Error 数量
- 失败用例可直接展开查看

---

## 6. 失败用例诊断

> **快速入口**（按优先级）：
>
> | 优先级 | 入口 | 适用场景 |
> |--------|------|---------|
> | ⭐⭐⭐ | **[6.2 Tests Tab](#62-level-1--tests-tab推荐首选)** | 最快，MR 页面直接看失败用例名 |
> | ⭐⭐⭐ | **[6.3 Coordinator 日志](#63-level-2--coordinator-日志)** | 看失败原因摘要 + Runner 链接 |
> | ⭐⭐ | **[6.4 Worker Job 日志](#64-level-3--worker-job-日志)** | 单 worker 全部用例执行顺序 |
> | ⭐⭐ | **[6.6 Runner HTTP 文件服务](#66-level-5--runner-http-文件服务)** | 浏览器直接访问原始日志目录 |
> | ⭐ | **[6.5 Artifacts](#65-level-4--artifacts完整日志)** | 下载完整日志 + ASAN 报告 |

### 6.1 失败信息层级（由浅入深）

```
Level 1: MR Pipeline 页面 → Tests tab     ← 最快速
Level 2: coordinator job 日志              ← 全局视角，折叠式错误摘要
Level 3: test-linux-N job 日志             ← 具体 worker 的执行日志
Level 4: Job Artifacts (results/logs/)     ← 完整日志 + taosd 日志
Level 5: Runner HTTP 文件服务              ← 失败用例目录浏览
```

### 6.2 Level 1 — Tests Tab（⭐ 推荐首选）

1. 打开 MR 页面
2. 点击 Pipeline → **Tests** tab
3. 失败用例列表会直接展示
4. 点击用例名可展开查看失败详情

### 6.3 Level 2 — Coordinator 日志（⭐ 最全面的失败摘要）

`coordinator` Job 收集所有 worker 的失败信息，统一展示：

1. 点击 `coordinator` Job
2. 在日志中搜索 **`FAIL`** 或 **`❌`**
3. 每个失败用例有一个**折叠 section**（红色标题），点击展开：

```
▶ FAIL [exit=1] [u1-47] .::./ci/pytest.sh pytest cases/05-VirtualTables/test_vtable_auth_alter_normaltable.py  (430.3s)
  ────────────────────────────────────────────────────────────────
  Case logs:   http://192.168.1.47:8899/job-11274/n8-05-VirtualTables_test_vtable_auth_alter_normaltable/run.log.txt
  Runner logs: http://192.168.1.47:8899/job-11274/n8-05-VirtualTables_test_vtable_auth_alter_normaltable/
  Fail dir:    root@192.168.1.47:/data1/tdengine-ci/fail-logs/job-11274/n8-05-VirtualTables_test_vtable_auth_alter_normaltable/
  复现方法:
  # workspace 在 builder 机器 u1-47 上，请 SSH 到该机器执行
  [本地非ASAN]
  cd /data1/tdengine-ci/mr199/tsdb && TAOS_BIN_PATH=$PWD/debug-others/build/bin ./tests/ci/scripts/run_case.sh --clean cases/05-VirtualTables/test_vtable_auth_alter_normaltable.py
  [容器ASAN]
  cd /data1/tdengine-ci/mr199/tsdb-san && ln -sfn debug-others debugSan 2>/dev/null; source/taos-community/test/ci/run_container.sh -w /data1/tdengine-ci/mr199/tsdb-san -s y -d . -c "./ci/pytest.sh pytest cases/05-VirtualTables/test_vtable_auth_alter_normaltable.py" -t 1
  摘要信息:
  FAILED cases/05-VirtualTables/test_vtable_auth_alter_normaltable.py::TestVtableAuthAlterDrop::test_alter_drop
  AssertionError: ...
  ────────────────────────────────────────────────────────────────
```

**关键信息**:
- **worker**: 执行该用例的机器（方括号中显示）
- **exit code**: 退出码（124 = 超时，1 = 断言失败，137 = 被 kill）
- **Case logs**: 直链到 `run.log.txt`，可一键在浏览器查看原始日志
- **Runner logs**: 目录列表链接，超时/宕机用例也会显示完整 URL（coordinator 通过心跳记录 job_id + node 并还原路径）
- **Fail dir**: 失败用例在 runner 上的保留目录路径（可 `scp` 或 SSH 过去查看）
- **复现方法**: 可直接复制粘贴执行的复现命令（整行选取即用）
  - `[本地非ASAN]` — 宿主机直跑（快速，需本地有 Python 依赖）
  - `[容器ASAN]` / `[容器非ASAN]` — Docker 容器内运行（与 CI 一致，推荐）
- **摘要信息**: 过滤 pip 噪音后的关键错误行（锚点往前 10 行 + 最多 80 行）

**尾期慢用例追踪**：进度达到 98% 且队列已空时，coordinator 每隔 30s 打印一次当前仍在运行的用例及已用时间，格式如下：

```
[coordinator] ⏳ tail 2 in_flight case(s) (progress 913/915, elapsed=1401s):
[coordinator]   [u3-146    ]   1040s  cases/99-Stress/test_stress_long.py
[coordinator]   [u3-145    ]    587s  cases/05-VirtualTables/test_vtable_perf.py
```

此日志每隔 `PROM_INTERVAL` 秒（默认 30s）刷新一次，直到所有用例完成。

### 6.4 Level 3 — Worker Job 日志

点击具体的 `test-linux-N` Job，日志中包含该 worker 执行的所有用例。

**实时 FAIL section**（用例失败时立即打印，默认折叠）：
```
▶ FAIL [exit=1] .::./ci/pytest.sh pytest cases/81-Tools/03-Benchmark/test_benchmark_basic.py
  ────────────────────────────────────────────────────────────────
  Case logs:   http://192.168.3.141:8899/job-14308/n1-81-Tools_03-Benchmark_test_benchmark_basic/run.log.txt
  Runner logs: http://192.168.3.141:8899/job-14308/n1-81-Tools_03-Benchmark_test_benchmark_basic/
  Fail dir:    root@192.168.3.141:/data1/tdengine-ci/fail-logs/job-14308/n1-81-Tools_03-Benchmark_test_benchmark_basic/
  摘要信息:
  FAILED cases/81-Tools/03-Benchmark/test_benchmark_basic.py::...
  ────────────────────────────────────────────────────────────────
```

**末尾 Summary section**（每个失败用例一个折叠块，格式与实时 section 相同）：
- 包含 Case logs / Runner logs / Fail dir / 摘要信息
- 日志超过 200KB 时，section 内仅显示最后 1500 行并给出提示；完整日志始终保存在 HTTP retain 目录

### 6.5 Level 4 — Artifacts（失败用例日志 + 耗时汇总）

coordinator job 的 artifacts 无论成功失败都会上传（`when: always`），内容为：

1. 在 Job 详情页右侧，找到 **Job artifacts → Browse**
2. 目录结构：

```
results/
  junit-aggregate.xml                  ← 聚合 JUnit 报告（reports.junit 路径，始终上传）
  case-timing.txt                      ← 所有用例耗时汇总（按耗时降序），始终上传
  junit-N.xml                          ← 各 worker JUnit 报告
  logs/
    <用例slug>/
      run.log.txt                      ← 用例运行日志（失败时上传）
```

**`case-timing.txt` 示例**（按耗时降序，方便定位慢用例）：

```
# case-timing.txt — generated by coordinator.py
# total=915  pass=910  fail=5  elapsed=1500s
# columns: status  elapsed_s  worker  path
PASS         1040.2s  u3-146        cases/99-Stress/test_stress_long.py
FAIL(1)       405.6s  u3-142        cases/12-UDFs/test_udf_create.py
PASS          380.1s  u3-145        cases/05-VirtualTables/test_vtable_perf.py
...
```

**说明**：
- `case-timing.txt` 每次运行都会上传，是性能分析和瓶颈排查的一手资料
- 只上传 `run.log.txt` 和 `junit-*.xml`（失败时），避免上传大量 `sim/taosd` 日志导致 413 错误
- `sim/`（taosd 日志、ASAN 报告等）保留在 worker 上，通过 **HTTP retain 目录**（6.6 节）访问，保留 7 天
- `run.log.txt` 超过 200KB 时，worker Job 日志中仅显示最后 1500 行；HTTP retain 目录中的文件始终是完整的

### 6.6 Level 5 — Runner HTTP 文件服务（⭐ 最完整的原始日志）

失败用例的完整日志会保留在 worker 机器上 7 天，通过 HTTP 可直接浏览：

```
http://<worker_ip>:8899/job-<JOB_ID>/<用例slug>/
```

- 这个 URL 会出现在 coordinator 的折叠 section 中（`Runner logs:` 行）
- 直接在浏览器中打开即可
- 包含比 artifacts 更完整的原始文件

> **注意**: 这些日志 7 天后会被自动清理。如需长期保存，请从 GitLab artifacts 下载。

---

## 7. Core Dump 文件查找

### 7.1 Core Dump 生成位置

每个测试用例运行在独立的 Docker 容器中。core dump 文件生成在容器内部的 `/home/coredump/` 目录，映射到宿主机的：

```
<WORKDIR>/tmp/thread_volume/<thread_no>/coredump/
```

其中 `thread_no` 是测试线程编号（由 `NODE_INDEX` 和 `slot` 计算得出）。

### 7.2 通过 Artifacts 查找

CI 的 `after_script` 会尝试收集 coredump 相关信息到 artifacts。但由于 artifacts 现在只上传 `run.log.txt` 和 JUnit XML，**core 文件不会被上传到 GitLab artifacts**。

查找 coredump 的正确方式：

1. **查看 worker job 日志中的自动 GDB 摘要**（🔍 Coredump GDB 折叠 section，见 7.5、6）
2. **通过 HTTP retain 目录浏览**（见 6.6、7.3 节）——原始 core 文件和 `gdb-bt-*.txt` 摘要均保留在7 天

### 7.3 通过 SSH 查找（如需要）

如果 artifacts 中未收集到 core 文件（可能因为文件过大未上传），可以 SSH 到对应 worker：

```bash
# 1. 从 coordinator 日志中找到失败用例的 worker 和 JOB_ID
#    例如: worker: u3-141, JOB_ID: 12345

# 2. SSH 到 worker
ssh u3-141

# 3. 查找 core 文件
# CI 运行期间:
find /data1/tdengine-ci/job-<JOB_ID>/tmp/thread_volume/*/coredump/ -name 'core*' 2>/dev/null

# CI 结束后（WORKDIR 已清理），在失败日志保留目录查找:
find /data1/tdengine-ci/fail-logs/job-<JOB_ID>/ -name 'core*' 2>/dev/null

# 或者在系统默认的 core dump 目录:
find /tmp/ -name 'core.*' -newer /tmp/some_reference_time_file 2>/dev/null
```

### 7.4 core_pattern 说明

CI 运行时会自动修正 `core_pattern`（避免 apport 管道导致的 exit=123 问题）：

```
# CI 设置的 core_pattern:
/corefile/core_%e-%p
# %e = 可执行文件名（如 taosd）或进程自定义名（如 dnode-mgmt）, %p = PID
# 生成示例: /corefile/core_taosd-12345  core_dnode-mgmt-836
#
# 容器内 /corefile/ 目录映射到宿主机:
# <WORKDIR>/tmp/thread_volume/<thread_no>/coredump/
```

### 7.5 实战示例：从失败日志到 Core Dump 分析

以下是一个完整的实际排查流程，以 `test_udf_create.py` 用例失败为例：

**1) CI 日志中看到失败信息（coordinator 折叠 section）**

```
▶ FAIL [exit=1] [u3-142] .::pytest cases/12-UDFs/test_udf_create.py  (405.6s)
  ────────────────────────────────────────────────────────────────
  Case logs:   http://192.168.3.142:8899/job-3854/n2-12-UDFs_test_udf_create/run.log.txt
  Runner logs: http://192.168.3.142:8899/job-3854/n2-12-UDFs_test_udf_create/
  Fail dir:    root@192.168.3.142:/data1/tdengine-ci/fail-logs/job-3854/n2-12-UDFs_test_udf_create/
```

**2) 浏览器打开 Runner HTTP 链接**

打开 `http://192.168.3.142:8899/job-3854/n2-12-UDFs_test_udf_create/` 可以看到：

```
build/
case.txt
coredump/
    core_taosd-200
    core_taosudf-229
    gdb-bt-core_taosd-200.txt          ← CI 自动生成的 GDB 摘要
run.log.txt
sim/
```

**3) 查看 worker job 日志中的自动 GDB 摘要（无需 SSH）**

在 `test-linux-2 [u3-142]` job 日志末尾，找到 `🔍 Coredump GDB — n2-12-UDFs_test_udf_create` 折叠 section，展开可看：

```
Core : core_taosd-200  (245M)
Binary: 'taosd'
file  : core_taosd-200: ELF 64-bit LSB core file, x86-64, ..., from 'taosd', execfn: '/usr/bin/taosd', ...
Exe   : /data1/tdengine-ci/fail-logs/job-3854/_shared_bin/taosd
[GDB] running thread apply all bt (timeout 90s) ...
Thread 48 (Thread 0x7f... (LWP 200) "dnode-mgmt"):
#0  0x00007f... in pthread_cond_wait () from /lib/x86_64-linux-gnu/libpthread.so.0
#1  0x00005555... in taosThreadCondWait ()
...
Thread 1 (Thread 0x7f... (LWP 200) "taosd"):
#0  0x00005555... in taosdMain ()
```

**4) 如需 SSH 到 worker 手动分析**

```bash
# 登录对应 worker
ssh 192.168.3.142

# core 文件和二进制均在 fail-logs 目录中
cd /data1/tdengine-ci/fail-logs/job-3854/n2-12-UDFs_test_udf_create/coredump

# 使用 GDB 分析（二进制在 _shared_bin/ 目录）
gdb ../../_shared_bin/taosd core_taosd-200

# 也可分析其他进程的 core
gdb ../../_shared_bin/taosd core_taosudf-229
```

> **路径说明**:
> - 失败日志保留目录: `/data1/tdengine-ci/fail-logs/job-<JOB_ID>/<用例slug>/`
> - 共享二进制目录: `/data1/tdengine-ci/fail-logs/job-<JOB_ID>/_shared_bin/`
> - core 文件命名格式: `core_<进程名>-<PID>`（进程名可能是线程名，如 `dnode-mgmt`）
> - GDB 摘要文件: `coredump/gdb-bt-<core文件名>.txt`（CI 自动生成）
> - 日志保留 7 天，过期自动清理

### 7.6 使用 GDB 分析 Core Dump

**CI 自动 GDB 分析（无需手动操作）**

`run-test-dynamic.sh` 发现 coredump 后自动在 Docker 容器内运行 GDB，分析结果：
- 内嵌在 worker job 日志的 `🔍 Coredump GDB` 折叠 section（默认折叠，点击展开）
- 保存为 `<retain_dir>/coredump/gdb-bt-<core文件名>.txt`（人工 SSH 到 runner 可查看）

Binary 识别逻辑：
1. 解析 `file <core>` 输出中的 `execfn: '/path/to/taosd'`（最准确）
2. 从 core 文件名 `core_%e-%p` 提取进程名 `%e`
3. Fallback：尝试 `taosd`（线程名如 `dnode-mgmt` 本质是 taosd 进程）

**手动 GDB 分析（需要更深入排查时）**

```bash
# 在 worker 上直接分析
gdb /data1/tdengine-ci/fail-logs/job-<JOB_ID>/_shared_bin/taosd \
    /data1/tdengine-ci/fail-logs/job-<JOB_ID>/<用例slug>/coredump/core_taosd-<PID>

# 或在 Builder 机器上用 workspace 的二进制
gdb /data1/tdengine-ci/job-<JOB_ID>/debugNoSan/build/bin/taosd \
    /path/to/core_taosd-12345

# GDB 内常用命令:
(gdb) bt             # 查看当前线程调用栈
(gdb) bt full        # 查看完整调用栈（含局部变量）
(gdb) info threads   # 查看所有线程
(gdb) thread apply all bt   # 所有线程的调用栈（CI 自动执行的命令）
(gdb) thread N       # 切换到线程 N
```

---

## 8. 本地复现与调试单个用例

### 8.1 直接运行 Python 测试用例

大部分系统测试是 Python 用例，可以在本地直接运行：

```bash
# 1. 先编译 TDengine（Debug 模式）
cd /path/to/tsdb/source/taos-community
mkdir -p debug && cd debug
cmake .. -DCMAKE_BUILD_TYPE=Debug -DBUILD_TEST=ON
make -j$(nproc)

# 2. 安装 TDengine（或设置环境变量）
export PATH=$PWD/build/bin:$PATH
export LD_LIBRARY_PATH=$PWD/build/lib:$LD_LIBRARY_PATH

# 3. 运行单个用例
cd /path/to/tsdb/source/taos-community/test
pytest cases/01-DataTypes/test_datatype_bigint.py -v

# 多节点用例加 -N 参数：
pytest cases/70-Cluster/test_5dnode3mnode_stop_follower_leader.py -N 5 -M 3
```

### 8.2 使用 rerun.sh 一键复现失败用例（推荐）

`rerun.sh` 是最便捷的复现工具，封装了"下载构建产物 → 准备容器 → 运行"全流程，无需手动构建或知道 runner 路径。

**脚本位置**: `source/taos-community/test/ci/rerun.sh`

#### 运行机制

```
rerun.sh 执行流程
  ├─ 1. 解析参数（--case / --mr / -d / -r / -s）
  ├─ 2. 确定 COMMUNITY_DIR（case.txt 读取 → build 目录自动扫描）
  ├─ 3. 确定 SANITIZER（case.txt → cases.task 推断 → 默认 n）
  ├─ 4. 确定 DEBUG_DIR：
  │      ├─ -d 手动指定（最高优先）
  │      ├─ --mr N：从 Nexus 下载，缓存到 /tmp/tdci-mr-N-{asan,noasan}/
  │      ├─ /data/tsdb/debug（开发机默认路径）
  │      └─ /data1/tdengine-ci/*/debugNoSan（runner 自动扫描）
  ├─ 5. 创建 /tmp/tdci-run-<PID>/，复制 COMMUNITY_DIR/test/* 到此目录
  └─ 6. docker run tdengine-ci:0.1 → run_case.sh -d "." -c "$CMD" -e
```

#### fail-log case.txt 格式

CI 每次用例失败时，会在 `/data1/tdengine-ci/fail-logs/job-<JOB_ID>/<slug>/case.txt` 写入：

```
COMMUNITY_DIR=/data0/gitlab-runner/builds/<hash>/0/rd-public/tsdb/source/taos-community
DEBUG_DIR=/data1/tdengine-ci/job-<JOB_ID>/debugNoSan
SANITIZER=y
CMD=./ci/pytest.sh pytest cases/18-StreamProcessing/20-UseCase/test_idmp_manager.py
```

`--case <slug>` 模式自动读取该文件，无需手动指定任何路径。

> **注意**: 只有本分支（`test/ci-pressure-test1`）最新提交触发的 job 才会生成 `case.txt`。旧 job（如 job-7333）无该文件，需手动指定 `-r` 和 `-d`。

#### slug 格式说明

失败用例的 slug（`fail-logs` 子目录名）由 CI 自动生成：

```
n<NODE_INDEX>-<casePath 去掉 cases/ 前缀，/ 改为 __，去掉 .py/.sh 后缀>

示例：
  cases/18-StreamProcessing/20-UseCase/test_idmp_manager.py
  → n1-18-StreamProcessing_20-UseCase_test_idmp_manager
```

#### 在 runner 上定位 rerun.sh

runner checkout 路径含随机 hash（如 `/data0/gitlab-runner/builds/MkPwYP46l/0/...`），推荐通过 `case.txt` 里的 `COMMUNITY_DIR` 字段定位脚本：

```bash
# 方法1：从 case.txt 中定位（推荐，适用于有 case.txt 的新 job）
slug="n1-18-StreamProcessing_20-UseCase_test_idmp_manager"
case_txt=$(find /data1/tdengine-ci/fail-logs -name case.txt \
    -path "*/${slug}/case.txt" 2>/dev/null | sort -rV | head -1)
comm_dir=$(grep '^COMMUNITY_DIR=' "$case_txt" | cut -d= -f2-)
${comm_dir}/test/ci/rerun.sh --case "$slug"

# 方法2：find 搜索 rerun.sh（适用于无 case.txt 的旧 job）
RERUN=$(find /data0/gitlab-runner/builds -name "rerun.sh" \
    -path "*/test/ci/*" 2>/dev/null | head -1)
# 重要：COMM_DIR 从 rerun.sh 所在位置往上走 3 级（ci/ → test/ → taos-community/）
COMM_DIR=$(cd "$(dirname "$RERUN")/../.." && pwd)
```

#### 使用示例

```bash
# ── 场景1（最常用）：runner 上一键复现 CI 失败用例 ──────────────────────────
slug="n1-18-StreamProcessing_20-UseCase_test_idmp_manager"
case_txt=$(find /data1/tdengine-ci/fail-logs -name case.txt \
    -path "*/${slug}/case.txt" 2>/dev/null | sort -rV | head -1)
comm_dir=$(grep '^COMMUNITY_DIR=' "$case_txt" | cut -d= -f2-)
${comm_dir}/test/ci/rerun.sh --case "$slug"

# ── 场景2：用指定 MR 的构建替换原 job 产物（下载后缓存，二次运行免下载）──
${comm_dir}/test/ci/rerun.sh --mr 147 --case "$slug"

# ── 场景3：runner 上无 case.txt，手动指定路径和命令 ─────────────────────────
RERUN=$(find /data0/gitlab-runner/builds -name "rerun.sh" -path "*/test/ci/*" 2>/dev/null | head -1)
COMM_DIR=$(cd "$(dirname "$RERUN")/../.." && pwd)
${RERUN} \
  -r "${COMM_DIR}" \
  -s \
  --mr 147 \
  "./ci/pytest.sh pytest cases/18-StreamProcessing/20-UseCase/test_idmp_manager.py"

# ── 场景4：开发机本地运行（自动检测 /data/tsdb/debug）────────────────────────
/data/tsdb/source/taos-community/test/ci/rerun.sh \
  "./ci/pytest.sh pytest cases/01-DataTypes/test_datatype_bigint.py"
```

#### 选项速查

| 选项 | 说明 |
|------|------|
| `--case <slug>` | 从 fail-logs 读 `case.txt`，自动填充所有参数 |
| `--mr <N>` | 从 Nexus 下载 MR N 的构建产物（ASAN/noASAN 自动选择，缓存到 `/tmp/tdci-mr-N-{asan,noasan}/`） |
| `-r <DIR>` | 手动指定 taos-community 根目录 |
| `-d <DIR>` | 手动指定 debug 目录（最高优先，覆盖 `case.txt` 和 `--mr`）|
| `-s` | 强制使用 ASAN 构建 |
| `--user / --pass` | Nexus 认证凭证（也可用环境变量 `NEXUS_USERNAME` / `NEXUS_PASSWORD`）|

#### 运行后查看日志

```bash
# 运行过程中实时查看 taosd 日志（另开终端，PID 见启动摘要输出的 Logs 行）
tail -f /tmp/tdci-run-<PID>/sim/dnode1/log/taosdlog.0

# 查看完整 sim 目录结构
ls /tmp/tdci-run-<PID>/sim/

# ASAN 报告（如有）
cat /tmp/tdci-run-<PID>/sim/asan/psim.info
```

---

### 8.3 从 Coordinator 输出的复现命令直接复现（推荐）

CI 失败时，coordinator 日志已输出可直接粘贴的复现命令，无需任何额外查找。

#### 步骤

1. 在 coordinator job 日志中找到失败用例（搜索 `FAIL`）
2. 展开后找到 `复现方法:` 部分
3. 根据需要选择 `[本地非ASAN]` 或 `[容器ASAN]` 行，整行复制
4. SSH 到 builder 机器执行（`复现方法:` 第一行注释中有 builder 主机名）

#### 容器模式（推荐，与 CI 环境一致）

```bash
# SSH 到 builder 机器（主机名在 复现方法 第一行注释中）
ssh 192.168.1.47

# 直接粘贴 [容器ASAN] 行：
cd /data1/tdengine-ci/mr199/tsdb-san && ln -sfn debug-others debugSan 2>/dev/null; source/taos-community/test/ci/run_container.sh -w /data1/tdengine-ci/mr199/tsdb-san -s y -d . -c "./ci/pytest.sh pytest cases/05-VirtualTables/test_vtable_auth_alter_normaltable.py" -t 1
```

> **说明**:
> - `-s y` 表示使用 ASAN 构建（对应 `debugSan`），`-s n` 使用普通构建（`debugNoSan`）
> - `-t 1` 指定线程号，避免与正在运行的 CI 冲突（CI 使用 0-N）
> - `ln -sfn debug-others debugSan` 是因为 MR 工作空间的产物目录名为 `debug-others`，需要创建符号链接

#### 本地宿主机模式（快速，适合非 ASAN 场景）

```bash
# SSH 到 builder 机器
ssh 192.168.1.47

# 直接粘贴 [本地非ASAN] 行：
cd /data1/tdengine-ci/mr199/tsdb && TAOS_BIN_PATH=$PWD/debug-others/build/bin ./tests/ci/scripts/run_case.sh --clean cases/05-VirtualTables/test_vtable_auth_alter_normaltable.py
```

> **说明**:
> - `run_case.sh` 会自动安装 `requirements.txt` 中的 Python 依赖（首次较慢）
> - `--clean` 表示运行前清理旧的 sim 目录
> - `TAOS_BIN_PATH` 环境变量指向编译产物，`run_case.sh` 据此设置 PATH 和 LD_LIBRARY_PATH

#### 什么时候用哪种模式

| 场景 | 推荐模式 | 原因 |
|------|---------|------|
| ASAN 用例 | 容器模式 | 宿主机 libasan 版本可能不匹配 |
| 需要精确复现 CI 环境 | 容器模式 | 与 CI 运行环境完全一致 |
| 快速验证非 ASAN 用例 | 本地模式 | 启动快，无需 Docker |
| 调试时需要交互式 GDB | 容器交互模式（8.5 节）| 需要在容器内 attach |

### 8.4 使用 run_container.sh 手动复现 CI 环境

`run_container.sh` 是 CI 实际使用的容器执行脚本，也可以在 worker 上手动调用：

```bash
# 参数说明:
#   -w  工作目录（需包含 debugNoSan/ 或 debugSan/ 目录）
#   -d  执行目录（. 表示项目根目录，cases 表示 test/cases/）
#   -c  执行命令
#   -t  线程号（随意指定一个不冲突的数字）
#   -n  容器名（可选）
#   -e  企业版模式
#   -s  sanitizer（y/n）

# 示例：运行一个普通用例
bash source/taos-community/test/ci/run_container.sh \
  -w /data1/tdengine-ci/job-<JOB_ID> \
  -e \
  -d "." \
  -c "./ci/pytest.sh pytest cases/01-DataTypes/test_datatype_bigint.py" \
  -t 99 \
  -n "debug-test-manual"

# 示例：运行一个 ASAN 用例
bash source/taos-community/test/ci/run_container.sh \
  -w /data1/tdengine-ci/job-<JOB_ID> \
  -e \
  -d "." \
  -c "./ci/pytest.sh pytest cases/19-TSMAs/test_tsma.py" \
  -t 99 \
  -s y \
  -n "debug-test-asan"
```

### 8.5 使用 run-test-batch.sh 运行一批用例

如果需要运行 `cases.task` 中的一部分用例：

```bash
# 设置环境变量
export WORKDIR="/data1/tdengine-ci/job-<JOB_ID>"
export CI_NODE_INDEX=1
export CI_NODE_TOTAL=1    # 设为1表示本机运行所有用例
export CI_PROJECT_DIR="$(pwd)"
export TEST_CONCURRENCY=4

# 运行
bash tests/ci/scripts/run-test-batch.sh
```

### 8.6 进入 CI 容器交互式调试

```bash
# 1. 启动一个与 CI 相同的容器（交互模式）
docker run -it --privileged \
  -v /data1/tdengine-ci/job-<JOB_ID>/TDinternal/community:/home/TDinternal/community \
  -v /data1/tdengine-ci/job-<JOB_ID>/debugNoSan:/home/TDinternal/debug \
  tdengine-ci:0.1 bash

# 2. 在容器内手动设置环境
export PATH=/home/TDinternal/debug/build/bin:$PATH
export LD_LIBRARY_PATH=/home/TDinternal/debug/build/lib:$LD_LIBRARY_PATH
ln -sf /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so
ldconfig

# 3. 启动 taosd
taosd &
sleep 5

# 4. 运行测试
cd /home/TDinternal/community/test
pytest cases/01-DataTypes/test_datatype_bigint.py -v -s

# 5. 如果需要 GDB 调试 taosd
taosd &
gdb -p $(pidof taosd)
```

### 8.7 单元测试本地运行

```bash
# 编译
cd source/taos-community/debug
cmake .. -DCMAKE_BUILD_TYPE=Debug -DBUILD_TEST=ON
make -j$(nproc)

# 运行全部单元测试
ctest --output-on-failure -j8

# 运行单个单元测试
ctest -R dataformatTest --output-on-failure

# 或直接运行测试二进制
./build/bin/dataformatTest
```

---

## 9. cases.task 用例配置

### 9.1 文件位置

```
source/taos-community/test/ci/cases.task
```

### 9.2 格式说明

```
#priority,rerunTimes,Run with Sanitizer,casePath,caseCommand
```

每行一个用例，字段用逗号分隔：

| 字段 | 位置 | 说明 | 示例 |
|------|------|------|------|
| priority | 第1列 | 优先级（空=普通，100=大内存 large-mem） | `100` / 空 |
| rerunTimes | 第2列 | 重试次数（暂未使用） | 空 |
| sanitizer | 第3列 | 是否使用 ASAN 二进制（`y`/`n`） | `y` |
| casePath | 第4列 | 执行目录（`.`=项目根，`cases`=test/cases/） | `.` |
| caseCommand | 第5列起 | 执行命令 | `./ci/pytest.sh pytest cases/...` |

### 9.3 示例

```bash
# 普通 Python 用例（ASAN）
,,y,.,./ci/pytest.sh pytest cases/01-DataTypes/test_datatype_bigint.py

# 普通 Python 用例（非 ASAN）
,,n,.,pytest cases/81-Tools/03-Benchmark/test_benchmark_basic.py

# 多节点集群用例
,,y,.,./ci/pytest.sh pytest cases/70-Cluster/test_5dnode3mnode_stop_follower_leader.py -N 5 -M 3

# Shell 脚本用例
,,n,cases,bash 83-DocTest/python.sh

# 大内存用例（需要 large-mem worker）
100,,y,.,./ci/pytest.sh pytest cases/01-DataTypes/test_datatype_decimal.py
```

### 9.4 添加新用例

1. 在 `cases.task` 中对应分类区域添加新行
2. 如果是长耗时用例（>5 分钟），添加到文件头部的 `long-running cases` 区域
3. 如果需要大内存（>16GB），第一列填 `100`
4. sanitizer 字段（第3列）：
   - `y` — 使用 `./ci/pytest.sh` 包装器（会设置 taosd 等环境），使用 ASAN 二进制
   - `n` — 直接 `pytest` 或 `bash`，使用普通二进制

### 9.5 已注释用例说明

cases.task 中有一部分被注释掉的用例（以 `#` 开头），原因包括不稳定、偏高并发负载下超时、以及 3.3.6 分支特有的兼容性问题。

目前已识别问题的用例均已创建工作项跟进修复，但在并发压力较高的情况下，依然可能出现其他 fail 用例。这类问题只能递进地发现和修复。发现新的失败用例时，请参考第 6 节先进行评估，再决定是禁用还是创建工作项修复。

注释格式规范：
```
# [disabled YYYY-MM-DD 原因摘要]
#「原来的用例行」
```

---

## 10. 常见问题 FAQ

### Q1: Pipeline 没有触发？

**可能原因**:
- 你的修改只涉及文档/非代码文件（`.md` 等），不满足 `rules-code-change` 规则
- MR 还处于 Draft 状态
- 分支名不在支持的目标分支列表中（`main`、`3.0*`、`3.3.6*`）

**解决**: 去 Web UI 手动触发（Build → Pipelines → Run Pipeline）

### Q2: build 阶段失败了？

1. 查看 `build-noasan` 或 `build-asan` 的 job 日志
2. 下载 artifacts 中的 `build-logs/build-nosan.log` 或 `build-asan.log`
3. 常见原因：编译错误、链接错误

### Q3: unit-test 失败了？

1. 在 Job 日志中搜索红色 `❌` 标记
2. 每个失败用例有折叠 section，展开即可看到输出
3. 下载 `build-logs/ctest.log` 查看完整输出
4. 下载 `build-logs/junit.xml` 查看结构化结果

### Q4: test-linux-N 失败了但不确定是哪个用例？

1. **先看 coordinator job 日志** — 它会汇总所有失败用例
2. 在 coordinator 日志中搜索 `FAIL`
3. 折叠 section 中包含用例名、worker、退出码、错误摘要

### Q5: 用例超时了（exit=124）？

超时原因可能是：
- 用例本身执行慢（数据量大 / 等待超时）
- taosd 启动失败导致用例卡住
- 服务器负载过高

**调查步骤**:
1. 查看 coordinator 日志中的超时用例信息
2. 查看 artifacts 中对应用例的 `dnode*/log/taosdlog.*`
3. 如果是偶发超时，可以重跑 Pipeline（Push 一个空 commit 或在 UI 上 Retry）

### Q6: 多人同时提 MR 会互相影响吗？

**不会。** 每条 MR Pipeline 有完全独立的：
- 源码目录、构建目录
- Docker 容器命名
- Nexus 产物路径
- Coordinator 端口

即使 10 条 MR 同时运行也互不干扰。每台 worker 的并发数会自动按 pipeline 数量等比降低。

### Q7: 如何重跑失败的 Pipeline？

**推荐：只需点一次 coordinator Retry（自动触发所有失败 worker）**

在 Pipeline 页面找到 `coordinator` job，点击 **Retry**：
- coordinator 以 `RERUN_MODE=auto` 启动，自动加载上次的失败用例列表
- HTTP server 就绪后，通过 GitLab API 自动触发所有 `failed`/`canceled` 的 `test-linux-*` job retry
- 无需手动逐个点击 worker 的 Retry

方法二（推送空 commit 触发全新 Pipeline，所有用例从头跑）：
```bash
git commit --allow-empty -m "ci: retrigger"
git push
```

方法三：在 GitLab Web UI → Build → Pipelines → Run Pipeline

> **边界情况说明**：
>
> | 操作顺序 | 行为 |
> |----------|------|
> | 只点 coordinator Retry | ✅ 推荐。coordinator API 触发所有失败 worker，所有失败用例重新分配 |
> | 只点某个 worker Retry | 该 worker 等待 coordinator 最多 600s；若 coordinator 未 retry 则超时失败（退出时有提示） |
> | 先点 worker 再点 coordinator | ✅ 正常。coordinator API 看到该 worker 已 running 会跳过它；worker 等到 coordinator 后直接连上，共同消费全部失败用例 |
>
> **注意**：coordinator 的失败用例队列来自全 pipeline 的 `results.json`，与哪个 worker 先 retry 无关；所有连上来的 worker 共同消费这份队列。

### Q8: 如何只跑测试阶段，跳过构建？

目前不支持。Pipeline 按阶段顺序执行，test 阶段依赖 upload-nexus（需要构建产物）。

如果你只修改了测试代码（`test/` 下），构建阶段会利用缓存快速完成（externals 缓存命中时 <1 分钟）。

### Q9: ASAN 报错了怎么看？

1. 在 artifacts 中找到对应用例目录：`results/logs/<slug>/sim/asan/`
2. ASAN 输出文件中包含内存错误详情（堆溢出、use-after-free 等）
3. 报告格式为：
   ```
   =================================================================
   ==12345==ERROR: AddressSanitizer: heap-buffer-overflow on address 0x...
       #0 0x... in function_name /path/to/file.c:123
       #1 0x... in caller_function /path/to/file.c:456
   ```
4. 关注 `#0` 和 `#1` 的文件路径和行号

### Q10: 构建镜像版本 `tdengine-ci:0.1` 是什么？

这是测试容器的 Docker 镜像，包含运行测试所需的依赖（Python、pytest、taos 连接器等）。
构建阶段使用的是 `harbor.tdengine.net/tsdb-builder/core:0.21-amd64`（当前版本 0.21）。
两个镜像用途不同：builder 用于编译，ci 用于运行测试。

---

## 11. GitHub PR 迁移到 GitLab MR

> **参考文档**: 飞书原文《[在 gitlab 和 github 同时开发](https://taosdata.feishu.cn/wiki/UzqlwFfqEiLVWwk5OUYc4YA7nFf)》  
> 如遇到具体问题（权限、分支策略变更等），请优先参考原文获取最新说明。

如果你在 GitHub 上有一个开发中的 PR（代码已在 GitHub 分支上），需要同时在 GitLab 上提 MR 并跑 CI，按以下步骤操作。

### 11.1 背景

| 平台 | 仓库 | 用途 |
|------|------|------|
| GitHub | `TDengine/TDengine`（或 TDinternal/community） | 公开社区仓库 |
| GitLab | `git.tdengine.net/rd-public/tsdb` | 内部 CI 仓库（本手册的目标） |

两个仓库代码同源，本地 clone 一份即可同时推送到两端。

### 11.2 前置准备（一次性）

**步骤 1：配置 SSH 公钥到 GitLab**

1. 打开 [https://git.tdengine.net/-/profile/keys](https://git.tdengine.net/-/profile/keys)
2. 把本机的 `~/.ssh/id_ed25519.pub`（或 `id_rsa.pub`）粘贴进去并保存
3. 验证：`ssh -T git@git.tdengine.net` → 看到 `Welcome to GitLab` 说明 OK

**步骤 2：在 GitLab 上创建开发分支**

在 `git.tdengine.net/rd-public/tsdb` 的 Web UI 上，基于 `main` 创建自己的开发分支，例如 `feat/my-feature`。

**步骤 3：把 GitLab 添加为本地仓库的第二个 remote**

```bash
# 进入本地已有的 GitHub clone 目录
cd /path/to/local/TDengine   # 或 TDinternal/community

# 查看当前 remote（一般只有 origin → GitHub）
git remote -v

# 添加 GitLab 为第二个 remote
git remote add gitlab git@git.tdengine.net:rd-public/tsdb.git

# 拉取两端最新分支信息
git fetch origin
git fetch gitlab
```

### 11.3 将 GitHub PR 的代码推送到 GitLab MR（具体示例）

**场景**: 你在 GitHub 上有 `feat/stream-usecase` 分支，已提 PR，现在要在 GitLab 上同步提 MR 并跑 CI。

```bash
# 1. 切到你的 GitHub 分支，确保是最新的
git checkout feat/stream-usecase
git pull origin feat/stream-usecase

# 2. 基于 GitLab 的 3.3.6 创建一个本地"桥接"分支
#    命名规范：gitlab-<你的分支名>，避免与 GitHub 分支冲突
git checkout -b gitlab-feat/stream-usecase gitlab/3.3.6

# 3. 把 GitHub 分支的改动 merge 进来
git merge feat/stream-usecase
# 若有冲突，解决后 git add . && git merge --continue

# 4. 推送到 GitLab 上对应的开发分支
git push gitlab gitlab-feat/stream-usecase:feat/stream-usecase

# 5. 在 GitLab Web UI 上创建 MR
#    Source: feat/stream-usecase → Target: 3.3.6
#    MR 创建后 CI Pipeline 自动触发
```

### 11.4 后续同步（GitHub 有新提交时）

每当 GitHub PR 有新提交，同步到 GitLab 只需：

```bash
# 更新 GitHub 分支
git checkout feat/stream-usecase
git pull origin feat/stream-usecase

# 切回桥接分支，重新 merge 并推送
git checkout gitlab-feat/stream-usecase
git merge feat/stream-usecase
git push gitlab gitlab-feat/stream-usecase:feat/stream-usecase
# GitLab MR 自动更新，旧 Pipeline 取消，新 Pipeline 触发
```

> **提示**: 如果改动频繁，也可以直接 `git rebase gitlab/3.3.6`（代替 merge）保持线性历史，推送时用 `--force-with-lease`。

### 11.5 分支命名建议

| 分支 | 存在于 | 命名示例 | 说明 |
|------|--------|---------|------|
| GitHub 开发分支 | GitHub remote (origin) | `feat/stream-usecase` | 正常开发分支，提 GitHub PR |
| GitLab 开发分支 | GitLab remote (gitlab) | `feat/stream-usecase` | 同名，在 GitLab 上提 MR |
| 本地桥接分支 | 本地 | `gitlab-feat/stream-usecase` | 只在本地使用，不推送到 GitHub |

### 11.6 完整命令速查

```bash
# ── 一次性配置 ──────────────────────────────────────────────────────────────
git remote add gitlab git@git.tdengine.net:rd-public/tsdb.git
git fetch gitlab

# ── 每次新 PR 首次同步 ───────────────────────────────────────────────────────
BRANCH="feat/my-feature"
git checkout ${BRANCH} && git pull origin ${BRANCH}
git checkout -b gitlab-${BRANCH} gitlab/3.3.6
git merge ${BRANCH}
git push gitlab gitlab-${BRANCH}:${BRANCH}
# → 在 GitLab 上创建 MR: Source=${BRANCH} → Target=3.3.6

# ── 后续追加提交同步 ─────────────────────────────────────────────────────────
git checkout ${BRANCH} && git pull origin ${BRANCH}
git checkout gitlab-${BRANCH}
git merge ${BRANCH}
git push gitlab gitlab-${BRANCH}:${BRANCH}
```

---

## 12. 磁盘清理机制

> **受众**：维护 CI 基础设施的人员。日常提 MR 开发不需要了解本节，但遇到磁盘告警或清理异常时可参考。

### 12.1 总体结构

CI 集群的磁盘消耗来自两类节点，各有独立的清理脚本：

| 节点类型 | 主要目录 | 清理脚本 |
|----------|---------|----------|
| **Coordinator（builder）** | `mr<N>/`、`daily-*/`、`web-*/`、`push-*/`、`coordinator-state/` | `tools/ci/scripts/cleanup-coordinator.sh` |
| **Worker（14 台）** | `job-<JOB_ID>/`、`fail-logs/` | `tools/ci/scripts/cleanup-worker.sh` |

两个脚本均已内置 `flock -n` 互斥锁，并发调用时只有一个实例实际运行，其余立即以 exit 0 退出，**不会产生冲突**。

---

### 12.2 Coordinator 节点清理

脚本：`tools/ci/scripts/cleanup-coordinator.sh`

#### Step 1 — 清理当前 pipeline 的 workspace

每次 pipeline 成功后，`cleanup-workspace` job（`when: on_success`）会删除本次 pipeline 使用的整个 workspace：

| 触发方式 | Workspace 路径 |
|----------|----------------|
| MR | `/data1/tdengine-ci/mr<IID>/` |
| schedule | `/data1/tdengine-ci/daily-<branch>-<YYYYMMDD>/` |
| web | `/data1/tdengine-ci/web-<pipeline_id>/` |
| push | `/data1/tdengine-ci/push-<branch>-<sha>/` |

**Workspace 内部结构**（以 MR 为例）：
```
/data1/tdengine-ci/mr<N>/
  tsdb/                     ← NoSan git checkout
    debug-others/           ← NoSan 编译产物 (~20G)   ← Step 1 删除
    source/ .git/ …         ← git checkout            ← Step 1 删除
  tsdb-san/                 ← ASAN git checkout + 编译产物 (~22G)  ← Step 1 整体删除
```
整棵树全部删除。下次同一 MR 推送新 commit 时，`prepare-workspace` 会重新 clone。

> **安全性**：test 阶段的 worker job 使用 `job-<JOB_ID>/` 目录（产物从 Nexus 下载），完全不依赖 `mr<N>/`，因此 cleanup stage 删除 workspace 是安全的。

#### Step 2 — Coordinator-wide 历史扫描

`cleanup-coordinator-sweep` job（`when: always, needs: []`）在每次 pipeline 启动时立即并行运行，调用 `cleanup-coordinator.sh` 扫描 coordinator 上**所有历史目录**。

**2a. `mr<N>/` 目录（通过 GitLab API 查询 MR 状态）**

| MR 状态 | 处理方式 |
|---------|----------|
| `merged` / `closed` | **始终**删除整个 `mr<N>/` 目录 |
| `open` 且有活跃进程（mtime < `ACTIVE_GRACE_MIN`，默认 30 分钟） | 跳过（保护正在运行的 pipeline） |
| `open`，空闲时间 ≥ `CLEANUP_KEEP_DAYS` 天 | 删除整个 `mr<N>/` 目录 |
| `open`，空闲时间 < `CLEANUP_KEEP_DAYS` 天 | 保留 |
| API 不可用（`unknown`），空闲时间 ≥ `CLEANUP_KEEP_DAYS` 天 | 删除整个目录 |
| API 不可用（`unknown`），空闲时间 < `CLEANUP_KEEP_DAYS` 天 | 保留 |

> **空闲时间判断**：使用 `mr<N>/` 目录自身的 mtime（最后一次在该目录下创建/删除子目录的时间，通常对应 `prepare-workspace` 完成时刻）。build 阶段写文件不改变 `mr<N>/` 的 mtime，因此空闲计时从 prepare 完成时就开始。

**2b. `daily-*/`、`web-*/`、`push-*/` 目录**

| 目录类型 | 处理方式 |
|---------|----------|
| `daily-<branch>-<YYYYMMDD>/` | 每个分支保留最新 `DAILY_KEEP_COUNT`（默认 3）个，其余删除 |
| `daily-<name>/`（无日期后缀，旧式/手动创建） | mtime > `CLEANUP_KEEP_DAYS` 天则删除 |
| `web-<ID>/`、`push-<branch>-<sha>/` | mtime > `CLEANUP_KEEP_DAYS` 天则删除 |

**2c. `coordinator-state/pipeline-*/` 目录**

mtime > `CLEANUP_KEEP_DAYS` 天则删除。

**GitLab API 认证**：脚本在 CI job 中通过内置变量 `CI_JOB_TOKEN` 调用 GitLab API 查询 MR 状态，无需额外配置。

---

### 12.3 Worker 节点清理

脚本：`tools/ci/scripts/cleanup-worker.sh`

每个 `cleanup-worker [*]` job 在本机执行，清理两类目录：

#### 1. `fail-logs/job-<N>/`（失败用例日志）

| 磁盘使用率（挂载点 `DISK_MOUNT`） | 保留时间 |
|----------------------------------|----------|
| < 90% | `CLEANUP_KEEP_DAYS` 天（默认 **5 天**） |
| ≥ 90%（剩余 < 10%） | `CLEANUP_KEEP_DAYS_URGENT` 天（默认 **3 天**） |

> **与 HTTP 文件服务的关系**：worker HTTP 文件服务（8899 端口，coordinator 日志中 `Runner logs:` 链接）从 `fail-logs/` 提供文件。日志保留期与 HTTP 可访问性一致——超过保留时间后目录删除，HTTP 链接失效。

#### 2. `job-<JOB_ID>/`（test job 工作目录）

| 磁盘使用率 | 保留时间 |
|-----------|----------|
| < 90% | `CLEANUP_KEEP_DAYS` 天（默认 **5 天**） |
| ≥ 90%（剩余 < 10%） | `CLEANUP_KEEP_DAYS_URGENT` 天（默认 **3 天**） |

跳过条件：检测到 `run-test-dynamic` 进程正在使用该目录时，跳过（正在运行的 job 不受影响）。

---

### 12.4 清理触发时机

```
pipeline stages:
  prepare → check → build → quality → verify → upload → test
                                                            ↓
                                                        cleanup
                  ┌──────────────────────────────────────────────────────────────────┐
                  │  cleanup-workspace         (when: on_success) ← 全部成功才执行    │ coordinator
                  │  cleanup-coordinator-sweep (when: always, needs:[]) ← 立即并行   │ coordinator
                  │  cleanup-worker [u3-141]   (when: always)                        │
                  │  cleanup-worker [u3-142]   (when: always)                        │ 14 台 worker
                  │  …                                                                │
                  └──────────────────────────────────────────────────────────────────┘
```

- `cleanup-workspace`（`when: on_success`）：全部测试通过后才执行，失败时**不删除** workspace，保留供重跑/调试
- `cleanup-coordinator-sweep`（`when: always, needs: []`）：立即与 test stage 并行运行，始终扫描历史目录
- `cleanup-worker [*]`（`when: always`）：始终运行，清理本机 job 目录和 fail-logs
- `allow_failure: true`：清理失败不影响 pipeline 最终状态
- 多个 MR 并发时：`cleanup-coordinator.sh` 内置 `flock -n`，同一时刻只有一个实例在运行，其余跳过

---

### 12.5 保留参数速查与调整方法

所有参数在 `.gitlab/.gitlab-ci.yml` 的全局 `variables:` 块中声明（可直接修改该文件，或在 GitLab UI 中覆盖）：

| 变量 | 默认值 | 含义 |
|------|--------|------|
| `CLEANUP_KEEP_DAYS` | `5` | **正常保留天数**（mr/fail-logs/job/state/daily/web/push），5 天覆盖周末 |
| `CLEANUP_KEEP_DAYS_URGENT` | `3` | **紧急保留天数**（磁盘用量 ≥ 90% 时启用，coordinator + worker 共用） |
| `DAILY_KEEP_COUNT` | `3` | 每个分支保留最新几个 `daily-<branch>-YYYYMMDD` 构建 |

#### 调整方式

**方式 1（推荐）：修改 `.gitlab-ci.yml`**
直接编辑 `.gitlab/.gitlab-ci.yml` 中 `variables:` 块，提交后对所有后续 pipeline 生效。

**方式 2：GitLab UI 临时覆盖**
GitLab → Settings → CI/CD → Variables 中添加同名变量，优先级高于 YAML 默认值，适合不想提交代码的临时调整。

**方式 3：手动 dry-run 验证**
两个脚本均支持 `DRY_RUN=1` 模式，只打印不删除，用于确认清理范围：
```bash
# 在 coordinator 上检查会删哪些目录（不实际删除）
DRY_RUN=1 WORKDIR=/data1/tdengine-ci \
  GITLAB_TOKEN="${CI_JOB_TOKEN}" \
  GITLAB_TOKEN_HEADER="JOB-TOKEN" \
  GITLAB_URL=https://git.tdengine.net \
  PROJECT_PATH=rd-public/tsdb \
  LOG_FILE=/dev/stdout \
  bash /usr/local/bin/ci-cleanup-coordinator

# 在 worker 上检查
DRY_RUN=1 WORKDIR=/data1/tdengine-ci LOG_FILE=/dev/stdout \
  bash /usr/local/bin/ci-cleanup-worker
```

---

## 附录

### A. Runner 机器列表

| 机器 | IP | 角色 | Tag | 说明 |
|------|------|------|-----|------|
| builder (u2-207) | 192.168.2.104 | 构建 + 协调 | `tsdb-builder` | 编译、unit-test、coordinator |
| u3-141 | 192.168.3.141 | Worker 1  | `TSDB-CI, u3-141` | |
| u3-142 | 192.168.3.142 | Worker 2  | `TSDB-CI, u3-142` | |
| u3-143 | 192.168.3.143 | Worker 3  | `TSDB-CI, u3-143` | |
| u0-210 | 192.168.0.210 | Worker 4  | `TSDB-CI, u0-210` | |
| u1-63  | 192.168.1.63  | Worker 5  | `TSDB-CI, u1-63`  | large-mem（大内存用例）|
| u1-59  | 192.168.1.59  | Worker 6  | `TSDB-CI, u1-59`  | large-mem（大内存用例）|
| u0-212 | 192.168.0.212 | Worker 7  | `TSDB-CI, u0-212` | |
| u1-47  | 192.168.1.47  | Worker 8  | `TSDB-CI, u1-47`  | large-mem（大内存用例）|
| u3-145 | 192.168.3.145 | Worker 9  | `TSDB-CI, u3-145` | |
| u3-146 | 192.168.3.146 | Worker 10 | `TSDB-CI, u3-146` | |
| u3-147 | 192.168.3.147 | Worker 11 | `TSDB-CI, u3-147` | |
| u2-205 | 192.168.2.205 | Worker 12 | `TSDB-CI, u2-205` | |
| u2-210 | 192.168.2.210 | Worker 13 | `TSDB-CI, u2-210` | |
| u2-217 | 192.168.2.217 | Worker 14 | `TSDB-CI, u2-217` | |

### B. 关键路径速查

| 路径 | 位置 | 说明 |
|------|------|------|
| `/data1/tdengine-ci/` | builder + worker | CI workspace 根目录（MR、日志、fail-logs 均在此）|
| `/data/cache/tsdb-builder/externals-others-amd64/` | builder | 第三方库缓存 |
| `/data1/tdengine-ci/fail-logs/job-<ID>/` | worker | 失败用例日志（默认 3 天，磁盘紧张时 2 天） |
| `/data1/tdengine-ci/job-<JOB_ID>/` | worker | test job 工作目录（默认 3 天，磁盘紧张时 2 天）|
| `tools/ci/scripts/cleanup-coordinator.sh` | 仓库内 | Coordinator 磁盘清理脚本 |
| `tools/ci/scripts/cleanup-worker.sh` | 仓库内 | Worker 磁盘清理脚本 |
| `source/taos-community/tests/parallel_test/cases.task` | 仓库内 | Legacy 用例配置文件 |
| `source/taos-community/test/ci/cases.task` | 仓库内 | 新框架（newfw）用例配置文件 |
| `source/taos-community/test/ci/run_case.sh` | 仓库内 | 容器内用例执行入口 |
| `source/taos-community/test/ci/run_container.sh` | 仓库内 | 容器启动脚本 |
| `scripts/coordinator.py` | 仓库内 | 测试调度协调器 |
| `scripts/run-test-dynamic.sh` | 仓库内 | Worker 端动态执行器 |
| `source/taos-community/test/ci/rerun.sh` | 仓库内 | 一键复现失败用例脚本 |

### C. 环境变量速查

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `TEST_CONCURRENCY` | 自动 | 每台 worker 的并发容器数 |
| `CASE_TIMEOUT` | 600s | 单用例超时 |
| `COORDINATOR_HOST` | 192.168.2.104 | 协调器地址（builder u2-207）|
| `WORKER_CAPS` | 空 | Worker 能力标签（`large-mem` 表示大内存机器）|
| `CI_NO_ASAN` | 1 | 禁用 LD_PRELOAD 注入（ASAN 内置于 debugSan 二进制）|
| `CI_BASE_DIR` | `/data1/tdengine-ci` | CI 工作目录根（builder + worker 共用）|
| `CI_SCHED_AGGR` | `1` | 调度激进程度：0=保守 / 1=中等（默认）/ 2=激进。控制 coordinator 分批阈值和 worker 并发上限 |
| `PROM_RATE_WINDOW` | `1m` | Prometheus `rate()` 窗口。`1m` 使负载数据在 1 分钟内反映，避免旧版 `5m` 的滞后 |
| `PROMETHEUS_URL` | `http://192.168.1.42:9090` | Prometheus 地址（coordinator 用于采集 worker 负载）|
| `CLEANUP_KEEP_DAYS` | `3` | 统一保留天数（fail-logs/、job-*/、state/、daily-*/、web-*/、push-*/）|
| `CLEANUP_KEEP_DAYS_WARN` | `2` | 磁盘 ≥ `DISK_WARN_PCT` 时的保留天数（worker 节点）|
| `IDLE_KEEP_HOURS` | `24` | open MR 构建产物空闲多少小时后清理（coordinator）|
| `DAILY_KEEP_COUNT` | `3` | 每分支保留最新几个 `daily-<branch>-YYYYMMDD` 构建 |
| `DISK_WARN_PCT` | `85` | worker 磁盘使用率警告阈值（%）|
| `DISK_SKIP_PCT` | `50` | 已用率低于此值时跳过所有非 merged/closed 清理（coordinator + worker 共用）|

