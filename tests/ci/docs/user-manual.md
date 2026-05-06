# TDengine CI 用户手册

> **适用对象**: 所有使用 TSDB 仓库的研发人员  
> **CI 系统**: GitLab CI/CD（`.gitlab/.gitlab-ci.yml`）  
> **Runner 集群**: 1 台 builder (tsdb-builder) + 7 台 test worker

---

## 目录

1. [流水线概览](#1-流水线概览)
2. [提交 MR 与触发 CI](#2-提交-mr-与触发-ci)
3. [Pipeline 触发规则](#3-pipeline-触发规则)
4. [流水线各阶段说明](#4-流水线各阶段说明)
5. [查看 Pipeline 状态与日志](#5-查看-pipeline-状态与日志)
6. [失败用例诊断](#6-失败用例诊断)
7. [Core Dump 文件查找](#7-core-dump-文件查找)
8. [本地复现与调试单个用例](#8-本地复现与调试单个用例)
9. [cases.task 用例配置](#9-casestask-用例配置)
10. [常见问题 FAQ](#10-常见问题-faq)

---

## 1. 流水线概览

每次触发的 CI Pipeline 包含 7 个阶段，按顺序执行：

```
prepare → build → quality → verify → upload → test → cleanup
                                                  │
                                                  ├── coordinator (调度器)
                                                  ├── test-linux-1 (u3-141)
                                                  ├── test-linux-2 (u3-142)
                                                  ├── test-linux-3 (u3-143)
                                                  ├── test-linux-4 (u0-210)
                                                  ├── test-linux-5 (u1-63, large-mem)
                                                  ├── test-linux-6 (u1-59, large-mem)
                                                  └── test-linux-7 (u0-212)
```

| 阶段 | Job | 说明 |
|------|-----|------|
| prepare | prepare-workspace | 克隆源码到 builder 机器 |
| build | build-externals | 构建第三方库 |
| build | build-noasan + build-asan | 编译 TDengine 主体（并行） |
| quality | check-assert + check-void | 代码质量检查（并行） |
| verify | unit-test | CTest 单元测试 |
| upload | upload-nexus | 打包上传产物到 Nexus |
| test | coordinator + test-linux-1~7 | 系统集成测试 |

---

## 2. 提交 MR 与触发 CI

### 2.1 提交 MR

```bash
# 1. 基于 main 创建分支
git checkout main && git pull
git checkout -b feat/my-feature

# 2. 开发 & 提交
git add -A && git commit -m "feat: add xxx"

# 3. 推送到远端
git push origin feat/my-feature

# 4. 在 GitLab Web UI 上创建 Merge Request
#    Source: feat/my-feature → Target: main
```

### 2.2 Pipeline 自动触发

创建目标为 `main` 分支的 MR 后，Pipeline **自动触发**。你不需要做任何额外操作。

> **注意**: 当前仅接受目标分支为 `main` 的 MR 触发 CI，其他分支的 MR 不会触发流水线。

### 2.3 推送新 commit

向已有 MR 推送新 commit 时：
- **旧 pipeline 自动取消**（`auto_cancel: on_new_commit: interruptible`）
- **新 pipeline 自动启动**
- 无需手动操作

### 2.4 手动触发（Web）

> **当前已禁用**: Web/Schedule/Push 触发方式已暂时注释，仅保留 MR 触发。如需恢复，请修改 `.gitlab/.gitlab-ci.yml` 的 `workflow.rules`。

<!--
在 GitLab 仓库页面 → **Build → Pipelines → Run Pipeline**：
- 选择分支
- 可选设置变量（如 `RELEASE_VERSION`）
- 点击 "Run Pipeline"
-->

---

## 3. Pipeline 触发规则

| 触发方式 | 条件 | 说明 |
|----------|------|------|
| **MR 事件** | 创建/更新 MR，目标分支为 `main` | 仅当目标为 main 且有代码变更时触发 |
| ~~**Push**~~ | ~~推送到 `main`, `3.0`, `3.3.6`, `3.3.6.citest`~~ | ~~保护分支推送自动触发~~（已注释） |
| ~~**定时调度**~~ | ~~Schedule~~ | ~~按 GitLab 配置的 Schedule 执行~~（已注释） |
| ~~**手动**~~ | ~~Web UI~~ | ~~随时可手动触发~~（已注释） |

> **注意**: MR pipeline 只有源分支涉及代码文件变更才会触发 build/test 阶段。如果你只修改了文档（`.md`），build/test 阶段会被跳过。

---

## 4. 流水线各阶段说明

### 4.1 prepare-workspace

- **作用**: 在 builder 机器上克隆两份源码（NoSan 用和 ASAN 用）
- **输出**: `build.env` 包含 `WORKSPACE`、`TSDB_SRC`、`TSDB_SRC_SAN` 路径
- **workspace 路径规则**:
  - MR: `/data1/tdengine-ci/mr<IID>/`
  - 定时: `/data1/tdengine-ci/daily-<branch>-<date>/`
  - 手动: `/data1/tdengine-ci/web-<pipeline_id>/`
  - Push: `/data1/tdengine-ci/push-<branch>-<sha>/`

### 4.2 build — 三步构建

```
build-externals (串行，缓存加速)
       ↓ needs
build-noasan ←─── build-asan
       (并行)           (并行)
```

- **build-externals**: 构建 RocksDB、libuv、curl、zlib 等第三方库。使用 flock 互斥锁防止并发写入。缓存命中时极快。
- **build-noasan**: 编译 TDengine 主体（不含 ASAN），产出测试二进制。
- **build-asan**: 编译 TDengine 主体（内置 ASAN 地址检查），不含测试二进制。

### 4.3 quality — 代码质量检查

- **check-assert**: 运行 `count_assert.py`，检查代码中的 assert 使用规范
- **check-void**: 运行 `check_void.sh`，检查返回值忽略的函数调用

**任意一个失败，后续所有阶段都会被终止。**

### 4.4 verify — 单元测试

- 在 builder 上的 Docker 容器内运行 `ctest`
- 执行 `clientTest` 和 `connectOptionsTest`
- 超时限制: 600 秒/用例，整体 1200 秒
- 产出 `junit.xml` 和 `ctest.log`

### 4.5 upload — 产物打包上传

将 NoSan 和 ASAN 两份构建产物分别打包为 tar.gz，上传到 Nexus：

```
Nexus 路径:
  MR:     tsdb/ci/mr<IID>/linux/x64/noasan/linux-x64-noasan.tar.gz
                                     asan/linux-x64-asan.tar.gz
  定时:   tsdb/daily/<date>/<branch>/linux/x64/...
  手动:   tsdb/release/<version>/linux/x64/...
```

### 4.6 test — 分布式集成测试

这是耗时最长的阶段，由 **1 个 coordinator + 7 个 worker** 组成：

- **coordinator** 运行在 builder 上，解析 `cases.task`，通过 HTTP API 动态分发用例
- **test-linux-1~7** 运行在各 worker 上，从 coordinator 拉取用例并执行
- 每个 worker 内部支持多容器并发（自适应计算并发数）
- Prometheus 监控各 worker 负载，空闲机器多分配用例

---

## 5. 查看 Pipeline 状态与日志

### 5.1 进入 Pipeline 页面

1. 打开 MR 页面
2. 找到 **Pipelines** tab，点击 pipeline ID 或状态图标
3. 或者: 仓库页面 → **Build → Pipelines**

### 5.2 Pipeline 视图

Pipeline 页面展示所有 Job 的状态：

```
✅ prepare-workspace
✅ build-externals
✅ build-noasan    ✅ build-asan
✅ check-assert    ✅ check-void
✅ unit-test
✅ upload-nexus
🔄 coordinator
🔄 test-linux-1  ❌ test-linux-2  ✅ test-linux-3 ...
```

| 图标 | 含义 |
|------|------|
| ✅ | 成功 |
| ❌ | 失败 |
| 🔄 | 运行中 |
| ⏸ | 等待依赖 |
| 🚫 | 已取消 |

### 5.3 查看 Job 日志

1. 点击任意 Job 名称（如 `test-linux-2`）
2. 进入 Job 详情页，展示实时日志
3. 右侧面板显示 **Duration**（耗时）和 **Runner**（执行机器）

### 5.4 查看 JUnit 测试报告

在 **MR 页面 → Pipeline → Tests** tab，GitLab 自动聚合所有 JUnit XML 报告：
- 显示 Pass / Fail / Error 数量
- 失败用例可直接展开查看

---

## 6. 失败用例诊断

### 6.1 失败信息层级（由浅入深）

```
Level 1: MR Pipeline 页面 → Tests tab     ← 最快速
Level 2: coordinator job 日志              ← 全局视角，折叠式错误摘要
Level 3: test-linux-N job 日志             ← 具体 worker 的执行日志
Level 4: Job Artifacts (results/logs/)     ← 完整日志 + taosd 日志
Level 5: Runner HTTP 文件服务              ← 失败用例目录浏览
```

### 6.2 Level 1 — Tests Tab（推荐首选）

1. 打开 MR 页面
2. 点击 Pipeline → **Tests** tab
3. 失败用例列表会直接展示
4. 点击用例名可展开查看失败详情

### 6.3 Level 2 — Coordinator 日志

`coordinator` Job 收集所有 worker 的失败信息，统一展示：

1. 点击 `coordinator` Job
2. 在日志中搜索 **`FAIL`** 或 **`❌`**
3. 每个失败用例有一个**折叠 section**（红色标题），点击展开：

```
▶ ❌ FAIL [1/3] test_benchmark_commandline.py  (worker: u3-141, 125.3s, exit=1)
  ──────────────────────────────────────────────
  Runner logs: http://192.168.3.141:8899/job-12345/81-Tools__03-Benchmark__test_benchmark_commandline/
  
  FAILED tests/cases/81-Tools/03-Benchmark/test_benchmark_commandline.py::TestBenchmarkCommandline::test_basic
  AssertionError: expected 100 rows, got 0
  ...
  ──────────────────────────────────────────────
```

**关键信息**:
- **worker**: 执行该用例的机器
- **exit code**: 退出码（124 = 超时，1 = 断言失败，137 = 被 kill）
- **Runner logs 链接**: 点击可在浏览器中查看完整日志目录

### 6.4 Level 3 — Worker Job 日志

点击具体的 `test-linux-N` Job，日志中包含该 worker 执行的所有用例：

```
------------------------------------------------------------
  [seq=12] [san=y] .::./ci/pytest.sh pytest cases/01-DataTypes/test_datatype_bigint.py
  => PASS (23.456s)
------------------------------------------------------------
  [seq=13] [san=n] .::pytest cases/81-Tools/03-Benchmark/test_benchmark_basic.py
  => FAIL (exit=1, 45.678s)
```

### 6.5 Level 4 — Artifacts（完整日志）

每个 Job 在完成后会上传 artifacts：

1. 在 Job 详情页右侧，找到 **Job artifacts → Browse**
2. 目录结构：

```
results/
  junit-N.xml                          ← JUnit 报告
  logs/
    <用例slug>/                        ← 按用例名分目录
      case.txt                         ← 用例执行完整日志
      sim/
        psim/log/                      ← 伪 sim 日志
        dnode1/log/taosdlog.*          ← taosd 日志（重要！）
        dnode1/cfg/taos.cfg            ← taosd 配置
        dnode2/log/...
        var_taoslog/                   ← /var/log/taos/ 拷贝
        asan/                          ← ASAN 输出（如有）
```

**重点文件**:
- `case.txt` — 用例执行的标准输出/标准错误，最重要的诊断文件
- `dnode*/log/taosdlog.*` — taosd 的运行日志，排查服务端问题
- `asan/` — ASAN 内存错误报告（如果用例使用了 ASAN 二进制）

### 6.6 Level 5 — Runner HTTP 文件服务

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

CI 的 `after_script` 会尝试收集 coredump 相关信息到 artifacts：

1. 在 Job 详情页 → **Browse artifacts**
2. 查看 `results/logs/<用例slug>/sim/` 目录
3. coredump 文件通常命名为 `core.<进程名>.<pid>` 或 `core.<pid>`

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
/tmp/core.%e.%p
# %e = 可执行文件名, %p = PID
# 例如: /tmp/core.taosd.12345
```

### 7.5 实战示例：从失败日志到 Core Dump 分析

以下是一个完整的实际排查流程，以 `test_udf_create.py` 用例失败为例：

**1) CI 日志中看到失败信息**

```
Failed cases:
  [u3-142] exit=1  405.658s  .::pytest cases/12-UDFs/test_udf_create.py
    http://192.168.3.142:8899/job-3854/n2-12-UDFs_test_udf_create/
```

**2) 浏览器打开 Runner HTTP 链接**

打开 `http://192.168.3.142:8899/job-3854/n2-12-UDFs_test_udf_create/` 可以看到：

```
build/
coredump/
run.log.txt
sim/
```

其中 `coredump/` 目录下有 core 文件：

```
core.taosd.200
core.taosudf.229
```

**3) SSH 到 worker 使用 GDB 分析**

```bash
# 1. 登录对应 worker
ssh 192.168.3.142

# 2. 进入失败日志的 coredump 目录
cd /data1/tdengine-ci/fail-logs/job-3854/n2-12-UDFs_test_udf_create/coredump

# 3. 使用 GDB 分析 taosd 的 core 文件
#    二进制文件在 fail-logs 同级的 _shared_bin/ 目录下
gdb ../../_shared_bin/taosd core.taosd.200

# 4. 分析其他进程的 core 文件（同理）
gdb ../../_shared_bin/taosudf core.taosudf.229
```

> **路径说明**:
> - 失败日志保留目录: `/data1/tdengine-ci/fail-logs/job-<JOB_ID>/<用例slug>/`
> - 共享二进制目录: `/data1/tdengine-ci/fail-logs/job-<JOB_ID>/_shared_bin/`
> - core 文件命名格式: `core.<进程名>.<PID>`
> - 日志保留 7 天，过期自动清理

### 7.6 使用 GDB 分析 Core Dump

```bash
# 在 worker 上直接分析（需要对应的二进制）
gdb /data1/tdengine-ci/job-<JOB_ID>/debugNoSan/build/bin/taosd \
    /path/to/core.taosd.12345

# 或者在 fail-logs 保留目录中分析
gdb /data1/tdengine-ci/fail-logs/job-<JOB_ID>/_shared_bin/taosd \
    /data1/tdengine-ci/fail-logs/job-<JOB_ID>/<用例slug>/coredump/core.taosd.<PID>

# GDB 内常用命令:
(gdb) bt        # 查看调用栈
(gdb) bt full   # 查看完整调用栈（含局部变量）
(gdb) info threads   # 查看所有线程
(gdb) thread N       # 切换到线程 N
(gdb) bt             # 查看该线程的调用栈
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

### 8.2 使用 run_container.sh 复现 CI 环境

`run_container.sh` 是 CI 实际使用的容器执行脚本，可以在 worker 上手动调用来精确复现 CI 环境：

```bash
# 前提：需要在已有 CI 产物的 worker 上执行
# WORKDIR 需要包含 debugNoSan/ 和/或 debugSan/ 目录

# 参数说明:
#   -w  工作目录
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

### 8.3 使用 run-test-batch.sh 运行一批用例

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

### 8.4 进入 CI 容器交互式调试

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

### 8.5 单元测试本地运行

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

---

## 10. 常见问题 FAQ

### Q1: Pipeline 没有触发？

**可能原因**:
- 你的修改只涉及文档/非代码文件（`.md` 等），不满足 `rules-code-change` 规则
- MR 还处于 Draft 状态
- 分支名不在保护分支列表中（`main`, `3.0`, `3.3.6`）

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

方法一（推荐）：在 Pipeline 页面，点击右上角 **Retry** 按钮（只重跑失败的 Job）

方法二：推送一个空 commit 触发新 Pipeline：
```bash
git commit --allow-empty -m "ci: retrigger"
git push
```

方法三：在 GitLab Web UI → Build → Pipelines → Run Pipeline

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

## 附录

### A. Runner 机器列表

| 机器 | IP | 角色 | Tag | 说明 |
|------|------|------|-----|------|
| builder | 192.168.2.104 | 构建 + 协调 | `tsdb-builder` | 编译、unit-test、coordinator |
| u3-141 | 192.168.3.141 | Worker 1 | `TSDB-CI, u3-141` | |
| u3-142 | 192.168.3.142 | Worker 2 | `TSDB-CI, u3-142` | |
| u3-143 | 192.168.3.143 | Worker 3 | `TSDB-CI, u3-143` | |
| u0-210 | 192.168.0.210 | Worker 4 | `TSDB-CI, u0-210` | |
| u1-63  | 192.168.1.63  | Worker 5 | `TSDB-CI, u1-63`  | large-mem |
| u1-59  | 192.168.1.59  | Worker 6 | `TSDB-CI, u1-59`  | large-mem |
| u0-212 | 192.168.0.212 | Worker 7 | `TSDB-CI, u0-212` | |

### B. 关键路径速查

| 路径 | 位置 | 说明 |
|------|------|------|
| `/data1/tdengine-ci/` | builder + worker | CI 工作根目录 |
| `/data/cache/tsdb-builder/externals-core-amd64/` | builder | 第三方库缓存 |
| `/data1/tdengine-ci/fail-logs/job-<ID>/` | worker | 失败用例日志保留（7天） |
| `source/taos-community/test/ci/cases.task` | 仓库内 | 用例配置文件 |
| `source/taos-community/test/ci/run_case.sh` | 仓库内 | 容器内用例执行入口 |
| `source/taos-community/test/ci/run_container.sh` | 仓库内 | 容器启动脚本 |
| `tests/ci/scripts/coordinator.py` | 仓库内 | 测试调度协调器 |
| `tests/ci/scripts/run-test-dynamic.sh` | 仓库内 | Worker 端动态执行器 |

### C. 环境变量速查

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `TEST_CONCURRENCY` | 自动 | 每台 worker 的并发容器数 |
| `CASE_TIMEOUT` | 600s | 单用例超时 |
| `COORDINATOR_HOST` | 192.168.2.104 | 协调器地址 |
| `WORKER_CAPS` | 空 | Worker 能力标签 |
| `CI_NO_ASAN` | 1 | 禁用 LD_PRELOAD 注入 |
