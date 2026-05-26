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

> 调用 `skill-telemetry`，传入 `name=tsdb-pr-patch-coverage version=0.1.0 author=copilot`。
> 失败不阻塞，但必须尝试。

## Input

| 参数 | 必需 | 默认值 | 说明 |
|---|:--:|---|---|
| `mr_url` | ⚠️ 二选一 | — | GitLab MR 链接，例如 `https://git.tdengine.net/rd-public/tsdb/-/merge_requests/254` |
| `base` + `head` | ⚠️ 二选一 | — | 直接给 BASE/HEAD revision（分支名、tag、commit SHA 均可） |
| `repo` | ❌ | `/root/code/tsdb` | tsdb 仓库根目录 |
| `pytest_files` | ❌ | 自动从 diff 检测 | 空格分隔的 pytest 路径（相对 `<repo>/source/taos-community/test/`） |
| `gtest_bins` | ❌ | 自动从 diff 检测 | 空格分隔的 gtest 二进制文件名（位于 `debug/build/bin/`） |
| `out_dir` | ❌ | `/tmp/tsdb-mr-cov-<id>` | 输出目录 |
| `include_session_unit_test` | ❌ | 询问 | 若 MR diff 里没有 gtest 但当前 session 添加了相关单测，是否纳入 |

> 若 `mr_url` / (`base`+`head`) 都没给，必须主动询问。

## Steps

### 第一步：解析 MR → BASE / HEAD / 测试清单

1. **如果给了 `mr_url`**：用 `gh`/`glab` CLI 或 GitLab API 拉 MR JSON，取 `source_branch` 与 `target_branch`。
   ```bash
   # 优先 glab（若已认证）
   glab mr view <id> --output json | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['source_branch'],d['target_branch'])"
   # 兜底：直接 curl GitLab API（需 PAT）
   curl -s --header "PRIVATE-TOKEN:$GITLAB_TOKEN" \
     "https://git.tdengine.net/api/v4/projects/<pid>/merge_requests/<id>" | jq -r '.source_branch + " " + .target_branch'
   ```
   若拿不到，**直接问用户** source/target 分支名。

2. **算 BASE**：`git merge-base <source_branch> <target_branch>`。**这一步至关重要**——直接用 `target_branch` 当 BASE 会把 target 上 MR 之后的提交也算成"被 MR 删掉"，diff 会乱。

3. **HEAD** = `source_branch` 的 tip。

4. **检测测试文件**（从 `git diff --name-status BASE..HEAD` 里挑）：
   - pytest：`test/cases/**/*.py` 且状态为 A 或 M
   - gtest：`source/**/test/*Test.cpp` 或 `source/**/tests/*Test.cpp` → 推断二进制名（去 `.cpp`）
   - 自动检测后**列出来给用户确认**，特别问：
     - "MR diff 里没有新增 gtest，但 session 里加了 `<XxxTest>`，要不要一并跑？"

### 第二步：环境前置（CRITICAL）

> ❌ **绝对禁止**跳过这一步。stale binary 是这个流程最容易踩的坑——上轮如果改过 .c 但没重链 `taosd`，跑出来的覆盖率会严重失真（实测过：13% vs 实际 84%）。

> ⚠️ **同样关键**：在跑 pytest 之前必须把测试框架的 taosd 终止信号从 `SIGKILL` 改成 `SIGTERM`（见下方"测试框架 SIGTERM 补丁"）。否则 libgcov 的 `__gcov_dump` 在 atexit 里来不及执行，绝大多数 case 跑完后 `.gcda` 不会被刷盘，patch coverage 会从真实值（~80%）跌到 10%~20% 区间，且看起来"测试都过了"——这是最难定位的覆盖率失真。

```bash
# 1. kill 残留 taosd（按 PID，禁用 pkill）
pids=$(pgrep taosd 2>/dev/null)
for p in $pids; do kill -9 "$p" 2>/dev/null; done

# 2. 强制重链 taosd（即使源码看起来"没变"）
(cd <repo>/source/taos-community/debug && cmake --build . --target taosd -j 4)

# 3. 验证 taosd mtime ≥ 所有相关 .o mtime
stat -c '%y' <repo>/source/taos-community/debug/build/bin/taosd

# 4. 给测试框架打 SIGTERM 补丁（见下方说明）
```

#### 测试框架 SIGTERM 补丁（覆盖率正确性的必要条件）

TDengine 自带的两条 pytest 路径在 stop / stopAll 时都用 `kill -9`（SIGKILL）干掉 taosd：

| 文件 | 函数 | 原始行为 | 补丁 |
|---|---|---|---|
| `test/new_test_framework/taostest/components/taosd.py` | `stop()` | `xargs kill -9` | 改 `kill -TERM` + 追加 `for n in $(seq 1 60); do ... ps ... wc -l ... [ "$c" = "0" ] && break; sleep 1; done` 同步等待最长 60s |
| `test/new_test_framework/utils/server/dnodes.py` | `TDDnodes.stop` / `TDDnodes.stopAll` 中两处 while-loop | `kill -9 %s` | `kill -TERM %s`（原循环里已有 `time.sleep(1)` 重试 + 再 ps 判存，等价于 wait-loop） |

**为什么**：

- `SIGKILL` 由内核直接终止进程，**不走** glibc 的 atexit 链。`libgcov_init` 在 `__attribute__((constructor))` 里挂的 `__gcov_dump` atexit handler 永远不被调用 → `.gcda` 文件不会被刷盘 → 这一轮 taosd 进程跑过的所有覆盖率全部丢失。
- `SIGTERM` 是可被 catch 的常规终止信号，glibc 默认 handler 会让 exit() 正常走完 atexit 链，`__gcov_dump` 把 in-memory counters merge 到 `.gcda`。
- `SIGTERM` 是异步信号，发完后必须**显式等待**进程退出再启动下一个 case 的 taosd——否则下一轮 taosd 启动时新进程的 `__gcov_init` 会把还没 flush 完的 `.gcda` 当 stale 处理（甚至并发写损坏）。`taosd.py` 的 60s wait-loop 就是为这个写的。

**症状识别**：跑完全套测试后 `lcov capture` 出来的 info 行数远小于预期（如 vnodeStream*.c 只 capture 到 200 行而代码 2800 行），patch coverage 落在 10%~25% 区间 → 八成是 SIGKILL 没改。

**应用方式**（已被本 skill `run_mr_coverage.sh` 自动 sed 在脚本起手处打入；如需手工跑）：

```bash
sed -i "s/xargs kill -9/xargs kill -TERM/" \
  <repo>/source/taos-community/test/new_test_framework/taostest/components/taosd.py
sed -i 's/kill -9 %s/kill -TERM %s/g' \
  <repo>/source/taos-community/test/new_test_framework/utils/server/dnodes.py
# wait-loop 追加见上表（手工 patch）
```

> 这两个改动**仅用于覆盖率采集场景**，不要提交到上游主线（生产用例希望 SIGKILL 更确定地清进程）。本仓库已用 `.gitignore` + 不 commit 约定隔离。

### 第三步：跑测试 + 抓 coverage（**单测先**，pytest 后）

> 顺序很重要：两个二进制（taosd / gtest）会同时写同一个 `.gcda` 文件，**并发写会损坏**。所以分两轮：
> 1. 清 `.gcda` → 跑 gtest → `lcov capture` 为 `ut.info` → 再清 `.gcda`
> 2. 跑 pytest → graceful 关 taosd → `lcov capture` 为 `pytest.info`
> 3. `lcov --add-tracefile ut.info --add-tracefile pytest.info` 合并

`scripts/run_mr_coverage.sh` 已封装了整个流程，直接调用即可：

```bash
bash <skill-dir>/scripts/run_mr_coverage.sh \
  --repo /root/code/tsdb \
  --build-dir /root/code/tsdb/source/taos-community/debug \
  --bin-dir   /root/code/tsdb/source/taos-community/debug/build/bin \
  --test-dir  /root/code/tsdb/source/taos-community/test \
  --base   $(git merge-base <src> <tgt>) \
  --head   <src> \
  --pytest-files "cases/.../foo.py cases/.../bar.py" \
  --gtest-bins   "fooTest barTest" \
  --out-dir  /tmp/tsdb-mr-cov-<id> \
  --skill-scripts <skill-dir>/scripts
```

### 第四步：生成与 HTML 同口径的 patch coverage

`run_mr_coverage.sh` 已经做了，但如果手动跑分步骤：

```bash
# 1. filter merged.info → only MR-touched source files
# 2. lcov2gcovr.py 把注释/`{}`/空行打 excluded 标记 + BB-coalescing 启发式回填
python3 <skill-dir>/scripts/lcov2gcovr.py merged.info coverage.gcovr.json

# 3. gcovr HTML
gcovr --json-add-tracefile coverage.gcovr.json \
  --html --html-details --html-theme blue \
  --root <repo> -o html/index.html --gcov-ignore-errors all

# 4. patch_cov.py（HTML 同口径：excluded 不计入分母）
python3 <skill-dir>/scripts/patch_cov.py \
  --repo <repo> --base <BASE> --head <HEAD> \
  --gcovr-json coverage.gcovr.json --out patch_coverage.txt
```

### 第五步：交付结果

向用户给出三个数字 + 一个链接：

```
✅ MR #<id> Patch Coverage

测试结果：
  pytest <files>             ✅ PASS / ❌ FAIL
  gtest  <bins>              ✅ N/N PASS

Patch coverage = <hit>/<exe> = <pct>%  (HTML 同口径)

最大 miss 文件：<file>: <ranges...>

HTML:   <out_dir>/html/index.html
报告:   <out_dir>/patch_coverage.txt
```

可选：在 `out_dir` 上起一个 `python3 -m http.server` 给用户直接看 HTML。

## Output

主要产物（全在 `--out-dir` 下）：

| 路径 | 说明 |
|---|---|
| `html/index.html` | gcovr 行级 HTML，注释/`{}` 已 excluded，BB-coalesced 行启发式回填 |
| `patch_coverage.txt` | 与 HTML 同口径的 patch coverage 文本报告（含 miss 行号区间） |
| `coverage.gcovr.json` | gcovr 中间格式 JSON（可重新喂给 gcovr 出其它格式） |
| `merged.info` | lcov 合并后的原始 info |
| `ut.info` / `pytest.info` | 两轮分别的 lcov info |
| `mr_src_files.txt` | MR diff 涉及的源文件列表 |
| `logs/` | relink / 单测 / pytest 各自的原始日志 |

**关键数字示意**：

```
Patch coverage = 1722/2081 = 82.7%  (HTML-aligned: excluded lines dropped)
```

含义：MR 新增的可执行行（剔除注释/`{}`/空行后）共 2081，其中 1722 被测试覆盖。

## Examples

**用户说**："https://git.tdengine.net/rd-public/tsdb/-/merge_requests/254 这个 MR 的覆盖率"

**Agent 行为**：

1. 调 GitLab API 拿到 source=`feat/6986382331`, target=`enh/tag-ref`
2. `git merge-base` → BASE=`ac53de75aa2`，HEAD=`feat/6986382331`
3. `git diff --name-status` 找出：
   - pytest 新增：`cases/18-StreamProcessing/02-Stream/stream_vtable_chain_ref.py`
   - gtest 新增：（无）
4. **询问**："session 里加了 `vnodeStreamVTableTest`，要不要一并跑？" → 用户答"要"
5. `kill -9 taosd` + `cmake --build . --target taosd` 重链
6. 跑 gtest → `lcov capture` → 清 gcda
7. 跑 pytest（约 6 分钟）→ kill -15 taosd 等 flush → `lcov capture`
8. 合并 → `lcov2gcovr.py` → `gcovr HTML` + `patch_cov.py`
9. 输出："**82.7%** (1722/2081)，最大 miss 在 `vnodeStreamVTable.c` 的 1067-1276 段错误处理路径"

## Directory Hints

- `scripts/lcov2gcovr.py` — lcov info → gcovr JSON，做两个 pass：
  - excluded 标记：注释 / `{}` / `/* */` / blank
  - BB-coalescing 启发式回填：把"明明顺序执行却显示 miss"的简单赋值行从相邻命中行继承计数
  - 头部 docstring 详细讲了为什么这些 pass 是必需的（GCC `.loc` 合并问题）
- `scripts/patch_cov.py` — 读 gcovr JSON + git diff，输出 patch coverage 文本报告
- `scripts/run_mr_coverage.sh` — 上面所有步骤的端到端编排

## Safety

- **禁止**：在用户没明确批准前 `git commit` / `git push` 任何变更
- **禁止**：跳过 "重链 taosd" 步骤（会导致 stale binary 测量错误，结果完全不可信）
- **禁止**：在 pytest 还在跑时 `pkill taosd`（项目禁用 pkill；统一用 `kill <PID>`）
- **禁止**：跑测试时把 `.gcda` 直接 `rm -rf` 整个 `debug/` 目录（会破坏 .gcno）
- **禁止**：用 `kill -9` / SIGKILL 终止 taosd（无论是 pytest 框架内还是手动），SIGKILL 跳过 atexit → `__gcov_dump` 不执行 → `.gcda` 丢失 → 覆盖率严重失真。必须用 SIGTERM，并显式等待进程退出。详见第二步"测试框架 SIGTERM 补丁"。
- **限制 scope**：
  - 写入仅 `--out-dir`、`<repo>/source/taos-community/sim/`、`<repo>/source/taos-community/debug/`（`.gcda`）
  - 不动 `source/` 下的源码、`test/` 下的测试用例（只读）
- **敏感数据**：不要把 GitLab PAT 写进任何输出文件或日志
- 若 lcov 报 `stamp mismatch with notes file`，说明 .gcno 比对应的二进制旧，**必须重编**才能继续；不要忽略警告硬抓
