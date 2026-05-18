# TDengine GitLab CI 快速入门


## 1. CI 流水线概览

每次向 `main`、`3.0` 或 `3.3.6` 分支提 MR，GitLab 会自动触发一条流水线，共 8 个阶段：

1. **prepare** — 在 builder 机器上克隆源码（两份独立副本，分别用于 NoSan 和 ASAN 编译）
2. **check** — 代码检查（`check-void` 失败会阻塞构建；`check-enum` 设有 `allow_failure: true`，失败仅为警告，不阻塞 build）
3. **build** — 编译 TDengine（externals 哨兵快速通道決来 → NoSan 与 ASAN **真并行**编译）
4. **quality** — 代码质量检查（检查 assert 使用规范）
5. **verify** — 单元测试（CTest）
6. **upload** — 产物打包并上传 Nexus
7. **test** — 分布式集成测试（在 14 台 worker 上并行运行系统集成测试）
8. **cleanup** — 清理

流水线通过 → MR 可以合并；失败 → 需要排查并修复。

---

## 2. 提交 MR

```bash
# 1. 基于目标分支创建开发分支
git checkout 3.3.6 && git pull
git checkout -b feat/my-feature

# 2. 开发完成后推送
git add -A && git commit -m "feat: xxx"
git push origin feat/my-feature

# 3. 在 GitLab 创建 MR
#    Source: feat/my-feature → Target: 3.3.6
#    MR 创建后 CI 自动启动，无需手动操作
```

> **触发条件**：只有目标分支为 `main`、`3.0`（及子版本）、`3.3.6`（及子版本）的 MR 才会触发 CI。
>
> **build/test 阶段的实际执行条件**：即使触发了流水线，build 和 test 阶段也只在以下文件有变更时才会运行，否则 job 会被跳过，流水线快速结束：
> - C/C++/CMake 源码：`**/*.{c,h,cpp,cmake}`、`CMakeLists.txt`
> - 源码目录：`source/**`
> - CI 脚本：`scripts/**`
> - CI 配置：`.gitlab/.gitlab-ci.yml`
> - **用例列表**：`source/**/test/ci/cases.task`（修改用例列表也会触发完整流水线）
>
> 仅修改文档、注释等其他文件时，build/test 阶段会自动跳过。

---

## 3. 从 GitHub PR 迁移到 GitLab MR

如果你已经在 GitHub 上提了 PR，需要将对应分支迁移到内部 GitLab 并创建 MR 来触发 CI。

### 3.1 配置 remote

```bash
# 查看当前 remote
git remote -v

# 如果 origin 还指向 GitHub，将 GitLab 加为新 remote
git remote add gitlab <GitLab 仓库 SSH/HTTPS 地址>

# 如果 origin 已经指向 GitLab，则跳过此步
```

### 3.2 将分支推送到 GitLab

```bash
# 确保本地分支是最新的
git checkout feat/my-feature
git pull origin feat/my-feature   # 从 GitHub 拉最新

# 推送到 GitLab
git push gitlab feat/my-feature
```

> 如果是别人在 GitHub 上的 PR（非自己分支），先把对方分支拉到本地再推：
> ```bash
> # 用 PR 编号直接抓取
> git fetch origin pull/<GitHub-PR号>/head:feat/from-github-pr
> git push gitlab feat/from-github-pr
> ```

### 3.3 在 GitLab 创建 MR

1. 打开 GitLab 项目页面
2. **Merge Requests → New merge request**
3. Source branch 选 `feat/my-feature`，Target branch 选目标分支（`main`、`3.0` 或 `3.3.6`）
4. 填写标题和描述，点击 **Create merge request**
5. MR 创建后 CI 自动启动

> 建议在 MR 描述中附上对应 GitHub PR 链接，方便追踪。

---

## 4. 查看结果

**MR 页面 → Pipelines tab** — 看整体状态（✅ 通过 / ❌ 失败）

**MR 页面 → Tests tab** — 看失败了哪些测试用例（最直接）

**点击具体 Job** — 看实时日志

> **提示**：Test stage 中每个 job 名称直接包含执行机器（如 `coordinator [tsdb-builder-0]`、`test-linux-5 [u1-63]`），
> 无需点进去就能确认跑在哪台机器上（需 GitLab ≥ 15.9）。

---

## 5. 流水线失败怎么办

### 5.1 先看 Tests tab

MR 页面 → Pipeline → **Tests tab**，直接列出失败用例名和错误摘要。

### 5.2 看 coordinator 日志

点击 `coordinator` job，搜索 `FAIL` 或 `❌`，每个失败用例有一个折叠 section：

```
▶ FAIL [exit=1] [u1-47] .::./ci/pytest.sh pytest cases/05-VirtualTables/test_vtable_auth.py  (430.3s)
  ────────────────────────────────────────────────────────────────
  Case logs:   http://192.168.1.47:8899/job-11274/n8-05-VirtualTables_test_vtable_auth/run.log.txt
  Runner logs: http://192.168.1.47:8899/job-11274/n8-05-VirtualTables_test_vtable_auth/
  Fail dir:    root@192.168.1.47:/data1/tdengine-ci/fail-logs/job-11274/n8-05-VirtualTables_test_vtable_auth/
  复现方法:
  # workspace 在 builder 机器 u1-47 上，请 SSH 到该机器执行
  [本地非ASAN]
  cd /data1/tdengine-ci/mr199/tsdb && TAOS_BIN_PATH=$PWD/debug-others/build/bin ./tests/ci/scripts/run_case.sh --clean cases/05-VirtualTables/test_vtable_auth.py
  [容器ASAN]
  cd /data1/tdengine-ci/mr199/tsdb-san && ln -sfn debug-others debugSan 2>/dev/null; source/taos-community/test/ci/run_container.sh -w /data1/tdengine-ci/mr199/tsdb-san -s y -d . -c "./ci/pytest.sh pytest cases/05-VirtualTables/test_vtable_auth.py" -t 1
  摘要信息:
  AssertionError: expected 100 rows, got 0
  ────────────────────────────────────────────────────────────────
```

- **Case logs** — 直链到 `run.log.txt`，可在浏览器一键查看原始日志；超时/宕机用例也会显示完整 URL
- **Fail dir** — 失败用例日志保留路径（含 `sim/taosd` 日志），可 SSH 过去查看
- **复现方法** — 可直接复制粘贴执行的复现命令
  - `[本地非ASAN]` 宿主机直跑，快速
  - `[容器ASAN]` 容器内运行，与 CI 环境一致（推荐）

点击 `Case logs` 或 `Runner logs` 链接可在浏览器中查看完整日志（保留 7 天）。

> **Pipeline 快结束时**：当进度达到 98% 且队列已空，coordinator 日志会每隔 30s 打印一次
> 当前仍在运行的用例及已耗时，便于确认哪个慢用例在拉长 pipeline：
> ```
> [coordinator] ⏳ tail 2 in_flight case(s) (progress 913/915, elapsed=1401s):
> [coordinator]   [u3-146    ]   1040s  cases/99-Stress/test_stress_long.py
> [coordinator]   [u3-145    ]    587s  cases/05-VirtualTables/test_vtable_perf.py
> ```

> **`case-timing.txt`**：coordinator artifacts 中（无论成功失败都上传）包含 `results/case-timing.txt`，
> 列出本次所有用例及耗时（按耗时降序），方便快速定位慢用例和性能退化。

### 5.3 判断是我的代码问题还是环境问题

| 现象 | 大概率原因 |
|------|----------|
| 失败用例与你的改动完全无关 | 环境偶发，重跑一次 |
| 失败用例刚好涉及你修改的模块 | 代码 bug，需修复 |
| exit=124（超时） | 可能偶发，先重跑 |
| 多个用例同时失败 | 可能是 taosd 崩溃，worker job 日志末尾有自动 GDB 摘要（🔍 Coredump GDB 折叠 section），点击展开即可 |

### 5.4 重跑 Pipeline

**推荐：只点 coordinator Retry，自动触发所有失败 worker**

在 Pipeline 页面找到 `coordinator` job，点击 **Retry**：
- coordinator 以 `RERUN_MODE=auto` 启动，加载上次所有失败用例
- HTTP server 就绪后，自动调 GitLab API 触发所有 `failed`/`canceled` 的 `test-linux-*` job
- 无需逐个点击 worker 的 Retry

方法二：推一个空 commit（触发全新 Pipeline，所有用例从头跑）
```bash
git commit --allow-empty -m "ci: retrigger"
git push
```

> **边界情况**：
> - **先点 worker 再点 coordinator**：正常。coordinator API 看到该 worker 已是 `running` 状态会跳过它，worker 等到 coordinator 起来后直接连上，共同消费全部失败用例队列。
> - **只点某个 worker**：worker 会等待 coordinator 最多 10 分钟；若 coordinator 一直未出现则超时退出（日志有提示"请同时 retry coordinator"）。

---

## 6. 本地复现失败用例

### 6.1 直接从 Coordinator 输出复现（推荐）

coordinator 日志的 `复现方法:` 块已输出可直接粘贴的命令，无需手动查找路径：

```bash
# 1. SSH 到 builder 机器（IP 在折叠块标题行或 Fail dir 行可以看到）
ssh 192.168.1.47

# 2. 复制 [容器ASAN] 行（推荐，与 CI 环境一致）：
cd /data1/tdengine-ci/mr199/tsdb-san && ln -sfn debug-others debugSan 2>/dev/null; source/taos-community/test/ci/run_container.sh -w /data1/tdengine-ci/mr199/tsdb-san -s y -d . -c "./ci/pytest.sh pytest cases/05-VirtualTables/test_vtable_auth.py" -t 1

# 或复制 [本地非ASAN] 行（快速，无需 Docker）：
cd /data1/tdengine-ci/mr199/tsdb && TAOS_BIN_PATH=$PWD/debug-others/build/bin ./tests/ci/scripts/run_case.sh --clean cases/05-VirtualTables/test_vtable_auth.py
```

### 6.2 使用 rerun.sh 复现（适合旧 job）

在 runner 机器上，使用 `rerun.sh` 一键复现：

```bash
# 从 coordinator 日志的 Runner logs 链接中找到 slug（用例目录名）
slug="n1-01-DataTypes_test_datatype_bigint"

# 找到对应的 case.txt
case_txt=$(find /data1/tdengine-ci/fail-logs -name case.txt \
    -path "*/${slug}/case.txt" 2>/dev/null | sort -rV | head -1)
comm_dir=$(grep '^COMMUNITY_DIR=' "$case_txt" | cut -d= -f2-)

# 一键复现
${comm_dir}/test/ci/rerun.sh --case "$slug"
```

> 如果是 main 分支的失败用例，可以用 `--mr <MR号>` 下载对应构建产物来复现：
> ```bash
> ${comm_dir}/test/ci/rerun.sh --mr 147 --case "$slug"
> ```

---

## 7. 常见问题

**Q: taosd 崩溃（core dump）怎么分析？**
在失败用例对应的 worker job 日志（如 `test-linux-5 [u1-63]`）末尾，会有一个折叠的 `🔍 Coredump GDB` section，展开可直接看 `thread apply all bt` 输出，无需 SSH 到 worker。
完整的 core 文件和 `gdb-bt-*.txt` 保留在 HTTP retain 目录（`Runner logs:` 链接下的 `coredump/` 子目录）。详见 [ci-guide.md §7.4–7.6](ci-guide.md)。

**Q: Pipeline 没触发？**
检查 MR 目标分支是否为 `main`、`3.0` 或 `3.3.6`，是否处于 Draft 状态。也可手动触发：Build → Pipelines → Run Pipeline。

**Q: build 失败了？**
点开 `build-noasan` 或 `build-asan` job 日志，通常是编译报错，按错误信息修复代码即可。

**Q: 单元测试（unit-test）失败？**
日志中搜索 `❌`，展开对应 section 查看输出。下载 artifacts 里的 `ctest.log` 可看完整信息。

**Q: 有些用例一直是注释掉的状态？**
cases.task 里有部分用例因不稳定或兼容性问题被临时注释，已登记工作项跟进修复。如果你的改动涉及这些用例，可联系 CI 维护人员协商处理。

**Q: 多人同时提 MR 会互相影响吗？**
不会。每条 MR 有完全独立的构建目录、容器命名和 Nexus 路径。

---

## 8. 需要更多帮助

- 详细操作和调试手册：[ci-guide.md](ci-guide.md)
- CI 维护问题：联系平台组
