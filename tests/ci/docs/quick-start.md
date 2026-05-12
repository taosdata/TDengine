# TDengine GitLab CI 快速入门


## 1. CI 是什么

每次向 `main`、`3.3.6` 或 `3.0` 分支提 MR，GitLab 会自动触发一条流水线：

1. **构建** — 编译 TDengine（普通 + ASAN 两份）
2. **检查** — 代码质量检查 + 单元测试
3. **测试** — 在多台 worker 机器上并行运行系统集成测试

流水线通过 → MR 可以合并；失败 → 需要排查并修复。

---

## 2. 提交 MR

```bash
# 1. 基于目标分支创建开发分支（以 main 为例，3.3.6 / 3.0 同理）
git checkout main && git pull
git checkout -b feat/my-feature

# 2. 开发完成后推送
git add -A && git commit -m "feat: xxx"
git push origin feat/my-feature

# 3. 在 GitLab 创建 MR
#    Source: feat/my-feature → Target: main（或 3.3.6 / 3.0）
#    MR 创建后 CI 自动启动，无需手动操作
```

> **触发条件**：目标分支为 `main`、`3.3.6`（含子版本）或 `3.0`（含子版本）的 MR 都会触发 CI。
> 仅修改文档、注释等非代码内容时，build/test 阶段会自动跳过，流水线快速结束。

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
3. Source branch 选 `feat/my-feature`，Target branch 选 `main`（或 `3.3.6` / `3.0`）
4. 填写标题和描述，点击 **Create merge request**
5. MR 创建后 CI 自动启动

> 建议在 MR 描述中附上对应 GitHub PR 链接，方便追踪。

---

## 4. 查看结果

**MR 页面 → Pipelines tab** — 看整体状态（✅ 通过 / ❌ 失败）

**MR 页面 → Tests tab** — 看失败了哪些测试用例（最直接）

**点击具体 Job** — 看实时日志

---

## 5. 流水线失败怎么办

### 5.1 先看 Tests tab

MR 页面 → Pipeline → **Tests tab**，直接列出失败用例名和错误摘要。

### 5.2 看 coordinator 日志

点击 `coordinator` job，搜索 `FAIL` 或 `❌`，每个失败用例有一个折叠 section：

```
▶ ❌ FAIL  test_benchmark_commandline.py  (worker: u3-141, exit=1, 125s)
  Runner logs: http://192.168.3.141:8899/job-12345/81-Tools.../
  AssertionError: expected 100 rows, got 0
```

点击 `Runner logs` 链接可在浏览器中查看完整日志（保留 7 天）。

### 5.3 判断是我的代码问题还是环境问题

| 现象 | 大概率原因 |
|------|----------|
| 失败用例与你的改动完全无关 | 环境偶发，重跑一次 |
| 失败用例刚好涉及你修改的模块 | 代码 bug，需修复 |
| exit=124（超时） | 可能偶发，先重跑 |
| 多个用例同时失败 | 可能是 taosd 崩溃，看 taosdlog |

### 5.4 重跑 Pipeline

方法一：Pipeline 页面右上角点 **Retry**（只重跑失败的 job）

方法二：推一个空 commit
```bash
git commit --allow-empty -m "ci: retrigger"
git push
```

---

## 6. 本地复现失败用例

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

**Q: Pipeline 没触发？**
检查 MR 目标分支是否为 `main`、`3.3.6` 或 `3.0`（含子版本），是否处于 Draft 状态。也可手动触发：Build → Pipelines → Run Pipeline。

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
