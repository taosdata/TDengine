# docs CI 服务器端脚本

本目录包含 docs CI 在 **GitLab Runner / Docker 容器内**实际执行的脚本，以及构建运行环境用的 Dockerfile。

> 文档贡献者本地复现请使用 [`local-validate.sh`](./local-validate.sh)（使用说明见 [`LOCAL-VALIDATE.md`](./LOCAL-VALIDATE.md)），那是这套脚本的包装器。

---

## 流水线架构

```text
tsdb 仓库根 .gitlab-ci.yml                       (父流水线 / 调度入口)
└── 触发 tsdb-docs job
    └── .gitlab/tsdb-build-docs.yml              (子流水线 / docs CI)
        ├── stage: prepare
        │   └── prepare-workspace.sh             准备 tsdb + 两个 docs 框架仓库
        ├── stage: lint
        │   ├── check-typos.sh                   拼写
        │   ├── check-autocorrect.sh             空格/标点
        │   └── check-markdownlint.sh            markdown 格式
        └── stage: build
            └── build-doc.sh                     yarn ass local && yarn build
```

所有 lint / build job 都通过 `run-in-docker.sh` 在统一的 docker 镜像里跑，runner 宿主机不需要装任何文档相关工具。

### 父子分离的意义

- 父级 `.gitlab-ci.yml` 用 `rules:changes` 判断「这次 MR 是不是只改了文档」，是的话**只**触发 docs 子流水线，跳过代码构建（节省 runner 资源）
- 反之亦然：纯代码 MR 不会跑 docs 检查
- 父级跟随 main 分支演进新增更多子流水线（比如 connector、tools）时互不干扰

---

## 文件清单

| 文件 | 作用 |
|------|------|
| `Dockerfile` | 构建 `docs-ci` 镜像（typos / autocorrect / markdownlint / Node.js 等） |
| `common.sh` | 共享变量与函数（路径、`changed_doc_files`、`changed_markdown_files` 等） |
| `prepare-workspace.sh` | 准备工作区，确保 tsdb + zh/en docs 框架就位 |
| `run-in-docker.sh` | docker run 包装器，统一挂载、环境变量、工作目录 |
| `check-typos.sh` | 调 typos 检查 |
| `check-autocorrect.sh` | 调 autocorrect 检查 |
| `check-markdownlint.sh` | 调 markdownlint-cli2 检查 |
| `autofix.sh` | autocorrect --fix + markdownlint --fix（仅 `--fix` 时调用） |
| `build-doc.sh` | yarn assemble + build，验证文档站点能编出来 |

---

## 关键设计

### 1. 工作区路径

工作区根目录由环境变量决定：

```text
${DOCS_CI_WORKDIR_BASE}/slot-${CI_CONCURRENT_PROJECT_ID}/
├── tsdb/                  ← 当前 MR 检出
├── docs.taosdata.com/     ← 中文站点框架
└── docs.tdengine.com/     ← 英文站点框架
```

- 默认 `DOCS_CI_WORKDIR_BASE=/root/gitlab_doc_ci_work`，可在 GitLab CI/CD 变量里覆盖
- **slot 隔离**：用 `CI_CONCURRENT_PROJECT_ID` 给每个并发槽位分独立目录，避免两个 pipeline 同时跑时互相覆盖 docs.* 仓库
- `prepare-workspace.sh` 幂等：已存在的仓库只 `git fetch + reset --hard`，不重新克隆

### 2. 增量检查

`common.sh` 提供两个核心函数：

- `changed_doc_files`：列出 MR 范围内 `source/taos-community/docs/**` 下变更的所有文件
- `changed_markdown_files`：上面再过滤 `.md` / `.mdx`

依赖 GitLab 注入的 `CI_MERGE_REQUEST_DIFF_BASE_SHA` 和 `CI_COMMIT_SHA`。**如果这俩变量未设置**（非 MR pipeline），函数会打印 sentinel 路径退化为"全量构建"，避免漏检；本地校验时由 `local-validate.sh` 用 `git merge-base` 算出再注入。

### 3. docker 镜像

`Dockerfile` 基于 `node:24-bookworm-slim`，主要工具：

| 工具 | 安装方式 | 版本 ARG |
|------|---------|---------|
| `markdownlint-cli2` | npm 全局 | `MARKDOWNLINT_VERSION` |
| `autocorrect` | npm 全局 (`autocorrect-node`，NAPI.RS 二进制) | `AUTOCORRECT_VERSION` |
| `typos` | GitHub Releases 预编译 musl 静态二进制 | `TYPOS_VERSION` |

选型考虑：
- 不用 Rust 工具链编译 typos / autocorrect，镜像只多 ~30 MB
- 镜像内 `apt` 走内部 Nexus 代理，国内 runner 拉包更快
- 基础包只装 `git curl ca-certificates python3 make g++`（node-gyp 备用）

构建方式：

```bash
docker build \
  --build-arg TYPOS_VERSION=v1.46.1 \
  --build-arg AUTOCORRECT_VERSION=2.14.0 \
  --build-arg MARKDOWNLINT_VERSION=0.22.1 \
  -t docs-ci:local \
  -f .gitlab/scripts/tsdb-docs-ci/Dockerfile \
  .gitlab/scripts/tsdb-docs-ci/
```

也可以由 CI 自动 build & push，CI 端通过 `DOCS_CI_IMAGE` 变量指定要用哪个 tag。

### 4. run-in-docker.sh

统一负责：
- `docker run --rm`
- 挂载工作区目录到容器内**同名路径**（保证容器内/外路径一致，方便错误信息直接跳转）
- 透传 CI 相关环境变量（`CI_COMMIT_SHA`、`CI_MERGE_REQUEST_DIFF_BASE_SHA` 等）
- 工作目录设为 `${TSDB_DIR}`

特殊处理：build-doc job 调用前会先 `prepare_docs_repo_on_host` 把两个 docs 框架仓库 reset 到最新，因为容器内是只读挂载。

---

## 添加 / 修改检查

### 加一个新的检查工具

1. 编辑 `Dockerfile` 装新工具
2. 在本目录新增 `check-foo.sh`，参考 `check-typos.sh` 写法
3. 在 `.gitlab/tsdb-build-docs.yml` 加 job 调用它
4. （可选）在 `autofix.sh` 加入自动修复支持
5. 重新 build & push docs-ci 镜像并更新 `DOCS_CI_IMAGE`

### 修改某个检查的范围

`common.sh` 的 `changed_doc_files` 函数控制扫描范围，目前限定 `source/taos-community/docs`。

### 项目名词加白名单

typos 用 `source/taos-community/docs/typos.toml`：

```toml
[default.extend-words]
TDengine = "TDengine"
taosd = "taosd"
```

---

## 常见问题

### CI 报 `dubious ownership`

`build-doc.sh` 里加了：

```bash
git config --global --add safe.directory '*'
```

因为容器内是 root，宿主机文件是 gitlab-runner，git 2.35+ 默认拒绝跨用户访问。该配置只影响容器内 git。

### tsdb-docs job 不触发

父 `.gitlab-ci.yml` 里 tsdb-docs 的 `rules:changes` 用了 `**/*` 模式（不能只写 `**`，否则不匹配任何文件）。详见父级 yaml 顶部的注释和 [MR !296/!300](https://git.tdengine.net/rd-public/tsdb/-/merge_requests) 的排查记录。

### pipeline 提示 `pipeline would be empty`

`trigger: strategy: depend` 触发的子 pipeline 没有任何 job 匹配。检查改动的文件是否在子流水线某个 job 的 `changes` 列表里。比如：纯改父 `.gitlab-ci.yml` 时，子流水线的 `.rules-code-change` 也要包含 `.gitlab-ci.yml`。

---

## 相关链接

- 父流水线：[`.gitlab-ci.yml`](../../../.gitlab-ci.yml)
- docs CI 子流水线：[`.gitlab/tsdb-build-docs.yml`](../../tsdb-build-docs.yml)
- 本地校验入口：[`local-validate.sh`](./local-validate.sh) / [`LOCAL-VALIDATE.md`](./LOCAL-VALIDATE.md)
- docs 子树（文档实际内容）：[`source/taos-community/docs/`](../../../source/taos-community/docs/)
