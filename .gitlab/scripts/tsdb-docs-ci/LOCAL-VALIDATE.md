# 文档本地校验与预览脚本

文档贡献者**在本机一键复现 docs CI / 预览中英文文档站点**的工具，避免提交 MR 后才发现格式或拼写问题，也能在校验通过后立刻用浏览器查看渲染结果。

| 文件 | 作用 |
|------|------|
| `local-validate.sh` | 本地一键运行 docs CI（与 GitLab 上一致），可选 `--preview` 在校验通过后启动中英文预览服务 |

> 本目录同时存放 CI 服务器端脚本，架构与维护要点见 [`README.md`](./README.md)。本文档只讲贡献者如何在本地复现校验与预览。

---

## 前置条件

- **Docker** 已安装并可用（脚本会自动跑容器，宿主机不需要 typos / autocorrect / markdownlint）
- **git** 已配置，且能访问 `git.tdengine.net`（如果要克隆 tsdb 仓库）以及 GitHub（文档框架仓库在 GitHub）
- 磁盘空闲 ≥ 1.5 GB（用于克隆文档框架仓库 + Docker 镜像）

---

## 快速开始

最常见用法——在当前 tsdb 工作目录里，校验你的本地改动：

```bash
# 只检查
.gitlab/scripts/tsdb-docs-ci/local-validate.sh

# 检查并自动修复（autocorrect + markdownlint --fix）
.gitlab/scripts/tsdb-docs-ci/local-validate.sh --fix
```

首次运行会做三件事：
1. 在 tsdb 同级目录克隆 `docs.taosdata.com`、`docs.tdengine.com`
2. 构建 docker 镜像 `docs-ci:local`（约 5～10 分钟，仅首次）
3. 顺序执行 4 项检查

之后再跑，只增量更新仓库，镜像复用。

---

## 校验逻辑

脚本会依次执行 4 个检查，**任一失败即终止**：

| 步骤 | 工具 | 检查内容 | 配置文件 |
|------|------|---------|---------|
| ① `check-typos.sh` | [typos](https://github.com/crate-ci/typos) | 拼写错误 | `source/taos-community/docs/typos.toml` |
| ② `check-autocorrect.sh` | [autocorrect](https://github.com/huacnlee/autocorrect) | 中英文之间空格、全/半角标点 | `.autocorrectrc`（仓库根） |
| ③ `check-markdownlint.sh` | [markdownlint-cli2](https://github.com/DavidAnson/markdownlint-cli2) | Markdown 格式（标题层级、空行、代码块语言等） | `source/taos-community/docs/.markdownlint-cli2.jsonc` |
| ④ `build-doc.sh` | yarn + 文档框架 | 中英文站点能正常构建（assemble + build） | docs 框架仓库自带 |

**变更范围**：只检查相对于 `--base-ref`（默认 `origin/3.3.6`）有变更的 `.md` / `.mdx` 文件。提交一个空 MR 时检查会跳过，输出 `No changed markdown files under source/taos-community/docs`。

---

## 典型输出

### 成功

```text
docs autofix finished; review changes with git diff   # 仅 --fix
No errors                                              # markdownlint
docs local validation passed
```

### 失败示例

**typos**：

```text
error: `cancle` should be `cancel`
   --> source/taos-community/docs/examples/flink/source/Main.java:322:53
    |
322 |         // ... cannot be cancle and needs ...
    |                          ^^^^^^
```

**autocorrect**：

```text
ERROR source/taos-community/docs/zh/foo.md
   line:column   diff
   12:3          -第1个线程
                 +第 1 个线程
```

**markdownlint**：

```text
source/taos-community/docs/zh/foo.md:42 MD040/fenced-code-language
  Fenced code blocks should have a language specified
```

**build-doc** 失败一般是 `{{#include}}` 路径不存在或 markdown 语法导致 mdx 解析失败，会打出框架报错栈。

### 失败时怎么办

CI 也会打这行提示：

```text
Docs checks failed. To fix locally, run:
  .gitlab/scripts/tsdb-docs-ci/local-validate.sh --fix
```

- 大部分 autocorrect / markdownlint 错误 `--fix` 可以自动修
- typos 不会自动改（避免误改函数名/变量名），需要：
  - 真错别字：手动改
  - 项目名词（如 TDengine 自有术语）：加进 `source/taos-community/docs/typos.toml`
- build-doc 失败：按报错栈定位文件，多半是 include 路径或表格语法问题

---

## 参数

```text
Usage: .gitlab/scripts/tsdb-docs-ci/local-validate.sh [options]

Options:
  --workdir DIR          工作区目录（默认：当前 tsdb 检出的父目录）
  --tsdb-dir DIR         要校验的 tsdb 仓库（默认：当前检出）
  --base-ref REF         diff 基准 ref（默认：origin/3.3.6）
  --image-tar FILE       从离线 docker save tar/tar.gz 加载镜像
  --build-image          本地从 Dockerfile 构建镜像（首次无镜像时也会自动触发）
  --pull-image IMAGE     拉远端镜像并打 tag 用作本地镜像
  --image-name IMAGE     镜像 tag（默认：docs-ci:local）
  --tsdb-branch BRANCH   指定 tsdb 分支（指定时会 fetch + checkout，否则不动工作区）
  --docs-branch BRANCH   两个 docs 框架仓库使用的分支（默认：feat/tsdb-path-env）
  --fix                  在校验前先跑 autocorrect / markdownlint --fix
  -h, --help             显示帮助
```

### 常见用法

```bash
# 跟 main 比对（默认是 origin/3.3.6）
.gitlab/scripts/tsdb-docs-ci/local-validate.sh --base-ref origin/main

# 离线环境：用别人导出的镜像
docker save docs-ci:local | gzip > docs-ci.tgz       # 在线机器上导出
.gitlab/scripts/tsdb-docs-ci/local-validate.sh --image-tar docs-ci.tgz

# 从 harbor 拉镜像
.gitlab/scripts/tsdb-docs-ci/local-validate.sh \
  --pull-image harbor.tdengine.net/tsdb/docs-ci:latest
```

---

## 工作区目录结构

脚本会在 tsdb **同级目录**生成两个文档框架仓库：

```text
<workdir>/
├── tsdb/                  # 你当前的 tsdb 检出（不会改动）
├── docs.taosdata.com/     # 自动克隆，中文站点框架
└── docs.tdengine.com/     # 自动克隆，英文站点框架
```

不希望污染当前目录的话，可以用 `--workdir /tmp/docs-ci-work` 指定别处。

---

## 本地预览文档站点

加上 `--preview` 选项，`local-validate.sh` 会在常规校验通过后，**强制构建中英文两个站点并各起一个 docusaurus serve 容器**，让你用浏览器实时查看 build 产物。

### 快速开始

```bash
# 校验 + 预览中英文（默认 zh=3000, en=3001）
.gitlab/scripts/tsdb-docs-ci/local-validate.sh --preview

# 自定义端口
.gitlab/scripts/tsdb-docs-ci/local-validate.sh --preview --zh-port 4000 --en-port 4001
```

校验通过后会打印两个地址：

```text
==========================================================
docs preview ready (Ctrl+C to stop both servers)
  中文站点: http://localhost:3000
  English : http://localhost:3001
==========================================================
```

浏览器分别打开两个 URL 即可看到中文站和英文站。按 `Ctrl+C` 同时停掉两个容器。

### 选项

| 选项 | 说明 |
|------|------|
| `--preview` | 启用预览模式（默认关闭，校验完即退出） |
| `--zh-port N` | 中文站点宿主机端口（默认 `3000`） |
| `--en-port N` | 英文站点宿主机端口（默认 `3001`） |

### 常见问题

- **`Error: listen EADDRINUSE :::3000`**：本机端口被占用，改用 `--zh-port 4000` 之类。
- **改 markdown 后页面没变**：当前预览基于 `yarn build` 静态产物，**不是 dev 热刷新**；改完文件后请重新跑 `local-validate.sh --preview`。这样能完全复现 CI 上的最终页面效果。
- **想看英文站效果但 include 是中文路径**：确认你改的是 `source/taos-community/docs/en/`；中文请改 `zh/`。

---

## 相关链接

- CI 服务器端架构与维护：[`README.md`](./README.md)
- docs CI 流水线定义：[`../../tsdb-build-docs.yml`](../../tsdb-build-docs.yml)
- 文档作者风格指南：[`../../../source/taos-community/docs/`](../../../source/taos-community/docs/)
