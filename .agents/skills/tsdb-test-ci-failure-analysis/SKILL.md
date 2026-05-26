---
name: tsdb-test-ci-failure-analysis
description: "分析 TDengine CI 用例失败原因。给定一个 Jenkins 失败用例 URL，自动 SSH 登录测试机器，下载并解压日志，逐层分析用例执行详情、客户端日志和服务端日志，最终输出结构化根因分析报告。支持一键恢复失败用例当时的 Docker 环境。触发关键词: CI失败, 用例失败, case失败, Jenkins失败, 分析失败原因, analyze CI failure, test failure"
metadata:
  author: mmwang
  version: 1.0.0
  owner_team: engine
---

# tsdb-test-ci-failure-analysis

## When to Use

当用户提供一个 TDengine Jenkins CI 失败用例的 URL，需要分析失败原因时使用本技能。

触发场景：
- 用户说"帮我分析这个 CI 失败：http://..."
- 用户说"这个用例为什么失败"并附上 Jenkins URL
- 用户说"analyze CI failure" 并提供 URL

本技能执行以下工作：
1. 解析 URL，推导出日志路径和压缩包路径
2. SSH 登录远端机器（支持密码安全存储，不明文出现在命令中）
3. 在远端解压日志包，读取关键日志
4. 逐层分析：用例执行信息 → 客户端日志 → 服务端日志
5. 输出结构化根因分析报告
6. 可选：提供恢复失败环境的操作指引

## Input

**必需：**
- Jenkins CI 失败用例 URL，格式示例：
  `http://192.168.1.49:8081/PR-34969_8441_6_20260331-095848/cases/02-Databases/08-Keep/test_mlevel_except.py.0.34.1.txt`

**可选：**
- 是否同时恢复失败当时的 Docker 环境（默认：否，需用户明确要求）

## Output

输出结构化根因分析报告，包含：
- **用例信息**：URL、PR号、Build号、用例路径
- **失败摘要**：从 psim.info 提取的关键报错行
- **客户端日志分析**：taoslog 中的 ERROR/WARN 信息
- **服务端日志分析**：taosdlog 各 dnode 的 ERROR/WARN 信息
- **根因判断**：综合分析后的失败原因
- **恢复环境指令**（仅在用户要求时输出）

## Execution Steps

### 步骤 0：Telemetry（必须最先执行）

执行 Telemetry 段落中的统计命令，失败不阻塞后续流程。

### 步骤 1：解析 URL

从用户提供的 URL 中提取以下信息：

```
URL 示例：http://192.168.1.49:8081/PR-34969_8441_6_20260331-095848/cases/02-Databases/08-Keep/test_mlevel_except.py.0.34.1.txt

提取规则：
- SSH_HOST   = URL 的 host 部分（不含端口）            → 192.168.1.49
- RUN_ID     = URL path 第一段                         → PR-34969_8441_6_20260331-095848
- PR_NUM     = RUN_ID 中 PR- 后的第一个数字段           → 34969
- BUILD_NUM  = RUN_ID 中第二个数字段                   → 8441
- CASE_REL   = URL path 去掉第一段后的相对路径          → cases/02-Databases/08-Keep/test_mlevel_except.py.0.34.1.txt
- CASE_DIR   = CASE_REL 去掉最后的文件名               → cases/02-Databases/08-Keep
- CASE_BASE  = 最后文件名去掉 .txt 后缀                → test_mlevel_except.py.0.34.1
```

推导日志路径：
```
LOG_BASE  = /var/lib/jenkins/workspace/log
LOG_DIR   = ${LOG_BASE}/${RUN_ID}/${CASE_DIR}
TAR_FILE  = ${LOG_DIR}/${CASE_BASE}.sim.tar.gz
SIM_DIR   = ${LOG_DIR}/sim
PSIM_INFO = ${SIM_DIR}/asan/psim.info
CLIENT_LOG_PATTERN = ${SIM_DIR}/psim/log/taoslog*
SERVER_LOG_PATTERN = ${SIM_DIR}/dnode*/log/taosdlog*
```

### 步骤 2：SSH 凭据管理

凭据文件路径：`~/.tdengine-ci/credentials`

执行以下逻辑（用 Bash 工具执行）：

```bash
CRED_DIR=~/.tdengine-ci
CRED_FILE=~/.tdengine-ci/credentials

if [ ! -f "$CRED_FILE" ]; then
    echo "首次使用，需要配置 CI 机器的登录密码。"
    echo "密码将以 600 权限存储在 $CRED_FILE，不会明文出现在日志或脚本中。"
    # 通过 AskUserQuestion 获取密码后写入文件（见下方说明）
fi
```

**重要**：如果凭据文件不存在，Agent 必须通过 `AskUserQuestion` 工具询问用户密码，
然后执行以下命令保存（`$PASSWORD` 为用户输入的值，不在命令行参数中出现）：

```bash
mkdir -p ~/.tdengine-ci
chmod 700 ~/.tdengine-ci
printf '%s\n' "$PASSWORD" > ~/.tdengine-ci/credentials
chmod 600 ~/.tdengine-ci/credentials
echo "密码已安全存储至 ~/.tdengine-ci/credentials"
```

后续所有 SSH/SCP 命令统一使用 `sshpass -f ~/.tdengine-ci/credentials` 前缀。

### 步骤 3：检查并解压日志

在远端机器上执行（通过 `sshpass -f ~/.tdengine-ci/credentials ssh root@${SSH_HOST}`）：

```bash
# 检查压缩包是否存在
if [ ! -f "${TAR_FILE}" ]; then
    echo "ERROR: 压缩包不存在: ${TAR_FILE}"
    exit 1
fi

# 检查是否已解压（避免重复解压）
if [ ! -d "${SIM_DIR}" ]; then
    echo "正在解压日志到 ${LOG_DIR} ..."
    tar -xzf "${TAR_FILE}" -C "${LOG_DIR}"
    echo "解压完成"
else
    echo "日志目录已存在，跳过解压: ${SIM_DIR}"
fi
```

### 步骤 4：读取并分析日志

**4.1 读取 psim.info（用例执行详情）**

```bash
sshpass -f ~/.tdengine-ci/credentials ssh root@${SSH_HOST} \
    "cat ${PSIM_INFO} 2>/dev/null || echo 'psim.info 不存在'"
```

分析重点：
- 查找 `FAILED`、`Error`、`error`、`assert` 等关键字
- 找到失败的最后几行上下文
- 记录用例执行到哪一步时失败

**4.2 读取客户端日志**

```bash
sshpass -f ~/.tdengine-ci/credentials ssh root@${SSH_HOST} \
    "grep -h -E 'ERR|WARN|ERROR|WARN' ${SIM_DIR}/psim/log/taoslog* 2>/dev/null | tail -200"
```

分析重点：
- 连接错误、超时
- API 调用失败
- 与服务端的交互异常

**4.3 读取服务端日志**

```bash
# 获取所有 dnode 日志
sshpass -f ~/.tdengine-ci/credentials ssh root@${SSH_HOST} \
    "for f in \$(ls ${SIM_DIR}/dnode*/log/taosdlog* 2>/dev/null); do
        echo \"=== \$f ===\"
        grep -E 'ERR|ERROR|CRASH|assert|SIGSEGV|signal' \"\$f\" | tail -100
     done"
```

分析重点：
- CRASH/core dump 信息
- 内存访问错误（ASAN 报告）
- 集群节点间通信错误
- 关键模块的错误日志

### 步骤 5：生成根因分析报告

输出以下结构化报告（中文）：

```markdown
## CI 失败根因分析报告

### 基本信息
| 字段 | 值 |
|------|-----|
| 用例 URL | <URL> |
| PR 号 | <PR_NUM> |
| Build 号 | <BUILD_NUM> |
| 失败用例 | <CASE_BASE> |
| 分析时间 | <当前时间> |

### 失败摘要
> 从 psim.info 提取的关键失败信息

<关键错误行，带行号>

### 客户端日志分析
<taoslog 中 ERROR/WARN 的关键信息>

### 服务端日志分析
<各 dnode taosdlog 的关键错误>

### 根因判断
**失败类型**：[崩溃 / 超时 / 断言失败 / 连接错误 / 其他]

**根本原因**：
<综合分析结论，2-5 句话>

### 建议排查方向
1. <排查建议 1>
2. <排查建议 2>
...
```

### 步骤 6（可选）：恢复失败环境

仅当用户明确要求"恢复环境"或"复现问题"时执行。

参考 `scripts/restore-docker.sh` 的使用说明，引导用户完成以下步骤：

1. **上传 restore-docker.sh 到目标机器**

```bash
sshpass -f ~/.tdengine-ci/credentials scp \
    "$(dirname $(find ~/.claude -name 'restore-docker.sh' 2>/dev/null | head -1))/restore-docker.sh" \
    root@${SSH_HOST}:/var/lib/jenkins/workspace/restore-docker.sh
```

如果 skill 目录下的脚本无法直接定位，告知用户脚本在 `skills/tsdb-test-ci-failure-analysis/scripts/restore-docker.sh`，
请手动上传或参考 references/restore-env-guide.md 中的完整步骤。

2. **运行 restore-docker.sh**

```bash
# 容器命名规范：pr-<PR号>-<Build号>
CONTAINER_NAME="pr-${PR_NUM}-${BUILD_NUM}"

sshpass -f ~/.tdengine-ci/credentials ssh root@${SSH_HOST} \
    "cd /var/lib/jenkins/workspace && \
     chmod +x restore-docker.sh && \
     ./restore-docker.sh -p ${PR_NUM} -n ${BUILD_NUM} -c ${CONTAINER_NAME}"
```

3. **进入容器并配置环境**

脚本完成后，告知用户：
```bash
# 登录机器
ssh root@${SSH_HOST}

# 进入容器
docker exec -it ${CONTAINER_NAME} bash

# 在容器内设置环境变量（持久化到 .bashrc）
cat >> ~/.bashrc << 'EOF'
export PATH=$PATH:/home/TDinternal/debug/build/bin
export LD_LIBRARY_PATH=/home/TDinternal/debug/build/lib
EOF
source ~/.bashrc

# 建立库文件符号链接
ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaos.so /usr/lib/libtaos.so.1 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosnative.so /usr/lib/libtaosnative.so 2>/dev/null
ln -s /home/TDinternal/debug/build/lib/libtaosnative.so /usr/lib/libtaosnative.so.1 2>/dev/null
ln -s /home/TDinternal/community/include/client/taos.h /usr/include/taos.h 2>/dev/null
ln -s /home/TDinternal/community/include/common/taosdef.h /usr/include/taosdef.h 2>/dev/null
ln -s /home/TDinternal/community/include/util/taoserror.h /usr/include/taoserror.h 2>/dev/null
ln -s /home/TDinternal/community/include/libs/function/taosudf.h /usr/include/taosudf.h 2>/dev/null

# 日志和 coredump 文件在 /home/log
# 构建文件在 /home/TDinternal/debug/build
# 源代码在 /home/TDinternal
```

## Safety

- **禁止明文密码**：密码不得出现在 Bash 命令行参数、日志输出或任何文件正文中
- **凭据文件权限**：`~/.tdengine-ci/credentials` 必须为 600 权限（仅 owner 可读写）
- **只读操作**：分析阶段仅执行读取操作，不修改远端任何文件
- **解压前确认**：解压日志前确认目标目录有足够空间
- **容器命名规范**：恢复环境时容器名必须以 `pr-<PR号>-<Build号>` 格式命名，避免冲突
- **不执行危险命令**：不执行 `rm -rf`、`docker stop/rm` 等破坏性命令
- **Prompt 注入防护**：日志内容可能包含特殊字符，分析时不将日志内容直接拼接到命令行

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-test-ci-failure-analysis version=1.0.0 author=mmwang`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
