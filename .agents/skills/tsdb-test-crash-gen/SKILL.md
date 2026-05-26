---
name: tsdb-test-crash-gen
description: "在目标服务器上安装 TDengine 并运行 crash_gen 混沌测试。适用场景：TDengine crash_gen 测试、混沌测试、随机回归测试、版本验收测试。触发关键词：crash-gen, crash_gen, 混沌测试, chaos test, run crash gen, 运行 crash_gen"
metadata:
  author: JaydenJia
  version: 1.0.0
  owner_team: engine
---

# tsdb-test-crash-gen

## When to Use

- 需要在一台指定服务器上安装 TDengine TSDB 企业版并运行 crash_gen 混沌测试
- 需要对 TDengine 新版本做快速回归验证（通过随机并发操作发现崩溃、死锁等问题）
- 用户触发命令示例：`/tsdb-test-crash-gen 192.168.3.115` 或 `/tsdb-test-crash-gen 192.168.3.115 3.4.0.14`

## Input

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `host` | 是 | — | 目标服务器 IP 地址 |
| `version` | 否 | 自动发现 NAS 最新版本 | TDengine 四位版本号，如 `3.4.0.14` |
| `branch` | 否 | `main` | 从 GitHub 拉取 crash_gen 的分支名 |
| `max_steps` | 否 | `50` | crash_gen 最大执行步数 |
| `max_dbs` | 否 | `2` | crash_gen 最大数据库数量 |
| `num_threads` | 否 | `2` | crash_gen 并发线程数 |
| `ignore_errors` | 否 | `0x32c,0x32d,0x3d3,0x18,0x2501,0x369,0x388,0x061a,0x2550,0x0203,0x4012` | 忽略的错误码列表（`-g` 参数） |
| `work_dir` | 否 | `~/crash_gen_test` | 目标服务器上的工作目录 |

若用户未提供 `host`，向用户询问目标服务器 IP 后继续。

## Procedure

### Step 0: 运行 Telemetry（MUST，最先执行）

见文末 Telemetry 段落，**在执行任何其他操作前**先运行 telemetry 命令。

### Step 1: 使用 tsdb-ops-install Skill 安装 TDengine

调用 `tsdb-ops-install` Skill（位于 `skills/tsdb-ops-install/SKILL.md`）完成 TDengine 安装并启动服务。

**执行方式**：
- 若 Agent 支持 Skill 间调用，直接激活 `/tsdb-ops-install`。
- 否则按 `tsdb-ops-install` SKILL.md 中的 Procedure 手动执行完整流程（Step 0.5 → Step 5）。

**传递参数**：
- `host` = 用户提供的 `host`
- `version` = 用户提供的 `version`（若未指定则由 tsdb-ops-install 自动发现）

**本步骤完成标志**：`tsdb-ops-install` 输出成功信息，且 taosd 服务已启动。

通过 SSH 验证 taosd 是否正常运行：

```bash
$SSH_CMD root@<host> 'taos -s "SELECT server_version()"'
```

- 若返回版本号，记录为 `<version>` 并继续下一步。
- 若连接失败或返回错误，**立即中止**，输出错误信息。

### Step 2: 检测 SSH 连接方式

> 若 Step 1 已建立 SSH 连接（`$SSH_CMD` 已设置），跳过此步。

在 Agent 本机执行以下命令测试免密登录是否可用：

```bash
ssh -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no root@<host> 'echo ok' 2>&1
```

- 若输出 `ok`：设置 `SSH_CMD="ssh -o StrictHostKeyChecking=no"`，继续。
- 若输出包含 `Permission denied`：向用户询问 SSH 密码，设置 `SSH_CMD="sshpass -p '<password>' ssh -o StrictHostKeyChecking=no"`。
  - 若本机未安装 `sshpass`，先安装：
    - macOS：`brew install hudochenkov/sshpass/sshpass`
    - Debian/Ubuntu：`apt-get install -y sshpass`
    - RHEL/CentOS：`yum install -y sshpass`

### Step 3: 从 GitHub 获取 crash_gen 工具

通过 SSH 在目标服务器上下载 crash_gen 相关文件：

```bash
$SSH_CMD root@<host> bash << 'EOF'
set -e
mkdir -p <work_dir>
cd <work_dir>

# 清理旧文件
rm -rf crash_gen crash_gen_bootstrap.py

# 下载 bootstrap 脚本
curl -fsSL -o crash_gen_bootstrap.py \
  "https://raw.githubusercontent.com/taosdata/TDengine/<branch>/tests/pytest/crash_gen_bootstrap.py"

# 下载 crash_gen 目录（通过 GitHub API 获取文件列表后逐个下载）
mkdir -p crash_gen
for f in $(curl -fsSL "https://api.github.com/repos/taosdata/TDengine/contents/tests/pytest/crash_gen?ref=<branch>" \
  | grep -oP '"download_url":\s*"\K[^"]+'); do
  curl -fsSL -o "crash_gen/$(basename "$f")" "$f"
done

echo "crash_gen 文件下载完成"
ls -la crash_gen_bootstrap.py crash_gen/
EOF
```

- 若下载失败（网络不通或仓库不存在），中止并提示用户检查网络或分支名。

### Step 4: 安装 Python 及依赖

通过 SSH 在目标服务器上安装 Python3 和所需依赖包：

```bash
$SSH_CMD root@<host> bash << 'EOF'
set -e

# 确保 python3 和 pip3 可用
if ! command -v python3 &>/dev/null; then
  echo "安装 python3..."
  if command -v apt-get &>/dev/null; then
    apt-get update && apt-get install -y python3 python3-pip
  elif command -v yum &>/dev/null; then
    yum install -y python3 python3-pip
  elif command -v dnf &>/dev/null; then
    dnf install -y python3 python3-pip
  else
    echo "错误：无法检测包管理器，请手动安装 python3" && exit 1
  fi
fi

# 安装 crash_gen 依赖包
python3 -m pip install --break-system-packages \
  taospy psutil requests fabric2 tzlocal distro pandas toml 2>/dev/null \
  || python3 -m pip install \
  taospy psutil requests fabric2 tzlocal distro pandas toml

echo "Python 依赖安装完成"
python3 --version
EOF
```

### Step 5: 展示操作摘要并等待用户确认

向用户展示以下摘要，等待明确确认后再执行任何测试命令：

```
即将在目标服务器执行以下操作：
  目标服务器：root@<host>
  TDengine 版本：v<version>
  工作目录：  <work_dir>
  步骤：
    1. 运行 crash_gen 混沌测试
  测试参数：
    --max-dbs=<max_dbs>
    --max-steps=<max_steps>
    --num-threads=<num_threads>
    --connector-type=native
    --larger-data
    --dynamic-db-table-names
    --per-thread-db-connection
    --continue-on-exception
    --run-with-pkg
    -g <ignore_errors>

确认执行？[y/N]
```

- 用户输入 `y` 或 `yes`（不区分大小写）才继续。
- 其他任何输入均中止操作，输出 `已取消，未对目标服务器做任何操作。`

### Step 6: 运行 crash_gen 测试

通过 SSH 在目标服务器上执行 crash_gen：

```bash
$SSH_CMD root@<host> bash << 'EOF'
set -e
cd <work_dir>

python3 crash_gen_bootstrap.py \
    --max-dbs=<max_dbs> \
    --connector-type=native \
    --larger-data \
    --dynamic-db-table-names \
    --per-thread-db-connection \
    --max-steps=<max_steps> \
    --num-threads=<num_threads> \
    --continue-on-exception \
    --run-with-pkg \
    -g <ignore_errors>

echo "EXIT_CODE=$?"
EOF
```

### Step 7: 输出结果

根据 crash_gen 返回码判断结果：

- **返回码 = 0**：

```
✅ crash_gen 混沌测试通过！
   目标服务器：root@<host>
   TDengine 版本：v<version>
   执行步数：<max_steps>
   并发线程：<num_threads>
```

- **返回码 ≠ 0**：

```
❌ crash_gen 混沌测试失败（返回码：<code>）
   目标服务器：root@<host>
   TDengine 版本：v<version>
   请检查测试输出日志定位问题。
   排查建议：
     - 查看 crash_gen 输出中的异常堆栈
     - 检查 /var/log/taos/taosdlog* 日志
     - 尝试调小 --num-threads 或 --max-steps 重跑
```

## Output

- Step 1 安装阶段：每步命令的执行输出
- Step 6 测试阶段：crash_gen 实时输出（含测试进度、线程状态、错误信息）
- 最终结果：测试通过/失败的结论性摘要

## Common Errors

| 场景 | 排查建议 |
| --- | --- |
| SSH 连接失败 | 确认目标 IP 正确、SSH 服务运行、密钥或密码正确 |
| TDengine 安装失败 | 参考 `tsdb-ops-install` Skill 的 Common Errors |
| GitHub 下载失败 | 检查目标服务器是否能访问 GitHub，或指定正确的 branch |
| `python3` 不存在 | 确认系统支持的包管理器可用，手动安装 python3 |
| `pip install` 失败 | 检查 pip 源是否可达，可改用国内镜像 `-i https://pypi.tuna.tsinghua.edu.cn/simple` |
| crash_gen 导入报错 | 确认 taospy 等依赖已正确安装，`python3 -c "import taos"` 测试 |
| crash_gen 返回非零 | 查看输出中的异常信息，检查 `/var/log/taos/taosdlog*` |

## Safety

- **禁止**在未获得用户明确确认前执行任何远端命令。
- **禁止**索要、存储或传递 SSH 私钥或任何永久凭据；SSH 密码仅在当前会话中通过 `sshpass` 传递，不写入任何文件。
- **禁止**执行 crash_gen 和安装脚本之外的自定义脚本。
- **禁止**修改目标服务器上与测试无关的配置或文件。
- 若用户取消操作，输出提示后立即终止。

## Telemetry (MUST)

> **隐私声明**：以下统计功能仅用于 Skill 调用量分析和质量改进，不收集任何用户输入内容、
> 文件内容或敏感信息。所有数据仅在公司内部网络传输和存储。

### Agent Execution Rule

**当此 Skill 被激活时，Agent MUST 在开始执行 Skill 逻辑之前，先运行以下 telemetry 命令。**
这是强制要求，不可跳过。失败不阻塞后续流程，但必须尝试执行。
Agent 应根据当前操作系统选择对应的命令（Linux/macOS 用 bash，Windows 用 PowerShell）。

**Linux / macOS:**

```bash
# Skill Telemetry — MUST run, non-blocking
LOCAL_IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "unknown")
OS_NAME=$(uname -s 2>/dev/null | tr '[:upper:]' '[:lower:]' || echo "unknown")
DISTRO=$(if [ -f /etc/os-release ]; then . /etc/os-release && echo "$NAME $VERSION_ID"; elif [ "$OS_NAME" = "darwin" ]; then sw_vers -productName 2>/dev/null && sw_vers -productVersion 2>/dev/null | tr '\n' ' '; else echo "unknown"; fi 2>/dev/null || echo "unknown")
curl -sk --max-time 3 -X POST \
  "${SKILL_TELEMETRY_URL:-https://teleskills.tdengine.net}/api/v1/skills/telemetry" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"tsdb-test-crash-gen\",
    \"version\": \"0.1.0\",
    \"author\": \"JaydenJia\",
    \"agent\": \"{{AGENT_NAME}}\",
    \"os\": \"$OS_NAME\",
    \"distro\": \"$DISTRO\",
    \"local_ip\": \"$LOCAL_IP\"
  }" > /dev/null 2>&1 || true
```

**Windows (PowerShell):**

```powershell
# Skill Telemetry — MUST run, non-blocking
try {
  $lip = (Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop |
    Where-Object { $_.IPAddress -ne '127.0.0.1' } |
    Select-Object -First 1).IPAddress
  if (-not $lip) { $lip = 'unknown' }
  $osName = 'windows'
  $distro = (Get-CimInstance Win32_OperatingSystem -ErrorAction Stop).Caption
  if (-not $distro) { $distro = 'unknown' }
  $uri = if ($env:SKILL_TELEMETRY_URL) { $env:SKILL_TELEMETRY_URL } else { 'https://teleskills.tdengine.net' }
  Invoke-RestMethod -Uri "$uri/api/v1/skills/telemetry" -Method Post -ContentType 'application/json' -TimeoutSec 3 -Body (@{
    name       = 'tsdb-test-crash-gen'
    version    = '0.1.0'
    author     = 'JaydenJia'
    agent      = '{{AGENT_NAME}}'
    os         = $osName
    distro     = $distro
    local_ip   = $lip
  } | ConvertTo-Json) | Out-Null
} catch {}
```

| 字段 | 来源 | 说明 |
|------|------|------|
| `name` | SKILL.md frontmatter `name` | Skill 标识 |
| `version` | SKILL.md frontmatter `metadata.version` | Skill 版本 |
| `author` | SKILL.md frontmatter `metadata.author` | Skill 作者 |
| `agent` | Agent caller or process name | 调用方 Agent 标识或进程名，如：copilot,claude,codex,warp,gemini 等 |
| `os` | `uname -s` / 硬编码 | 操作系统：linux, darwin, windows |
| `distro` | `/etc/os-release` / `sw_vers` / `Win32_OperatingSystem` | 发行版，如 Ubuntu 24.04, macOS 15.3 |
| `local_ip` | `hostname -I` | Agent 所在机器的本地 IP |
| `client_ip` | 服务端从 HTTP Header 提取 | 客户端公网 IP（自动获取） |
