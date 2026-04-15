# 拷贝模式数据修复 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-20 | - | 0.1 | 张博民 | 初稿 |

## 2. 背景

在多副本集群中，当损坏数据量巨大时，直接使用 `restore` 命令恢复的性能无法满足客户要求，故对一些可以接受停机的用户，交付人员会通过手动拷贝文件的方式来恢复损坏的数据。但这种手工操作效率低下且易出现错误。

故希望数据修复工具可以通过命令直接实现拷贝方式修复损坏的数据。

## 3. 定义

| 术语 | 定义 |
| --- | --- |
| **TFS** | Tiered File System，TDengine 的多级存储管理系统。管理多个存储层级（tier）和每层内的多个挂载点（disk）。 |
| **磁盘 ID 重映射** | 当源端与目标端的 TFS 磁盘布局不同时（例如层级数或磁盘数不同），将源端文件的 SDiskID 转换为目标端有效 SDiskID 的过程。 |
| **主磁盘（Primary Disk）** | 每个存储层级中 `primary` 标记为 `1` 的磁盘，用于存放元数据和配置文件。 |
| **SCP** | Secure Copy Protocol，基于 `SSH` 的远程文件拷贝工具，为 `OpenSSH` 套件的组成部分，`Linux` 系统默认安装。 |

## 4. 行为说明

### 4.1 命令行语法

```
taosd -r --mode copy --node-type vnode \
  [--source-host <user@host>] \
  --source-cfg <path> \
  {--vnode <id> [--vnode <id> ...] | --all-vnodes source | --all-vnodes local}
```

**参数说明：**

| 参数 | 必需 | 说明 |
| --- | --- | --- |
| `-r` | 是 | 激活修复模式 |
| `--mode copy` | 是 | 指定文件拷贝修复模式 |
| `--node-type vnode` | 是 | 指定修复对象为 vnode |
| `--source-host <user@host>` | 否 | 远程数据源主机地址（格式：`user@host`）。省略时为本地模式。 |
| `--source-cfg <path>` | 是 | 数据源的 `taos.cfg` 配置文件路径。工具从中解析 `dataDir` 条目以获取源端磁盘布局。远程模式下为远程主机上的配置文件路径（工具通过 SSH 读取），本地模式下为本地文件路径。 |
| `--vnode <id>` | 三选一 | 指定要修复的 vnode ID，可重复指定多个。 |
| `--all-vnodes source` | 与 `--vnode` 和 `--all-vnodes local` 三选一 | 修复源端 `vnodes.json` 中的所有 vnode。适用于本地 `vnodes.json` 也损坏或需要完整克隆节点数据的场景。 |
| `--all-vnodes local` | 与 `--vnode` 和 `--all-vnodes source` 三选一 | 修复本地 `vnodes.json` 中列出的所有 vnode（取本地与源端的交集）。若某 vnode 存在于本地但不在源端，跳过并输出警告。要求本地 `vnodes.json` 可读。 |

**配置文件解析说明：**

工具仅从 `--source-cfg` 指定的配置文件中提取 `dataDir` 相关条目，其他配置项（如 `firstEp`、`fqdn`、`serverPort` 等）将被忽略。配置文件格式与标准 `taos.cfg` 完全一致：

```cfg
# 支持的 dataDir 格式：
dataDir /var/lib/taos              # 单盘（默认 level=0, primary=1）
dataDir /var/lib/taos 0 1          # 指定 level 和 primary
dataDir /mnt/ssd1 0 0              # 同层级非主磁盘
dataDir /mnt/hdd1 1 0              # 不同层级
dataDir /mnt/hdd1 1 0 0            # 带 disable 标志（企业版）
```

工具使用 TDengine 内置的 `cfgInit()` / `cfgLoad()` API 独立解析配置文件，不依赖 dnode 初始化流程。

### 4.2 使用示例

**示例 1：从本地目录恢复所有 vnode（单盘）**

适用场景：用户已提前将健康副本数据拷贝到本地目录（或通过 NFS 等挂载了远程目录）。需准备一个描述源端磁盘布局的临时配置文件。

```bash
# 准备源端配置文件
cat > /tmp/source-taos.cfg <<EOF
dataDir /mnt/backup/taos 0 1
EOF

taosd -r --mode copy --node-type vnode \
  --source-cfg /tmp/source-taos.cfg \
  --all-vnodes source
```

**示例 2：从本地目录恢复指定 vnode（多级存储）**

适用场景：数据源有多级存储，仅需修复 vnode 2 和 vnode 5。

```bash
# 准备源端配置文件
cat > /tmp/source-taos.cfg <<EOF
dataDir /mnt/backup/data1 0 1
dataDir /mnt/backup/data2 0 0
dataDir /mnt/backup/ssd1 1 0
EOF

taosd -r --mode copy --node-type vnode \
  --source-cfg /tmp/source-taos.cfg \
  --vnode 2 --vnode 5
```

**示例 3：从远程健康节点恢复（SCP 模式）**

适用场景：健康副本位于远程节点 192.168.1.10，通过 SSH 密钥认证直接拷贝。直接指定远程主机上的配置文件路径。

```bash
# 前置步骤：配置免密登录
ssh-copy-id root@192.168.1.10

# 执行修复（--source-cfg 指向远程主机上的配置文件）
taosd -r --mode copy --node-type vnode \
  --source-host root@192.168.1.10 \
  --source-cfg /etc/taos/taos.cfg \
  --vnode 2 --vnode 5
```

注意：当指定 `--source-host` 时，`--source-cfg` 为远程主机上的路径。工具会通过 SSH 读取该配置文件，从中提取 `dataDir` 条目以确定远程磁盘布局。

### 4.3 执行流程

`--mode copy` 的执行流程如下（taosd 进程在修复完成后退出，不会进入正常服务模式）：

1. **解析和校验命令行参数**
2. **加载目标节点 TFS 配置**（从 `taos.cfg` 读取本地磁盘布局）
3. **解析源端配置文件并构建 TFS 模型**（本地模式直接读取 `--source-cfg` 文件；远程模式通过 `ssh <host> cat <cfg_path>` 获取内容后解析 `dataDir` 条目）
4. **连通性检查**（远程模式：`ssh -o BatchMode=yes -o ConnectTimeout=5 <host> true`）
5. **读取源端 `vnodes.json`**（以及本地 `vnodes.json`，若使用 `--all-vnodes local`），确定最终 vnode 列表：
   - `--vnode <id>`：使用显式指定的列表，校验每个 ID 在源端存在
   - `--all-vnodes source`：使用源端 `vnodes.json` 中的全部 vnode
   - `--all-vnodes local`：读取本地 `vnodes.json`，取与源端 `vnodes.json` 的交集；本地存在但源端不存在的 vnode 跳过并警告
6. **对每个目标 vnode 依次执行：**
   - a. 读取源端 `vnode.json` 和各 `current.json`，构建文件清单
   - b. 计算磁盘 ID 重映射关系
   - c. 清除目标端已有 vnode 数据（防止残留文件干扰）
   - d. 创建目标端目录结构
   - e. 拷贝非 TSDB 数据（meta/、wal/、tq/、bse/、vnode.json等）到主磁盘
   - f. 拷贝 TSDB 数据文件，按重映射后的磁盘 ID 写入对应磁盘目录
   - g. 重写 `current.json`，将文件条目中的磁盘 ID 替换为重映射后的值
   - h. 删除 `current.c.json` / `current.m.json`（未完成操作的残留）
7. **更新目标端 `vnodes.json`**（添加或更新修复后的 vnode 条目）
8. **输出汇总报告并退出**

### 4.4 磁盘 ID 重映射规则

当源端和目标端磁盘布局不一致时，按以下规则映射：

| 条件 | 映射行为 |
| --- | --- |
| 源端 tier N 在目标端存在 | 映射到目标端 tier N，在该 tier 内的磁盘间轮询分配 |
| 源端 tier N 在目标端不存在（目标端层级较少） | 折叠到目标端最高可用 tier |
| 目标端只有单盘 | 所有文件映射到 `{0, 0}` |

### 4.5 出错处理

| 错误场景 | 行为 |
| --- | --- |
| `--source-cfg` 配置文件不存在或无法读取 | 校验阶段报错并退出 |
| 配置文件中未找到 `dataDir` 条目 | 报错并退出 |
| `dataDir` 指向的路径不存在（本地模式） | 校验阶段报错并退出 |
| SSH 连接失败（远程模式） | 连通性检查失败，报错并退出 |
| 数据源 `vnodes.json` 缺失或解析失败 | 报错并退出 |
| 指定的 `--vnode <id>` 在源端不存在 | 报错列出源端可用 vnode 列表 |
| 单个文件拷贝失败 | 记录错误日志，终止对当前 vnode 的修复，继续处理下一个 vnode |
| 目标磁盘空间不足 | 拷贝期间 I/O 报错，记录日志并退出 |
| 修复进程被 kill | 目标 vnode 处于不完整状态；下次运行 `--mode copy` 时步骤 6c 会自动清除后重新拷贝 |

### 4.6 退出码

| 退出码 | 含义 |
| --- | --- |
| 0 | 所有目标 vnode 修复成功 |
| 1 | 参数校验失败 |
| 2 | 连通性检查失败（远程模式） |
| 3 | 部分 vnode 修复失败（成功的 vnode 仍然有效） |
| 4 | 所有 vnode 修复失败 |

## 5. 性能

### 5.1 性能特征

`--mode copy` 为文件级拷贝操作，性能主要受限于 I/O 带宽：

- **本地模式**：性能取决于源和目标磁盘的读写 IOPS 和吞吐量。
- **远程模式**：性能取决于网络带宽和磁盘 I/O 中的瓶颈。
- **对比 RESTORE 命令**：`RESTORE` 按行级解析、Raft 快照传输和重放，涉及反序列化和重新写入。`--mode copy` 直接拷贝文件，跳过所有编解码开销，预期恢复速度提升一个数量级以上。

### 5.2 不启用传输压缩

TSDB 数据文件中的数据一般已处于压缩状态。对已压缩数据再做传输层面的压缩会浪费 CPU 且难以明显减小传输量。WAL 和 meta 数据虽未压缩，但体积通常远小于 TSDB 数据文件，不值得为此开启传输压缩。

### 5.3 对正常运行的影响

`--mode copy` 在 taosd 进入正常服务模式之前执行并退出，不会影响正常的写入、查询和启动流程。

## 6. 安全

### 6.1 SSH 认证

远程模式仅支持 SSH 密钥认证，不支持密码认证。使用 `BatchMode=yes` 确保非交互式执行，避免密码提示阻塞修复流程。用户须在执行修复前运行 `ssh-copy-id` 在目标节点和源节点之间配置免密登录。

### 6.2 命令注入防护

所有传递给 `ssh` / `scp` 命令的路径参数需经过严格的 shell 转义处理，防止命令注入攻击。具体措施：

- 对路径中的特殊字符（`$`, `` ` ``, `"`, `\`, `;`, `&`, `|`, `(`, `)`, `<`, `>`, `*`, `?`, `[`, `]`, `{`, `}`, `#`, `!`, `~`, 空格, 换行等）进行转义
- `ssh` / `scp` 命令仅在 `dmRepairFlowEnabled()` 返回 true 时被允许执行。`taosOpenCmd()` 内部通过 `isCommandAllowed()` 维护命令白名单（默认仅允许 `taos`、`taosd`、`taosdump` 等 TDengine 自有命令）。修复模式激活时，将 `ssh` 和 `scp` 临时加入白名单；修复结束后恢复原白名单，确保正常运行时不可执行外部命令。
- 不允许用户通过参数注入额外的 SSH 选项

### 6.3 源数据只读

修复流程对数据源（本地或远程）始终以只读方式访问，不会修改源端任何文件。

### 6.4 最小权限

远程模式需要 SSH 登录权限和源端数据目录的读取权限。建议使用专用的只读 SSH 账户。

## 7. 兼容性

**无破坏性变更。**

- `--mode copy` 是新增模式，不影响现有 `--mode force` 的行为。
- 不修改任何现有配置参数、SQL 命令、API 接口。
- 修复后的 vnode 数据格式与正常运行产生的数据完全一致，taosd 可直接以正常模式启动使用。
- 目标端 `vnodes.json` 的更新采用原子写入（临时文件 + rename），确保写入过程中不会破坏已有配置。

## 8. 运维

### 8.1 前置条件

- **目标 taosd 必须停止运行**：`--mode copy` 在修复模式下运行，不与正在运行的 taosd 并存。
- **源端 taosd 必须停止运行**：拷贝期间源端 taosd 持续写入会导致文件不一致（WAL 追加、TSDB compaction 重写文件、`current.json` 更新等）。必须在源端 taosd 停止后再执行修复，以确保拷贝到的数据是一致快照。
- **远程模式的 SSH 密钥配置**：执行修复前需确保目标节点可通过密钥免密登录源端主机。

### 8.2 操作步骤

```bash
# 1. 停止目标节点 taosd
systemctl stop taosd

# 2. 停止源端 taosd（必须）
# 在源端执行：
ssh root@<source-host> systemctl stop taosd

# 3.（远程模式）配置 SSH 免密登录
ssh-copy-id root@<source-host>

# 4. 执行修复
taosd -r --mode copy --node-type vnode \
  --source-host root@<source-host> \
  --source-cfg /etc/taos/taos.cfg \
  --all-vnodes source

# 5. 检查退出码和日志
echo $?
cat /var/log/taos/taosdlog.0

# 6. 正常启动 taosd
systemctl start taosd
```

### 8.3 客户支持注意事项

- 修复前务必确认目标节点的磁盘空间充足，至少需要与源端 vnode 数据等量的可用空间。
- 修复过程中被中断后可安全重新执行，工具会先清除不完整的数据再重新拷贝。
- 日志中包含进度信息，可用于排查问题。

## 9. 使用场景

多副本集群中，当损坏数据量巨大时，直接使用 `restore` 命令恢复的性能无法满足客户要求时。

## 10. 约束和限制

### 约束

- **仅支持 Linux 操作系统**。不支持 Windows、macOS。
- **不支持容器部署环境**。Docker / Kubernetes 环境中 SSH/SCP 通常不可用且密钥配置复杂，Pod 重启策略（`restartPolicy: Always`）与修复后退出行为冲突，容器内路径映射增加操作复杂度。初始版本仅支持物理机和虚拟机裸机部署。
- **目标 taosd 必须处于停机状态**。不能在 taosd 运行时执行 `--mode copy`。
- **源端 taosd 必须处于停机状态**。拷贝期间源端写入会导致文件不一致（WAL 追加、TSDB compaction、`current.json` 更新等）。
- **源端与目标端的 TDengine 版本必须一致**。不支持跨版本数据拷贝。
- **远程模式要求 SSH 密钥认证**。不支持密码认证。

### 限制

- **不支持增量拷贝**。每次执行时目标 vnode 数据会被完全替换。如数据量大且仅少量文件损坏，仍会全量拷贝。
- **不支持并行拷贝多个 vnode**。初始版本按 vnode 串行执行拷贝。
- **不执行数据一致性校验**。拷贝完成后仅校验文件存在性和 JSON 可解析性，不校验数据块完整性（CRC 等）。

## 11. 常见错误和排查

后续根据实现情况补充。

## 12. 可观测性

`--mode copy` 为停机修复工具，执行期间 taosd 不提供服务，因此对 taos shell、taosExplorer、TDinsight 等组件无运行时影响。

修复过程的观测通过以下方式实现：

- **日志输出**：修复过程通过 `uInfo` / `uError` 输出到 taosd 日志文件（`/var/log/taos/taosdlog.0`），内容包括：
  - 启动参数（模式、源端配置、目标 vnode 列表）
  - 每个 vnode 的修复开始和完成
  - 每个文件的拷贝进度（文件路径、字节数）
  - 磁盘 ID 重映射详情
  - 汇总报告（总 vnode 数、文件数、字节数）
- **退出码**：脚本和运维工具可通过进程退出码判断修复结果
- **标准输出**：关键进度信息同时打印到标准输出（stdout），方便在终端中直接观察

## 13. 安装和卸载

无额外要求。`--mode copy` 功能内建于 `taosd` 二进制文件，不引入新的外部依赖或安装包组件。

远程模式依赖系统自带的 OpenSSH 套件（`ssh` / `scp`），Linux 系统默认安装，无需额外安装。

安装包和 Docker 镜像不需要做任何改动。

## 14. 文档

- **需要修改官网文档**：在 `taosd` 命令行参数文档中增加 `--mode copy` 的使用说明（参数列表、示例、使用场景）。
- **需要修改企业版文档**：在数据修复/运维章节中增加文件拷贝修复模式的操作指南。
- 文档需在功能发布前准备完毕。

## 15. 参考文档

- [数据修复工具 RS](../../TSDB-v3.4.1-[20260331]/04-需求文档/数据修复工具%20-%20RS.md)
- [数据修复工具 FS](../../TSDB-v3.4.1-[20260331]/05-设计文档/数据修复工具%20FS.md)

## 16. 附录

  无。
