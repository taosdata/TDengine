---
sidebar_label: 网络与 FQDN 配置
title: 网络与 FQDN 配置
toc_max_heading_level: 4
---

### IPv6

TDengine 支持 IPv6 网络环境。该功能允许用户在现代网络基础设施中部署和连接 TDengine，减少对 IPv4 的依赖，以满足日益增长的 IPv6 网络需求。

#### 支持范围

- 支持版本：TDengine Server 和 Client 版本均需 ≥ 3.3.7.0

- 支持组件：

  - taosd：TDengine 数据库服务端
  - taos：`taos` shell
  - 各种连接器：如 JDBC, Go, Python, C#, Rust 等（需使用支持 IPv6 的版本）

- 网络环境：纯 IPv6 环境或 IPv4/IPv6 双栈环境均支持

#### 服务端 (taosd) 配置

启用 IPv6 支持需在 TDengine 服务端的配置文件 `taos.cfg` 中进行设置，步骤如下：

1. 定位配置文件：默认路径通常为 `/etc/taos/taos.cfg`
2. 修改配置参数：找到并修改以下关键参数

```bash
// 设置 TDengine 服务端在指定网络接口上监听 IPv6 地址，值为该接口对应的 IPv6 地址，或 "::" 表示监听所有可用 IPv6 接口

firstEp    ipv6_address1:port
secondEp   ipv6_address2:port
fqdn       ipv6_address1
enableIPv6 1

```

3. 重启服务：修改配置后，需要重启 TDengine 服务以使配置生效

```bash
sudo systemctl restart taosd
```

重要说明

- 强烈建议使用 FQDN（全限定域名）配置 firstEP 和 secondEP，而非直接使用 IP 地址。通过 DNS 解析可自动选 IPv6 地址，提升灵活性和兼容性。
- 默认端口 6030 同样适用于 IPv6 连接。

#### 客户端连接方式

客户端可通过以下两种方式连接到支持 IPv6 的 TDengine 服务端：

1. 使用 FQDN：在客户端的 taos.cfg 或连接字符串中，使用服务端的域名。若该域名的 AAAA 记录指向正确的 IPv6 地址，客户端会自动通过 IPv6 建立连接。示例：

```bash
taos -h your_server_fqdn -P 6030
```

2. 直接使用 IPv6 地址：连接时需直接指定服务端的 IPv6 地址，且在命令行或连接字符串中必须用中括号包裹。示例：

```bash
taos -h [2001:db8::1] -P 6030
```

3. 验证连接：成功连接后，可在 TDengine 日志中确认所有连接均通过 IPv6 进行。

#### 注意事项与故障排除

- 网络基础设施：确保服务器、客户端及中间的路由器、防火墙等网络设备均正确配置 IPv6，并允许 6030 端口（默认）的通信。
- DNS 配置：若使用 FQDN，需确保 DNS 服务器正确配置该域名的 AAAA 记录（指向 IPv6 地址），而非 A 记录（指向 IPv4 地址）。
- 双栈环境优先级：在同时支持 IPv4 和 IPv6 的双栈主机上，需显式配置 FQDN 对应 IPv6 地址，以优先使用 IPv6 连接。
- 连接器版本：确保所有 TDengine 客户端连接器（如 JDBC、Go、Python 等）为 3.3.7.0 或更高版本，以完全兼容 IPv6。

### TLS

TDengine 的传输层支持加密通信，保障数据在网络传输过程中的安全性。

#### 参数说明

1. `tlsCaPath`：CA 证书路径，客户端和服务端均需配置，不可动态调整。
2. `tlsSvrCertPath`：服务端证书路径，仅服务端配置，不可动态调整。
3. `tlsSvrKeyPath`：服务端私钥路径，仅服务端配置，不可动态调整。
4. `tlsCliCertPath`：客户端证书路径，客户端和服务端均需配置（服务端用于集群间通信，含单节点），不可动态调整。
5. `tlsCliKeyPath`：客户端私钥路径，客户端和服务端均需配置（服务端用于集群间通信，含单节点），不可动态调整。
6. `enableTLS`：客户端和服务端均需配置的开关参数。启用 TLS 前，必须先配置所有必需的证书路径参数；若配置错误或不完整，服务将无法启动。

#### 查看参数

通过以下命令可查看集群中各节点的 TLS 相关配置：

```sql
SHOW VARIABLES LIKE '%tls%'; 
```

#### 约束说明

1. 服务端启用 TLS 时，需完整配置上述 5 个证书路径参数，否则启动失败；若 5 个参数均未配置，则以非 TLS 模式启动。
2. 客户端启用 TLS 时，需配置 `tlsCaPath`、`tlsCliCertPath`、`tlsCliKeyPath` 3 个参数，否则启动失败；若 3 个参数均未配置，则以非 TLS 模式启动。
3. 集群所有节点需统一以 TLS 模式或非 TLS 模式启动，否则集群间访问会失败。
4. 非 TLS 模式的客户端无法访问 TLS 模式的集群，会返回连接失败。
5. TLS 模式的客户端无法访问非 TLS 模式的集群，会返回连接失败。
6. 启用 TLS 后，所有网络传输数据均需双向鉴权认证；即使仅部署单个 taosd，其内部模块通过 RPC 通信时也需双向鉴权。

#### 部署用例

1. 生成相关证书文件

```bash
# 生成 CA 证书和私钥
openssl req -newkey rsa:2048 -nodes -keyout ca.key -x509 -days 365 -out ca.crt -subj "/CN=MyCA"

# 生成服务器私钥和证书签名请求(CSR)
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/CN=localhost"  # CN 通常设为服务器域名或 IP

# 用 CA 证书签发服务器证书
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365

# 生成客户端私钥和证书签名请求(CSR)
openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr -subj "/CN=Client"

# 用 CA 证书签发客户端证书
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 365
```

2. 客户端配置（taos.cfg）

```bash
tlsCliKeyPath  /path/client.key
tlsCliCertPath /path/client.crt
tlsCaPath      /path/ca.crt
enableTLS      1 
```

3. 服务端配置（taos.cfg）

```bash
tlsCliKeyPath  /path/client.key
tlsCliCertPath /path/client.crt
tlsSvrKeyPath  /path/server.key
tlsSvrCertPath /path/server.crt
tlsCaPath      /path/ca.crt
enableTLS      1
```

4. 启动服务端后，使用客户端访问即可

#### 性能说明

启用 TLS 会对性能产生一定影响，整体性能下降约 1%~4%，实际性能为非 TLS 模式的 96%~99%。

#### 运维和升级

不支持动态升级（需重启服务使配置生效）。

### 修改节点地址

TDengine 的 dnode 以 `FQDN:端口` 作为节点地址，并把它持久化到配置文件和数
据目录中。主机名、域名或 `serverPort` 变化后，如果直接以新地址启动，taosd 会
因为本机地址与数据目录中记录的地址不一致而拒绝启动。

修改地址需要在**全部 dnode 都停止**的情况下，在每个节点上执行本文的离线命令
，把配置和数据目录中的旧地址改写为新地址，然后正常启动。该命令不修改数据。

修改地址包括“预检”和“执行”两步操作。

:::note

历史版本曾支持使用 `ep.json` 文件在启动时映射新旧地址。该方式已不再支持，遗留
文件会被忽略。如果数据目录中存在 `<dataDir>/dnode/ep.json`，请删除它，并按本
文步骤操作。

:::

#### 适用范围

- 单机部署和多 dnode 集群。集群中每个节点都要执行。
- 可以只改 FQDN、只改端口，或两者同时修改。
- 只支持状态正常、数据完整的系统。执行前必须已完成数据落盘并正常停止全部节点。
- 执行成功后不支持回退。执行前请按需备份配置目录和数据目录。

#### 命令语法

预检，逐节点生成预检结果文件：

```bash
taosd [-c <cfgDir>] --rewrite-endpoints-precheck <预检文件保存目录> \
  --map '<旧FQDN:旧端口=新FQDN:新端口>' [--map '<旧FQDN:旧端口=新FQDN:新端口>' ...]
```

`--map` 至少一条，集群内所有节点使用同一份映射，仅列出需要修改的节点即可，未列入的
节点地址不会被改写。

执行，按已收齐的预检结果文件改写本节点：

```bash
taosd [-c <cfgDir>] --rewrite-endpoints-apply <预检文件保存目录>
```

#### 操作步骤

1. 停止数据写入、元数据操作和订阅消费，暂停流式计算，等待未完成的事务等在途操作结束。

2. 对所有数据库和 mnode 执行 flush：

   ```sql
   flush database <dbName>;
   flush mnode;
   ```

   等待 flush 完成后，正常停止全部 dnode 节点：

   ```bash
   systemctl stop taosd
   ```

   在改写完成前不要重新启动任何节点。

3. 如有必要，配置新 FQDN 的解析。在 DNS 或 `/etc/hosts` 中添加记录，确认可以解析到对应
   主机：

   ```bash
   getent hosts <newFqdn>
   ```

4. 在每个节点上执行预检，`<预检文件保存目录>` 需为一个已存在的目录，例如：

   ```bash
   taosd -c <cfgDir> --rewrite-endpoints-precheck <预检目录> \
     --map 'localhost:6030=tdengine-1.example:6030' \
     --map 'localhost:7030=tdengine-2.example:6030'
   ```

   预检不修改任何文件。成功时在预检目录中生成 `precheck.dnode<节点ID>.json`，
   并列出将要修改的位置、旧值和新值。任何一项检查失败时，文件中的 `status`
   为 `failed`，命令以非零码退出，需按提示处理后重新预检。

5. 收集所有节点的预检结果文件，放到同一个目录，并确保每个节点都能读到
   **全部**文件（可在每台机器上各放一份）。

6. 在每个节点上执行改写：

   ```bash
   taosd -c <cfgDir> --rewrite-endpoints-apply <预检目录>
   ```

   命令会重新校验所有节点的状态是否一致，以及本节点状态是否与预检结果一致，
   然后才修改文件。校验不通过时不修改任何文件。命令执行失败时也不修改任何文件。

   改写期间，每个被修改文件的所在目录会临时出现 `<原文件名>.before-rewrite-endpoints`
   备份。无论命令执行成功与否，这些备份文件都会在命令结束后被自动删除。但如果
   命令被中断或强制结束，这些备份可能残留，此时 taosd 会拒绝启动。请按下面的
   规则处理：

   - 若被修改文件都还有对应的备份，说明改写没有写完。此时，应删除被修改的文件，
     把备份文件重命名为 `<原文件名>`，这将回滚到改写前的状态。
   - 若部分备份文件已不存在，且相关文件中已是新地址，说明写入阶段已结束，只
     是删除备份时被中断。此时，删除剩余的备份文件即可彻底完成改写。
   - 若部分备份文件已不存在，且相关文件中仍是旧地址，说明写入阶段尚未开始就
     被意外中断了。此时，删除剩余的备份文件即可回滚到改写前的状态。
   - **注意**：在集群环境中，若只有部分节点存在残留备份，且通过上述操作将这
     些节点回滚到了改写前的状态，请务必重新执行改写操作，以确保整个集群状态
     一致。

7. 启动集群：

   ```bash
   systemctl start taosd
   ```

8. 检查修改结果：

   - 节点地址：`show dnodes` 和 `show mnodes` 等命令所展示的 `endpoint`
     应为新地址，状态为 `ready`。启动后的数秒内 `status` 可能为 `offline`
     `note` 为 `status not received`，稍后会变为 `ready`。

   - 数据：原有数据库、表和数据都能查询到，行数与修改前一致。继续写入后，新数
     据可以正常查询。

   - 流式计算：恢复流式计算的执行，等待大约 10 - 20 秒，`show <db>.streams`
     中流的状态为 `Running` 或 `Idle`。

   - 数据订阅：消费者使用新地址可以继续订阅。

#### 本命令不会修改的地址

本命令只会修改 TDengine TSDB 集群自身使用的 dnode 地址，所有其它地址需要
根据实际情况手工修改，这些地址包括但不限于：

| 项目 | 修改方式 |
| --- | --- |
| monitor FQDN | `ALTER DNODE <id> 'monitorFqdn' '<new monitor fqdn>'` 或 `ALTER ALL DNODES 'monitorFqdn' '<new monitor fqdn>'` |
| 联邦查询外部源 | `ALTER EXTERNAL SOURCE` |
| ANode 地址 | 删除后重新添加 |
| XNode 地址 | 停止相关任务，删除并重新添加 XNode 节点，修改任务对应的 XNode ID 后重启任务 |
| 客户端、连接器、TMQ 消费者 | 未与 taosd 共用 `taos.cfg` 时，手工修改各自的连接配置 |
| taosAdapter | 未与 taosd 共用 `taos.cfg` 时，修改 `taosAdapter` 读取的 `taos.cfg` |
| taosKeeper、taosExplorer 等 | 检查各自配置中是否使用了旧地址 |

#### 常见问题

##### 启动失败，日志中出现 `Unable to resolve FQDN`

新 FQDN 无法解析。检查 DNS 或 `/etc/hosts` 配置，并确认 `taos.cfg` 和
`<dataDir>/dnode/config/local.json` 中的 `fqdn`、`firstEp`、`secondEp` 都是新
地址。

##### 启动失败，日志中出现 `need to be reconfigured`

本机地址与 `<dataDir>/dnode/dnode.json` 中记录的地址不一致，说明没有执行改
写命令，或只改了配置文件。日志中同时给出正确的命令。按本文步骤重新预检和执
行改写。

##### 预检或执行报告 `apply failed`

输出中会给出具体文件、字段、实际值和期望值。常见原因：预检之后又有写入导致
状态变化、预检结果文件不全、不同机器的映射不一致、`ep.json` 之外的手工修改。
按提示重新预检。

##### 日志中出现 `file .../ep.json is ignored`

数据目录中存在遗留的 `ep.json`。该文件已不再生效，删除即可，并按本文步骤修改
地址。

##### 修改期间 taosd 处于运行状态

命令发现实例仍在运行时会拒绝执行。必须先正常停止 taosd 再操作。

##### 启动失败，日志中出现 `backup left behind`

数据目录、配置目录或磁盘目录中残留了 `<原文件名>.before-rewrite-endpoints`
文件，日志和标准输出中会列出全部路径。说明上一次改写没有正常结束（例如被中
断或强杀）。按第 6 步的说明处理这些备份后再启动。
