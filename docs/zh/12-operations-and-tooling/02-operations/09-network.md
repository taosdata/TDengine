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

### 修改节点 FQDN

TDengine 的 dnode 以 `FQDN:端口` 作为节点地址，并把它持久化到配置文件和数据目录中。主机名变更后，taosd 会因为本机地址与数据目录中记录的地址不一致而启动失败。本节说明在单机部署下，主机名变更后让 taosd 以新 FQDN 启动并继续提供服务的手工方法。

:::note

修改 FQDN 的迁移工具正在开发中。工具发布前，请按本节步骤手工修改。

:::

#### 适用范围

- 仅适用于单机部署，即集群中只有一个 dnode。多 dnode 集群请勿使用本方法。
- 只修改 FQDN，端口保持不变。
- 未启用配置文件加密（`CFG_KEY`）。启用加密后，相关 JSON 文件无法手工编辑。
- 最多一个 snode，且无副本。
- 已在 TDengine `v3.4.x` 上验证，包括数据写入、查询和流式计算。

#### 原理说明

taosd 的本机地址保存在以下位置：

| 位置 | 说明 |
| --- | --- |
| `taos.cfg` | `fqdn`、`firstEp`、`secondEp` 配置项 |
| `<dataDir>/dnode/config/local.json` | 持久化的配置项，加载顺序在 `taos.cfg` 之后，会覆盖 `taos.cfg` 中的同名配置 |
| `<dataDir>/dnode/dnode.json` | 本机 dnodeId 和 dnode 列表 |
| mnode、vnode 的元数据 | dnode 的地址、副本地址等 |

启动时 taosd 比较本机地址与 `dnode.json` 中记录的地址，不一致时报错退出。`<dataDir>/dnode/ep.json` 用于声明旧地址与新地址的对应关系。存在该文件时，taosd 在内存中把旧地址替换为新地址并跳过上述检查。taosd 正常停止时会把新地址写回 `dnode.json`、mnode 元数据，并把 `ep.json` 重命名为 `ep.json.bak`。

只修改 `taos.cfg` 不能解决问题：`local.json` 中的旧 FQDN 会覆盖 `taos.cfg`，taosd 在读取 `ep.json` 之前就因为旧 FQDN 无法解析而退出。因此 `taos.cfg`、`local.json`、`ep.json` 三处都需要修改。

#### 操作前准备

下文使用以下占位符，操作时替换为实际值：

| 占位符 | 含义 | 默认值 |
| --- | --- | --- |
| `<cfgDir>` | taosd 配置目录 | `/etc/taos` |
| `<dataDir>` | taosd 数据目录，即 `taos.cfg` 中的 `dataDir` | `/var/lib/taos` |
| `<oldFqdn>` | 原 FQDN | - |
| `<newFqdn>` | 新 FQDN | - |
| `<port>` | taosd 端口，即 `serverPort` | `6030` |

1. 主机名变更前，记录当前状态，便于修改后核对：

   ```bash
   taos -h <oldFqdn> -P <port> -s "show dnodes; show snodes; show databases;"
   ```

   如果使用了流式计算，同时记录各数据库的 `show <db>.streams` 结果和流结果表的行数。

2. 停止 taosd，然后备份配置目录、数据目录和 `/etc/hosts`：

   ```bash
   systemctl stop taosd
   cp -a <cfgDir> <cfgDir>.bak
   cp -a <dataDir> <dataDir>.bak
   cp -a /etc/hosts /etc/hosts.bak
   ```

   必须正常停止 taosd，不要使用 `kill -9`。

#### 操作步骤

以下步骤均在 taosd 停止状态下进行。

1. 配置新 FQDN 的解析。修改主机名后，在 `/etc/hosts` 或 DNS 中添加新 FQDN 的解析，确认可以解析到本机：

   ```bash
   getent hosts <newFqdn>
   ```

2. 修改 `<cfgDir>/taos.cfg`，把 `fqdn`、`firstEp`、`secondEp` 中的旧 FQDN 改为新 FQDN，端口不变：

   ```text
   fqdn      <newFqdn>
   firstEp   <newFqdn>:<port>
   ```

   如果配置了 `secondEp` 且指向本机，同样修改。

3. 修改 `<dataDir>/dnode/config/local.json`。查看其中与地址相关的配置项：

   ```bash
   python3 - <<'EOF'
   import json
   c = json.load(open("<dataDir>/dnode/config/local.json"))["configs"]
   for k in ("fqdn", "firstEp", "secondEp", "serverPort"):
       print(k, c.get(k))
   EOF
   ```

   把 `fqdn`、`firstEp`、`secondEp` 中的旧 FQDN 改为新 FQDN，端口不变。修改后再次执行上述命令确认。文件不存在时跳过本步骤。

4. 查看 `<dataDir>/dnode/dnode.json` 中的 `dnodeId`，单机部署一般为 `1`。新建 `<dataDir>/dnode/ep.json`：

   ```json
   {
     "dnodes": [
       {
         "id": 1,
         "fqdn": "<oldFqdn>",
         "port": 6030,
         "new_fqdn": "<newFqdn>",
         "new_port": 6030
       }
     ]
   }
   ```

   - `id` 填写 `dnodeId`。
   - `port` 和 `new_port` 都填写原端口，两者必须相同。
   - `fqdn` 和 `port` 是必填字段。

5. 启动 taosd：

   ```bash
   systemctl start taosd
   ```

   taosd 日志（位于 `logDir`，默认 `/var/log/taos/taosdlog.0`）中应出现以下内容：

   ```text
   check global fqdn:<newFqdn> and port:<port>
   dnode:1, will update ep:<oldFqdn>:<port> to <newFqdn>:<port>
   dnode:1, endpoint changed
   The daemon initialized successfully
   ```

6. 使用新 FQDN 连接，等待 dnode 状态变为 `ready`：

   ```bash
   taos -h <newFqdn> -P <port> -s "show dnodes;"
   ```

   启动后的数秒内，`status` 可能显示为 `offline`，`note` 为 `status not received`，稍后会变为 `ready`。

7. 正常停止 taosd 后再启动一次。taosd 会把 `ep.json` 重命名为 `ep.json.bak`，`dnode.json` 中记录的已是新 FQDN，此后启动不再需要 `ep.json`。

#### 结果验证

使用新 FQDN 连接，与修改前记录的状态核对。

1. 节点状态：`show dnodes` 和 `show snodes` 中的 endpoint 均为 `<newFqdn>:<port>`，状态为 `ready`。

2. 数据：原有数据库、表和数据都能查询到，行数与修改前一致。继续写入后，新数据可以正常查询。

3. 流式计算：`show <db>.streams` 中流的状态为 `Running` 或 `Idle`，写入新数据后，流结果表的行数随之增加。

   taosd 重启或修改 FQDN 后，流会先显示为 `Undeployed`，`message` 为 `Mnode may be unstable`，大约 10～20 秒后恢复为 `Running` 或 `Idle`。这是正常的重新部署过程，不代表流已失效。

#### 修改关联组件

上述步骤只修改了 taosd 自身的 `taos.cfg` 和数据目录。客户端和其他组件如果在各自的配置中使用了旧 FQDN，会继续连接旧地址，需要分别修改并重启对应进程。

| 组件 | 需要检查的配置 | 说明 |
| --- | --- | --- |
| 客户端 | 客户端机器上 `taos.cfg` 的 `firstEp`、`secondEp`；应用连接串；环境变量 `TAOS_FIRST_EP` | 与 taosd 共用同一份 `taos.cfg` 时无需修改 |
| taosAdapter | `taosadapter.toml` 中 `taosConfigDir` 指向的 `taos.cfg` | 与 taosd 共用 `taos.cfg` 时只需重启；应用中的 `http://<oldFqdn>:6041` 需要修改 |
| taosKeeper | `taoskeeper.toml` 中 `[tdengine]` 的 `host`；taosd `taos.cfg` 中的 `monitorFqdn` | 默认 `host` 为 `127.0.0.1`，无需修改；`monitorFqdn` 为 taosKeeper 的地址 |
| taosExplorer | `explorer.toml` 中的 `cluster`、`cluster_native` | 默认 `cluster` 为 `http://localhost:6041`，无需修改 |

具体说明如下：

- **客户端**：应用连接串中的主机名需要修改，例如 JDBC 的 `jdbc:TAOS://<oldFqdn>:6030/`、Python 的 `taos.connect(host="<oldFqdn>")`、Go 的 DSN。
- **taosAdapter**：taosAdapter 通过 `taosConfigDir`（默认 `/etc/taos`）读取 `taos.cfg` 连接 taosd。taosAdapter 部署在其他机器上时，修改该机器上的 `taos.cfg`。
- **taosKeeper**：`[tdengine]` 中的 `host` 是 taosKeeper 连接 taosAdapter 的地址，使用了旧 FQDN 时才需要修改。taosKeeper 与 taosd 部署在同一台机器上，且 `monitorFqdn` 使用了旧 FQDN 时，将其改为新 FQDN，然后重启 taosd 和 taosKeeper。
- **taosExplorer**：`cluster` 或 `cluster_native`（形如 `taos://<oldFqdn>:6030`）使用了旧 FQDN 时修改，然后执行 `systemctl restart taos-explorer`。浏览器中保存的旧访问地址需要更新。

#### 回滚

修改失败需要恢复时，停止 taosd，用操作前的备份恢复配置目录、数据目录和 `/etc/hosts`，然后启动 taosd：

```bash
systemctl stop taosd
rm -rf <cfgDir> <dataDir>
cp -a <cfgDir>.bak <cfgDir>
cp -a <dataDir>.bak <dataDir>
cp -a /etc/hosts.bak /etc/hosts
systemctl start taosd
```

恢复后需要保证旧 FQDN 可以解析到本机。

#### 常见问题

##### 启动失败，日志中出现 `Unable to resolve FQDN`

日志内容类似：

```text
check global fqdn:<oldFqdn> and port:<port>
failed to apply cfg at ... since Unable to resolve FQDN
failed to start since apply config error
```

`check global fqdn` 后仍是旧 FQDN，说明 `local.json` 未修改，旧值覆盖了 `taos.cfg`。按操作步骤 3 修改 `local.json`。

##### 启动失败，日志中出现 `need to be reconfigured`

本机地址与 `dnode.json` 中记录的地址不一致，且没有找到 `ep.json`。检查 `ep.json` 是否位于 `<dataDir>/dnode/` 目录，内容是否为合法 JSON，`id` 是否与 `dnode.json` 中的 `dnodeId` 一致。

##### 修改后流显示为 `Undeployed`

taosd 启动后流需要重新部署。等待 10～20 秒后再次执行 `show <db>.streams`，状态会恢复为 `Running` 或 `Idle`。

##### 数据目录中出现 `ep.json.bak`

taosd 正常停止时自动将 `ep.json` 重命名为 `ep.json.bak`，表示新地址已写入 `dnode.json`。确认服务正常后可以删除该文件。

##### 修改期间 taosd 处于运行状态

taosd 运行时修改的文件可能在停止时被覆盖。必须先停止 taosd，再修改文件。
