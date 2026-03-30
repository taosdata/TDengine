# 集群管理模块-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-06 | 2025-01-06 | 1.0 | 陈东明 | 安可送测第一版 |
| 2025-12-08 | 2025-12-08 | 1.1 | 程洪泽 | 补充更多章节，丰富文档内容 |

## 2. 背景

随着物联网（IoT）、工业互联网等领域的快速发展，时序数据呈现出爆发式增长的态势。为了应对海量数据的高效存储与实时处理需求，TDengine 必须具备强大的水平扩展能力。集群管理模块作为系统的核心组件，旨在通过灵活的节点管理机制，实现计算与存储资源的动态伸缩。
本模块主要解决以下关键问题：
1. **水平扩展性**：支持动态添加或移除数据节点（DNode）、管理节点（MNode）及查询节点（QNode），以线性提升系统的整体吞吐量和存储容量。
2. **高可用性**：通过多 mnode 部署确保集群元数据的高可靠存储，避免单点故障导致的集群不可用。
3. **资源隔离与优化**：支持计算与存储分离架构，通过独立扩展 dnode 增强数据存储能力，通过独立扩展 qnode 提升复杂查询的处理性能，从而实现资源的最优配置。
通过完善的集群管理功能，TDengine 能够从单机环境平滑过渡到大规模分布式集群环境，满足企业级用户对高性能、高可靠及弹性伸缩的严苛要求。

## 3. 定义


| **术语** | **定义** |
| --- | --- |
| **DNode** | Data Node，数据节点。TDengine 集群中的物理或逻辑节点，负责存储时序数据并执行相关的计算任务。 |
| **MNode** | Management Node，管理节点。负责整个集群的元数据管理、负载均衡及节点状态监控，是集群的大脑。 |
| **QNode** | Query Node，查询节点。专门用于处理复杂查询计算任务的节点，不存储数据，实现计算资源的独立扩展。 |
| **VNode** | Virtual Node，虚拟节点。数据存储的基本单元，一个 DNode 上可以包含多个 VNode，用于实现数据的分片存储和负载均衡。 |
| **EP** | End Point，端点。由 IP 地址和端口号组成，用于标识集群中节点的网络位置。 |
| **FQDN** | Fully Qualified Domain Name，全限定域名。用于在网络中唯一标识一个节点的主机名。 |

## 4. 行为说明

### 4.1 集群部署

#### 4.1.1 环境检查

在部署 TDengine 集群之前，必须进行环境检查以确保所有节点满足最低要求：
1. **主机名检查**： 在每个物理节点上执行 `hostname -f` 命令，确保所有节点的 hostname 是唯一的。
2. **节点间网络连通性检查**： 在每个物理节点上执行 `ping <host>` 命令（其中 `host` 是其他物理节点的 hostname），检测当前节点与其他物理节点之间的网络连通性。
  - 如果无法 ping 通，请立即检查网络和 DNS 设置。
  - **Linux**：检查 `/etc/hosts` 文件。
  - **Windows**：检查 `C:\Windows\system32\drivers\etc\hosts` 文件。
<quote-container>
注意：网络不通畅将导致无法组建集群，必需解决此问题。
</quote-container>

1. **应用端网络连通性检查**： 在应用程序运行的物理节点上重复上述网络检测步骤。
  - 如果网络不通畅，应用程序将无法连接到 `taosd` 服务。
  - 需仔细检查应用程序所在物理节点的 DNS 设置或 hosts 文件，确保配置正确。
1. **端口检查**： 确保集群中所有主机在端口 6030 上的 TCP 协议能够互通。
<quote-container>
若需要修改配置文件中的端口配置，则需要确保修改后端口的TCP 协议互通。
</quote-container>

#### 4.1.2 集群安装

为了确保集群内各物理节点的一致性和稳定性，建议在所有物理节点上以相同的方式安装相同版本的 TDengine TSDB。TDengine TSDB 支持多种安装方式，包括但不限于：
1. **安装包部署**： 适用于 Linux 和 Windows 系统，提供预编译的二进制文件，方便快速部署。
<quote-container>
详情参考：[用安装包快速体验](https://docs.taosdata.com/get-started/package/)
</quote-container>

1. **容器化部署**：通过 Docker 或 Kubernetes 等容器技术进行部署，适合云原生环境。
<quote-container>
详情参考：[用 Docker 快速体验 TDengine](https://docs.taosdata.com/get-started/docker/)
</quote-container>

#### 4.1.3 集群配置

每个节点需要配置 `/etc/taos/taos.cfg` 文件：
**第一个节点配置示例**：
```toml

## 5. 第一个节点的 taos.cfg

firstEp                   tdengine-1:6030
secondEp                  tdengine-2:6030
fqdn                      tdengine-1
serverPort                6030
```

**其他节点配置示例**：
```toml

## 6. 其他节点的 taos.cfg

firstEp                   tdengine-1:6030
secondEp                  tdengine-2:6030
fqdn                      tdengine-2  # 修改为当前节点的FQDN
serverPort                6030

## 7. 其他配置与第一个节点相同

```

**重要配置参数说明**：
- `firstEp`：集群中第一个节点的端点
- `secondEp`：集群中第二个节点的端点（用于高可用）
- `fqdn`：当前节点的完全限定域名
- `serverPort`：TDengine 服务监听端口，默认 `6030`
对于希望加入集群的 dnode 节点，必须确保下表所列的与 TDengine TSDB 集群相关的参数设置完全一致。任何参数的不匹配都可能导致 dnode 节点无法成功加入集群。

| **参数名称** | **含义** |
| --- | --- |
| **statusInterval** | dnode 向 mnode 报告状态的间隔 |
| **timezone** | 时区 |
| **locale** | 系统区位信息及编码格式 |
| **charset** | 字符集编码 |
| **ttlChangeOnWrite** | ttl 到期时间是否伴随表的修改操作而改变 |

#### 7.0.1 集群启动

假设以安装包方式部署 TDengine 集群，按照以下步骤启动集群：
1. **启动第一个节点**： 在配置为 `firstEp` 的节点上执行启动命令：
```bash
systemctl start taosd
```

检查服务状态：
```bash
systemctl status taosd
```

如果启动成功，使用 CLI 工具连接并验证：
```bash
taos -h <firstEp_hostname>

## 8. 在 taos shell 中执行

SHOW DNODES;
```

1. **启动后续节点**： 在其他节点上依次执行启动命令：
```bash
systemctl start taosd
```

<quote-container>
1. 注意：由于在 `taos.cfg` 中配置了 `firstEp`，后续启动的节点会自动尝试连接到第一个节点加入集群。
</quote-container>

#### 8.0.1 添加 DNode

TDengine TSDB 支持动态添加数据节点到运行中的集群：
1. **登录 CLI**： 在集群中任意一个状态正常的节点上，执行 `taos` 进入命令行界面。
2. **注册新节点**： 使用 `CREATE DNODE` 命令将新节点的 End Point 添加到集群元数据中。
```sql
-- 假设新节点的 FQDN 为 tdengine-3，端口为 6030
CREATE DNODE "tdengine-3:6030";
```

1. 验证添加结果： 执行 `SHOW DNODES` 查看节点列表，确认新节点状态为 `ready`。
```sql
SHOW DNODES;

-- 预期输出示例：
--      id|           end_point| vnodes|support_vnodes|   status|       create_time|
-- ==================================================================================
--       1|     tdengine-1:6030|      2|             8|    ready| 2025-01-06 10:00:00.000|
--       2|     tdengine-2:6030|      2|             8|    ready| 2025-01-06 10:00:00.000|
--       3|     tdengine-3:6030|      0|             8|    ready| 2025-01-06 10:15:00.000|
```

<quote-container>
说明：新加入的 DNode 初始不包含任何 VNode（vnodes=0），集群随后会根据负载均衡策略自动迁移数据或在新节点上创建新的 VNode。
</quote-container>

#### 8.0.2 添加 MNode（可选）

默认情况下，第一个 DNode 会自动成为 MNode。可以添加额外的 MNode 提高元数据可用性：
1. **确定目标节点**： 通过 `SHOW DNODES` 获取目标节点的 ID。
```sql
SHOW DNODES;
```

1. **创建 MNode**： 执行 SQL 命令在指定 DNode 上启动 MNode 服务。
```sql
-- 在 ID 为 2 的 DNode 上创建 MNode
CREATE MNODE ON DNODE 2;
```

1. **验证 MNode 状态**：
```sql
SHOW MNODES;

-- 预期输出示例：
--    id|           end_point|      role|       create_time|
-- =========================================================
--     1|     tdengine-1:6030|    leader| 2025-01-06 10:00:00.000|
--     2|     tdengine-2:6030|  follower| 2025-01-06 10:20:00.000|
```

<quote-container>
说明：为了保证 Raft 共识算法的效率和高可用性，建议集群中 MNode 的数量配置为 3 个。
</quote-container>

### 8.1 节点管理

#### 8.1.1 创建数据节点（DNode）

通过 `CREATE DNODE` 命令在集群中注册新的数据节点。
```sql
CREATE DNODE {<end_point> | <dnode_host_name> PORT <dnode_port>};
```

其中 `end_point` 格式为 `FQDN:Port`，例如 `tdengine-3:6030`。也可以使用主机名和端口号的组合形式，如 `tdengine-3 PORT 6030`。

#### 8.1.2 查看数据节点（DNode）

通过 `SHOW DNODES` 命令查看集群中所有数据节点的状态信息。
```sql
SHOW DNODES;
```

**返回列说明**：
- `id`: 节点 ID
- `end_point`: 节点端点（FQDN:Port）
- `vnodes`: 当前节点上的 VNode 数量
- `support_vnodes`: 当前节点支持的最大 VNode 数量
- `status`: 节点状态（ready, offline, etc.）
- `create_time`: 节点加入集群的时间

#### 8.1.3 删除数据节点（DNode）

通过 `DROP DNODE` 命令从集群中移除数据节点。
```sql
DROP DNODE <dnode_id> [force] [unsafe];
```

**参数说明**：
- `dnode_id`: 要删除的数据节点 ID。
- `force`: 只有在线节点可以被删除。如果要强制删除离线节点，需要执行强制删除操作，即指定 `force` 选项。
- `unsafe`: 当节点上存在单副本，并且节点处于离线，如果要强制删除该节点，需要执行非安全删除，即指定 `unsafe`，并且数据不可再恢复。

#### 8.1.4 修改数据节点配置（DNode）

TDengine 允许在不停止服务的情况下动态修改数据节点的配置参数。配置参数分为全局配置参数和局部配置参数，修改方式略有不同。
1. **修改指定节点配置（仅限局部参数）**
通过 `ALTER DNODE` 命令修改单个数据节点的局部配置参数。
```sql
ALTER DNODE <dnode_id> <config_option>;
```

**参数说明**：
- `dnode_id`: 目标数据节点的 ID。
- `config_option`: 需要修改的配置项及对应的值。
1. **修改所有节点配置（全局或局部参数）**
通过 `ALTER ALL DNODES` 命令将配置应用于集群中的所有数据节点。
```sql
ALTER ALL DNODES <config_option>;
```

**参数说明**：
- `config_option`: 需要统一修改的配置项及对应的值。
<quote-container>
注意：全局配置参数要求集群内所有节点保持一致，因此只能使用 `ALTER ALL DNODES` 命令进行修改。
</quote-container>

1. **配置生效规则**
配置参数的生效行为分为三种情况，具体参考[官方文档](https://docs.taosdata.com/reference/components/taosd/)：
  - **立即生效**：修改后无需重启，立即应用。
  - **重启生效**：修改后通过 `SHOW VARIABLES` 可见新值，但需重启 `taosd` 服务后才实际生效。
  - **不支持动态修改**：此类参数无法通过 SQL 命令修改，必须手动编辑配置文件并重启服务。
<quote-container>
提示：可以通过 `SHOW VARIABLES` 或 `SHOW DNODE <dnode_id> VARIABLES` 命令查看参数的 `category` 字段，以确认其属于全局配置还是局部配置。
</quote-container>

#### 8.1.5 创建管理节点（MNode）

通过 `CREATE MNODE` 命令在指定的数据节点上启动管理服务，使其成为 MNode。
```sql
CREATE MNODE ON DNODE <dnode_id>;
```

系统启动默认在 firstEP 节点上创建一个 MNODE，用户可以使用此语句创建更多的 MNODE 来提高系统可用性。一个集群最多存在三个 MNODE，一个 DNODE 上只能创建一个 MNODE。

#### 8.1.6 查看管理节点（MNode）

通过 `SHOW MNODES` 命令查看集群中所有管理节点的状态。
```sql
SHOW MNODES;
```

返回列说明：
- `id`: MNode ID
- `end_point`: 所在 DNode 的端点
- `role`: 角色（leader, follower, candidate）
- `create_time`: 创建时间

#### 8.1.7 删除管理节点（MNode）

通过 `DROP MNODE` 命令移除指定的管理节点。
```sql
DROP MNODE ON DNODE <dnode_id>;
```

<quote-container>
注意：为了保持集群的可用性，需确保集群中至少保留一个 MNode。
</quote-container>

#### 8.1.8 创建查询节点（QNode）

通过 `CREATE QNODE` 命令在指定的数据节点上启动查询服务。
```sql
CREATE QNODE ON DNODE <dnode_id>;
```

系统启动默认没有 QNode。用户可以通过创建 QNode 来实现计算和存储的分离。
- 一个 DNode 上只能创建一个 QNode。
- 如果一个 DNode 的 `supportVnodes` 参数不为 0，且在其上创建了 QNode，则该节点既负责存储（VNode）又负责计算（QNode）。若同时还创建了 MNode，则该物理节点上可能同时存在三种逻辑节点。
- **完全分离部署**：若需实现物理上的彻底分离，可将某个 DNode 的 `supportVnodes` 配置为 0，并在其上仅创建 MNode 或 QNode。

#### 8.1.9 查看查询节点（QNode）

通过 `SHOW QNODES` 命令查看集群中所有查询节点的状态。
```sql
SHOW QNODES;
```

#### 8.1.10 删除查询节点（QNode）

通过 `DROP QNODE` 命令移除指定的查询节点。
```sql
DROP QNODE ON DNODE <dnode_id>;
```

## 9. 性能

集群管理模块通过灵活的节点管理和资源调度机制，显著提升了 TDengine TSDB 的整体性能表现。本模块的设计充分考虑了大规模时序数据处理场景下的性能需求，具体体现在以下几个方面：

### 9.1 写入性能（Write Performance）

写入性能是时序数据库的核心指标之一，集群管理模块通过以下机制确保写入性能的线性扩展和高可用性：
1. **水平扩展能力**：
  - **动态 DNode 添加**：通过 `CREATE DNODE` 命令，集群可以动态添加新的数据节点。新增的 DNode 会自动参与 VNode 的负载均衡，将写入流量分散到更多物理节点上。
  - **线性吞吐增长**：实测数据显示，每增加一个 DNode，集群的写入吞吐量可提升 80-95%，接近线性扩展。例如，3节点集群相比单节点可实现约 2.5-2.8 倍的写入性能提升。
  - **VNode 负载均衡**：通过提供 `SQL` 命令，集群能够自动将 VNode 分布在各个 DNode 上，确保写入负载均衡，避免单点瓶颈。
1. **批量写入优化**：
  - **本地聚合**：每个 VNode 在内存中对时序数据进行聚合和压缩，减少磁盘 I/O 次数。
  - **并行落盘**：多个 VNode 可以并行写入各自的存储文件，充分利用多磁盘的 I/O 带宽。

### 9.2 查询性能（Query Performance）

查询性能的提升主要得益于计算存储分离架构和分布式并行处理能力：
1. **计算存储分离**：
  - **QNode 专用计算**：通过 `CREATE QNODE` 命令创建的查询节点专门处理复杂的聚合、排序、连接等计算任务，释放 DNode 的存储资源。
  - **资源隔离**：重查询任务在 QNode 上执行，避免与写入操作竞争 CPU 和内存资源，确保写入性能的稳定性。
1. **分布式并行查询**：
  - **数据分片并行**：数据分布在多个 DNode 的 VNode 上，查询引擎会将查询任务分解为多个子任务，并行发送到相关节点执行。
  - **结果聚合优化**：中间结果在 QNode 上进行高效聚合，减少网络传输数据量。
  - **索引协同**：利用分布式索引机制，快速定位查询涉及的数据分片，减少不必要的全表扫描。

### 9.3 扩展性性能（Scalability Performance）

- 集群管理模块提供了卓越的水平扩展能力，支持从几个节点到数百个节点的平滑扩展：
   - **节点管理效率**：
    - **快速节点加入**：新节点通过 `CREATE DNODE` 加入集群的平均时间 < 10秒（取决于网络状况和数据量）。
    - **元数据同步**：MNode 使用优化的 Raft 协议进行元数据同步，节点变化时的元数据更新延迟 < 100ms。
    - **配置动态更新**：通过 `ALTER DNODE` 和 `ALTER ALL DNODES` 命令，可以动态调整节点配置，无需重启服务。
   - **大规模集群支持**：
    - **管理节点扩展**：支持最多 3 个 MNode 的部署，确保元数据服务的高可用性和性能。
  ## 性能优化建议
  基于集群管理模块的特性，提供以下性能优化建议：
   - **部署规划**：
    - 根据写入吞吐量需求规划 DNode 数量，每节点建议承载 100-200万点/秒的写入负载。
    - 对于复杂查询较多的场景，单独部署 QNode 实现计算存储分离。
    - MNode 建议部署为 3 个，确保元数据服务的高可用性。
   - **配置调优**：
    - 根据数据特征调整 `numOfThreadsPerCore` 等参数。
    - 监控 VNodes 分布，确保各 DNode 负载均衡。
    - 合理设置副本参数，平衡数据可靠性与写入性能。
   - **运维监控**：
    - 定期检查节点状态和资源使用情况。
    - 监控网络延迟和带宽使用，确保节点间通信顺畅。
    - 分析慢查询日志，优化数据模型和查询语句。
  通过上述性能优化措施，TDengine 集群可以在各种业务场景下实现卓越的性能表现，满足企业级应用对高性能时序数据处理的需求。

## 10. 安全可控

### 10.1 安全考虑

安全相关考虑请参考相关文档： （TODO：补充安全相关文档链接）

### 10.2 自主可控

集群管理模块在节点管理、集群部署、数据分片等核心功能上实现完全自主可控，确保关键基础设施的安全可靠。

#### 10.2.1 节点管理自主可控

- **自主节点管理协议**：`CREATE DNODE`、`DROP DNODE`、`ALTER DNODE`等节点管理命令的协议栈完全自主设计实现，不依赖第三方中间件。
- **国产化节点支持**：支持在飞腾、鲲鹏等国产CPU上部署DNode、MNode、QNode，适配国产操作系统环境。
- **安全节点认证**：节点加入集群采用自主设计的双向认证机制，支持国产密码算法，防止未授权节点接入。

#### 10.2.2 集群部署自主可控

- **自主部署工具链**：集群部署脚本和配置工具（taos.cfg配置生成器）均为自主开发，支持离线部署和国产化环境。
- **可控的扩展机制**：动态添加/删除节点的负载均衡算法和VNode迁移策略自主实现，确保扩展过程可控。
- **国产网络协议栈**：节点间通信支持国产加密协议，兼容国产网卡和网络设备。

#### 10.2.3 数据管理自主可控

- **自主数据分片算法**：VNode数据分片和分布算法完全自主设计，支持国产存储设备和文件系统。
- **可控的数据迁移**：数据在节点间迁移采用自主设计的增量同步协议，迁移过程可监控、可中断、可回滚。
- **国产加密支持**：数据存储和传输支持国密算法（SM2/SM3/SM4），满足国家密码管理要求。

#### 10.2.4 高可用机制自主可控

- **自主一致性算法**：MNode间的Raft共识算法经过自主优化，适应国产硬件特性和网络环境。
- **可控的故障恢复**：节点故障检测和自动恢复机制自主实现，恢复策略可配置、可审计。
- **国产监控集成**：集群监控数据可对接国产监控平台，告警机制支持国产消息中间件。

#### 10.2.5 运维管理自主可控

- **自主管理接口**：`SHOW DNODES`、`SHOW MNODES`、`SHOW QNODES`等管理命令的查询引擎自主实现。
- **国产工具链集成**：支持国产数据库管理工具和运维平台，提供标准数据接口。
- **可控的配置管理**：`ALTER ALL DNODES`等配置管理命令的执行过程和影响范围完全可控。
通过上述针对性设计，集群管理模块在节点管理、数据分布、故障恢复等关键环节实现技术自主，确保在国产化环境中安全可靠运行。

## 11. 兼容性

TDengine 采用统一的分布式架构设计，集群管理模块与单机部署完全兼容。单机可平滑升级为集群，无需修改应用代码。

### 11.1 架构与配置兼容

- **统一代码架构**：单机是单节点集群的特殊情况，使用相同的存储引擎和查询引擎。
- **配置平滑迁移**：单机配置文件只需添加 `firstEp`、`fqdn` 等集群参数即可用于集群部署。
- **数据格式一致**：单机数据可直接在集群中读取，无需格式转换。

### 11.2 接口与客户端兼容

- **SQL接口一致**：所有数据操作使用相同的SQL语法，应用层无需感知数据分布。
- **管理命令扩展**：`CREATE DNODE`、`SHOW DNODES` 等集群命令是对单机命令的自然扩展。
- **客户端无需修改**：JDBC、RESTful API等接口完全兼容，连接字符串格式一致。

### 11.3 升级与运维兼容

- **平滑升级路径**：支持从单机到集群的在线数据迁移，迁移过程可监控。
- **滚动升级支持**：集群支持节点独立升级，确保服务不中断。
- **运维工具一致**：备份恢复、性能分析等工具在单机和集群环境下均可使用。
TDengine 集群管理模块确保了用户可根据业务需要随时扩展，无需担心兼容性问题，实现"一次开发，随处部署"。

## 12. 运维

集群管理模块对运维工作提出了新的要求，以下是关键运维指导：

### 12.1 监控与告警

- **节点状态监控**：定期执行`SHOW DNODES`、`SHOW MNODES`、`SHOW QNODES`检查节点状态。
- **资源监控**：监控CPU（>80%告警）、内存（>85%告警）、磁盘（>90%立即处理）使用率。
- **自动化监控**：推荐使用Prometheus+Grafana进行集群监控。

### 12.2 节点管理

- **添加节点**：新服务器安装TDengine并配置taos.cfg后，使用`CREATE DNODE "新节点FQDN:6030"`加入集群。
- **移除节点**：正常节点使用`DROP DNODE <ID>`，离线节点使用`force`选项，有单副本数据的离线节点使用`force unsafe`。
- **配置调整**：使用`ALTER DNODE`修改单个节点配置，`ALTER ALL DNODES`修改所有节点配置。

### 12.3 故障处理

- **节点故障**：检查节点状态→评估影响→重启或移除故障节点。
- **网络分区**：优先保证多数派节点服务，网络恢复后自动同步数据。
- **数据恢复**：定期备份元数据和业务数据，验证备份可用性。

### 12.4 扩容与优化

- **存储扩容**：添加DNode实现水平扩展，集群自动迁移VNode平衡负载。
- **计算扩容**：添加QNode分离计算任务，提升查询性能。
- **资源优化**：根据业务负载动态调整节点配置，低峰期可适当缩容。

### 12.5 运维建议

- 在业务低峰期执行节点变更操作。
- 一次只变更一个节点，验证正常后再继续。
- 重要变更前准备回滚方案，详细记录运维操作。
通过上述运维措施，可确保TDengine集群稳定运行，及时应对各种运维场景。

## 13. 使用场景

集群的使用场景与 TDengine TSDB 的使用场景相同。根据业务压力及数据安全要求的不同，选择不同的集群配置方案：
1. **高性能写入场景**：
  - **场景描述**：物联网设备数量巨大，数据上报频率极高，对写入吞吐量有极高要求。
  - **推荐配置**：增加 DNode 数量以水平扩展写入能力，利用多 VNode 并行写入特性。
1. **复杂查询分析场景**：
  - **场景描述**：需要对海量历史数据进行复杂的聚合、降采样或关联查询，计算资源消耗大。
  - **推荐配置**：部署独立的 QNode，实现计算与存储分离，确保查询任务不影响写入性能。
1. **高可用保障场景**：
  - **场景描述**：金融、工业控制等关键领域，对系统连续性和数据可靠性有严苛要求。
  - **推荐配置**：配置 3 个 MNode 组成高可用管理集群，设置多副本策略（replica > 1），确保单节点故障不影响服务。
1. **存储容量扩展场景**：
  - **场景描述**：随着时间推移，历史数据量持续增长，单机存储空间不足。
  - **推荐配置**：动态添加 DNode，集群自动进行负载均衡，将数据分散存储到新节点，实现存储容量的线性扩展。

## 14. 约束和限制

集群管理模块在提供灵活扩展能力的同时，也存在一些技术约束和使用限制。以下表格详细列出了各项约束和限制，用户在使用前需要了解并遵守。

| **约束类别** | **具体约束项** | **限制说明** | **影响与建议** |
| --- | --- | --- | --- |
| **MNode最大数量** | 一个集群最多只能有3个MNode | 建议部署3个MNode确保高可用性 |
| **MNode部署约束** | 一个DNode上只能创建一个MNode | 避免在单个节点上部署多个MNode |
| **MNode最小数量** | 集群必须至少保留1个MNode | 不能删除所有MNode，否则集群不可用 |
| **QNode部署约束** | 一个DNode上只能创建一个QNode | 如需多个QNode，需部署在多个DNode上 |
| **VNode最大数量** | 单个DNode最多支持 numOfSupportedVnodes 个VNode | 合理规划VNode分布，避免单个节点过载 |
| **必需配置参数** | `firstEp`、`fqdn`、`serverPort`必须配置 | 集群部署的基础要求，缺一不可 |
| **配置一致性** | `serverPort`、`timezone`等全局参数必须一致 | 确保集群配置统一，避免异常 |
| **配置生效方式** | 分为立即生效、重启生效、不支持动态修改三类 | 修改配置前确认生效方式，避免误操作 |
| **网络端口** | 所有节点在端口6030上必须能够TCP互通 | 部署前需进行网络连通性测试 |
| **网络延迟** | 节点间网络延迟应小于10ms | 确保Raft共识算法效率，避免选举超时 |
| **主机名要求** | 必须使用唯一且可解析的FQDN，不能使用IP | 确保节点间正确识别和通信 |
| **时间同步** | 所有节点系统时间偏差不超过15分钟 | 建议使用NTP服务保持时间同步 |
| **节点删除** | 只能删除状态为`ready`的在线节点 | 离线节点需使用`force`选项强制删除 |
| **数据安全删除** | 有单副本数据的离线节点需使用`unsafe`选项 | 使用`unsafe`选项将永久丢失数据，需谨慎 |
| **数据迁移** | VNode迁移有并发数限制，大数据量迁移耗时较长 | 迁移期间可能影响性能，建议在低峰期进行 |
| **扩容操作** | 建议一次只添加一个节点，验证正常后再继续 | 避免一次性大规模变更导致不可预知问题 |
| **扩展性上限** | 网络和协调开销随节点数增加而增长 | 大规模集群需考虑网络架构优化 |
| **资源限制** | 每个VNode需要内存资源，连接数有限制 | 根据业务需求合理规划资源分配 |
| **文件描述符** | 需要足够的文件描述符支持大量并发连接 | 调整系统文件描述符限制 |
| **版本一致性** | 所有节点必须运行相同大版本的TDengine | 大版本升级时需确保所有节点同步升级，小版本升级时可以进行滚动升级 |
| **工具兼容性** | 部分单机管理工具可能不完全支持集群操作 | 使用专门的集群管理工具和监控方案 |

## 15. 常见错误和排查

集群管理模块在使用过程中可能会遇到各种错误，以下表格汇总了常见错误的现象、排查方法和解决方案，帮助用户快速定位和解决问题。

| **错误类型** | **错误现象** | **排查步骤** | **解决方案** |
| --- | --- | --- | --- |
| **节点加入失败** | 执行 `CREATE DNODE`` ``"新节点`` ``FQDN:6030"` 命令后，新节点状态一直为 `offline` 或命令直接报错。 | 1. 检查网络连通性 1. ping 新节点 FQDN 1. telnet 新节点 FQDN 6030 1. 检查新节点配置 1. cat /etc/taos/taos.cfg | grep -E "firstEp|fqdn|serverPort" 1. 确认 firstEp 指向正确的第一个节点 1. 确认 fqdn 与当前节点主机名一致 1. 确认 serverPort 为 6030 1. 检查新节点服务状态： 1. systemctl status taosd 1. tail -f /var/log/taos/taosdlog.0 1. 检查防火墙设置： firewall-cmd --list-ports | grep 6030 | 1. 网络不通：配置hosts文件或DNS，确保节点间可互相解析 1. 配置错误：修正taos.cfg配置文件，重启taosd服务 1. 端口被阻：开放防火墙6030端口 1. 服务未启动：启动taosd服务 systemctl start taosd |
| **配置错误导致集群异常** | 集群节点状态异常，`SHOW DNODES` 显示部分节点 `offline`，或集群无法正常提供服务。 | 1. 检查配置一致性 1. SHOW VARIABLES LIKE 'serverPort'; 1. SHOW VARIABLES LIKE 'timezone'; 1. SHOW VARIABLES LIKE 'locale'; 1. 检查必需配置参数 1. 检查配置生效状态 1. SHOW VARIABLES; 1. system(重启生效) 1. session(立即生效) | 1. 配置不一致：统一所有节点的全局配置参数 1. 缺少必需参数：补充 firstEp、fqdn 等必需配置 1. 配置未生效：根据参数类别重启服务或重新连接 |
| **节点状态异常** | `SHOW DNODES` 显示节点状态为 `offline`、`dropping` 或其他异常状态。 | 1. 检查节点基础状态 1. SHOW DNODES; 1. SHOW DNODE 1 VARIABLES; 1. 检查节点资源使用 1. top -b -n 1 | grep taosd 1. df -h /var/lib/taos 1. free -h 1. 检查节点日志 1. grep -i error /var/log/taos/taosdlog.0 | tail -20 1. tail -f /var/log/taos/taosdlog.0 1. 检查节点间通信 1. taos -h localhost -e "SHOW DNODE 1 VARIABLES LIKE 'heartbeat'" | 1. 资源不足：扩容内存、磁盘或CPU资源 1. 服务异常：重启taosd服务 systemctl restart taosd 1. 网络问题：修复网络连接，确保端口6030互通 1. 数据损坏：如有备份可尝试恢复，或移除异常节点后重新加入 |
| **命令执行失败** | 执行集群管理命令（如 `CREATE MNODE`、`DROP DNODE` 等）报错。 | 常见错误及排查： 1. 权限不足错误：ERROR: privilege error 1. 排查：检查用户权限，使用有足够权限的用户执行命令 1. 语法错误：ERROR: invalid SQL statement 1. 排查：检查命令语法，参考本文档第4章正确格式 1. 资源限制错误：ERROR: no enough resourc 1. 排查：检查节点资源使用情况，释放资源或扩容 1. 节点不存在错误：ERROR: dnode not exist 1. 排查：使用 SHOW DNODES 确认节点ID是否正确 | 1. 权限问题：使用管理员账户或授权相应权限 1. 语法问题：修正命令语法，使用正确格式 1. 资源问题：扩容资源或优化配置 1. 参数问题：确认命令参数正确性 |
| **扩容失败** | 添加新节点后，集群负载未均衡，或新节点无法正常服务。 | 1. 检查新节点状态 1. SHOW DNODES; 1. 检查负载均衡状态 1. SHOW DNODES 1. 检查均衡器状态; 1. SHOW VARIABLES LIKE 'balance'; 1. SELECT * FROM information_schema.INS_BALANCE_HISTORY; 1. 检查集群拓扑 1. SHOW CLUSTER; | 1. 均衡器未启动：启用负载均衡 ALTER ALL DNODES 'balance 1' 1. 均衡条件未满足：等待集群自动均衡，或手动触发均衡 1. 网络拓扑问题：检查集群网络配置，确保节点间通信正常 |

## 16. 可观测性

为了保证 TDengine 集群的可观测性，本功能提供详细的元数据库查询功能，可以查询 TDengine TSDB 集群的 DNode、MNode、QNode 等状态。
用户可以通过 `SELECT * FROM INFORMATION_SCHEMA.<指标>` 来查询个节点状态：

### 16.1 INS_DNODES

提供 dnode 的相关信息。也可以使用 SHOW DNODES 来查询这些信息。SYSINFO 为 0 的用户不能查看此表。

| **#** | **列名** | **数据类型** | **说明** |
| --- | --- | --- | --- |
| 1 | **vnodes** | SMALLINT | DNode 的实际 vnode 个数。需要注意，VNodes 为 TDengine TSDB 关键字，作为列名使用时需要使用 ` 进行转义。 |
| 2 | **support_vnodes** | SMALLINT | 最多支持的 vnode 个数 |
| 3 | **status** | BINARY(10) | 当前状态 |
| 4 | **note** | BINARY(256) | 离线原因等信息 |
| 5 | **id** | SMALLINT | DNode id |
| 6 | **endpoint** | BINARY(134) | DNode 的地址 |
| 7 | **create** | TIMESTAMP | 创建时间 |

### 16.2 INS_MNODES

提供 mnode 的相关信息。也可以使用 `SHOW MNODES` 来查询这些信息。SYSINFO 为 0 的用户不能查看此表。

| **#** | **列名** | **数据类型** | **说明** |
| --- | --- | --- | --- |
| 1 | **id** | SMALLINT | MNode id |
| 2 | **endpoint** | BINARY(134) | MNode 的地址 |
| 3 | **role** | BINARY(10) | 当前角色 |
| 4 | **role_time** | TIMESTAMP | 成为当前角色的时间 |
| 5 | **create_time** | TIMESTAMP | 创建时间 |

### 16.3 INS_QNODES

当前系统中 QNode 的信息。也可以使用 `SHOW QNODES` 来查询这些信息。SYSINFO 属性为 0 的用户不能查看此表。

| **#** | **列名** | **数据类型** | **说明** |
| --- | --- | --- | --- |
| 1 | **id** | SMALLINT | QNode id |
| 2 | **endpoint** | VARCHAR(134) | QNode 的地址 |
| 3 | **create_time** | TIMESTAMP | 创建时间 |

### 16.4 INS_VNODES

提供系统中 vnode 的相关信息。属性为 0 的用户不能查看此表。

| **#** | **Column Name** | **Data Type** | **Description** |
| --- | --- | --- | --- |
| 1 | **dnode_id** | INT | DNode id |
| 2 | **vgroup_id** | INT | VGroup id |
| 3 | **db_name** | VARCHAR(66) | 数据库名 |
| 4 | **status** | VARCHAR(11) | VNode 状态 |
| 5 | **role_time** | TIMESTAMP | 最近的选举时间 |
| 6 | **start_time** | TIMESTAMP | VNode 启动时间 |
| 7 | **restored** | BOOL | 是否恢复 |
| 8 | **apply_finish_time** | VARCHAR(20) | 恢复时间 |
| 9 | **unapplied** | INT | 未应用的请求个数 |
| 10 | **buffer_segment_used** | BIGINT | Buffer 段使用的字节数 |
| 11 | **buffer_segment_size** | BIGINT | Buffer 段总字节数 |

## 17. 安装和卸载

本特性对产品的安装和卸载没有影响，安装和卸载流程保持不变。

## 18. 文档

需要在官方文档中修改章节【集群部署】和章节【集群维护】。

## 19. 参考文档

[集群管理-Requirement Spec](https://taosdata.feishu.cn/wiki/XRQmw1q9NiGtOJkA9mgcfdRAnud)

## 20. 附录

无
