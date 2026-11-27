# FQ 客户端版本选择（Linux）

安装包准备客户端依赖，安装完成后使用 `taos-fq-client` 管理选择，无需保留原安装包。

```bash
taos-fq-client oracle show
# 停止使用 Oracle FQ 的 taosd 及其他客户端进程后执行：
sudo taos-fq-client oracle use 19c
# 或切回：
sudo taos-fq-client oracle use 23ai
```

完成后重新启动相关进程。命令显示磁盘上的选择，不代表运行中进程已经切换；同一进程使用一套 Oracle 客户端，不能对不同数据源分别选择。先停止再切换，避免切换过程中启动新进程。

普通安装不增加 Oracle 提问：首装默认 19c，升级保留选择。安装时的显式 Oracle 参数继续保留兼容，日常切换使用独立命令。自定义安装目录可使用 `<安装目录>/bin/taos-fq-client`；也可指定 `--install-dir <目录>`。

切换检查目标客户端必需文件，只替换 `driver/` 内指向 `oracle/19c` 或 `oracle/23ai` 的 Oracle 库链接，实体库保持在对应版本子目录，不再创建安装目录下的 `lib/`。旧布局中 `lib/` 内指向这两个客户端目录的软链会在切换成功后移除，其他文件保留。不改供应商二进制、不创建全局 FQ 库链接，也不修改动态库全局配置。遇到同名普通文件、其他软件的链接、不完整客户端或并发操作时失败并保留原选择。链接更新失败时恢复此前链接。中断后若遗留 `.client-switch.lock`，应先确认没有切换操作运行，再检查链接与选择记录，处理锁目录。

## 按需启用联邦查询

core 不携带 FQ 客户端依赖，需要使用 FQ 时先安装匹配版本的 extension，或使用 full 包。客户端版本选择、QNODE 创建与 FQ 开关是独立操作；安装依赖和自动创建 QNODE 都不会开启 FQ。

`federatedQueryEnable` 保持默认关闭。开启后，`SELECT ... FROM a.b` 需要区分 `a` 是数据库还是外部源，普通查询也可能增加名称解析开销。因此只在确认使用 FQ 的实例和客户端上显式开启。

在使用 FQ 的服务端及客户端配置 `taos.cfg` 中设置：

```text
federatedQueryEnable 1
```

保存后重启对应服务及客户端进程。Docker 把该配置持久化到挂载的配置文件；Apex 使用自己的部署配置提供该值，两者都不会自动改写此默认值。容器内临时修改不能代替持久配置。

已有运行实例也可按需执行：

```sql
ALTER LOCAL 'federatedQueryEnable' '1';
ALTER ALL DNODES 'federatedQueryEnable' '1';
```

`ALTER LOCAL` 仅影响执行语句的当前客户端进程，不会替其他客户端、IDMP 或 taosAdapter 的连接统一开启。需要这些连接使用 FQ 时，也应为对应进程提供配置并重新连接。动态修改后的重启行为应通过持久配置保证。

正常 `start-all.sh` / `start-all.bat`、Docker 和配套 Apex 启动流程会在集群尚无 QNODE 时创建一个；`start-all` 的 `-S` 只启动服务，跳过初始化。直接启动 taosd 的部署应自行检查 QNODE，必要时在选定 DNODE 上创建：

```sql
SHOW DNODES;
SHOW QNODES;
CREATE QNODE ON DNODE <id>;
```

## Docker

通过启动环境变量选择版本；未设置时使用镜像默认 19c：

```bash
docker run -d --name tsdb -e TAOS_ORACLE_CLIENT=19c <镜像>
```

在 Compose 中把 `TAOS_ORACLE_CLIENT` 写入服务的 environment，再重建容器；每次启动都在 taosd 启动前应用。参数无效或目标客户端缺失会使启动失败。镜像内也可用 `taos-fq-client oracle show` 查看选择。不要把运行中容器内的手工改动作为重建后的配置。

切换工具识别镜像中的 `driver/oracle -> ../lib/oracle` 布局，在 `/usr/local/lib/` 更新所选客户端的相对软链；普通 Linux 安装包仍在安装目录的 `driver/` 更新软链。镜像构建与启动使用同一切换工具，`show` 输出中的 `Runtime links` 可查看实际链接目录。

## 扩展约定

命令结构为 `taos-fq-client <数据源> show|use <客户端版本>`。这次只实现 Oracle 的 19c / 23ai；`pg` 等数据源明确返回尚不支持。以后增加数据源时，分别实现该客户端的文件校验、私有加载路径、切换及生效策略，不直接套用 Oracle 的库名和兼容矩阵。
