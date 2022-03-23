# 准备工作
## 第零步

规划集群所有物理节点的 FQDN，将规划好的 FQDN 分别添加到每个物理节点的/etc/hostname；修改每个物理节点的/etc/hosts，将所有集群物理节点的 IP 与 FQDN 的对应添加好。【如部署了 DNS，请联系网络管理员在 DNS 上做好相关配置】

## 第一步

如果搭建集群的物理节点中，存有之前的测试数据、装过 1.X 的版本，或者装过其他版本的 TDengine，请先将其删除，并清空所有数据（如果需要保留原有数据，请联系涛思交付团队进行旧版本升级、数据迁移），具体步骤请参考博客《TDengine 多种安装包的安装和卸载》。

:::note
因为 FQDN 的信息会写进文件，如果之前没有配置或者更改 FQDN，且启动了 TDengine。请一定在确保数据无用或者备份的前提下，清理一下之前的数据（rm -rf /var/lib/taos/*）；
:::

:::note
客户端也需要配置，确保它可以正确解析每个节点的 FQDN 配置，不管是通过 DNS 服务，还是修改 hosts 文件。
:::


## 第二步

建议关闭所有物理节点的防火墙，至少保证端口：6030 - 6042 的 TCP 和 UDP 端口都是开放的。强烈建议先关闭防火墙，集群搭建完毕之后，再来配置端口；

## 第三步

在所有物理节点安装 TDengine，且版本必须是一致的，但不要启动 taosd。安装时，提示输入是否要加入一个已经存在的 TDengine 集群时，第一个物理节点直接回车创建新集群，后续物理节点则输入该集群任何一个在线的物理节点的 FQDN:端口号(默认 6030)；

## 第四步

检查所有数据节点，以及应用程序所在物理节点的网络设置：

每个物理节点上执行命令hostname -f，查看和确认所有节点的 hostname 是不相同的(应用驱动所在节点无需做此项检查)；

每个物理节点上执行ping host，其中 host 是其他物理节点的 hostname，看能否 ping 通其它物理节点；如果不能 ping 通，需要检查网络设置，或/etc/hosts 文件(Windows 系统默认路径为 C:\Windows\system32\drivers\etc\hosts)，或 DNS 的配置。如果无法 ping 通，是无法组成集群的；

从应用运行的物理节点，ping taosd 运行的数据节点，如果无法 ping 通，应用是无法连接 taosd 的，请检查应用所在物理节点的 DNS 设置或 hosts 文件；

每个数据节点的 End Point 就是输出的 hostname 外加端口号，比如h1.taosdata.com:6030。

## 第五步

修改 TDengine 的配置文件（所有节点的文件/etc/taos/taos.cfg 都需要修改）。假设准备启动的第一个数据节点 End Point 为 h1.taosdata.com:6030，其与集群配置相关参数如下：

```
// firstEp 是每个数据节点首次启动后连接的第一个数据节点
firstEp               h1.taosdata.com:6030

// 必须配置为本数据节点的FQDN，如果本机只有一个hostname, 可注释掉本项
fqdn                  h1.taosdata.com

// 配置本数据节点的端口号，缺省是6030
serverPort            6030

// 副本数为偶数的时候，需要配置，请参考《Arbitrator的使用》的部分
arbitrator            ha.taosdata.com:6042
```
一定要修改的参数是 firstEp 和 fqdn。在每个数据节点，firstEp 需全部配置成一样，但 fqdn 一定要配置成其所在数据节点的值。其他参数可不做任何修改，除非你很清楚为什么要修改。

加入到集群中的数据节点 dnode，涉及集群相关的下表 9 项参数必须完全相同，否则不能成功加入到集群中。

| **#** | **配置参数名称**   | **含义**                                    |
| ----- | ------------------ | ------------------------------------------- |
| 1     | numOfMnodes        | 系统中管理节点个数                          |
| 2     | mnodeEqualVnodeNum | 一个 mnode 等同于 vnode 消耗的个数          |
| 3     | offlineThreshold   | dnode 离线阈值，超过该时间将导致 Dnode 离线 |
| 4     | statusInterval     | dnode 向 mnode 报告状态时长                 |
| 5     | arbitrator         | 系统中裁决器的 End Point                    |
| 6     | timezone           | 时区                                        |
| 7     | balance            | 是否启动负载均衡                            |
| 8     | maxTablesPerVnode  | 每个 vnode 中能够创建的最大表个数           |
| 9     | maxVgroupsPerDb    | 每个 DB 中能够使用的最大 vgroup 个数        |

:::note
在 2.0.19.0 及更早的版本中，除以上 9 项参数外，dnode 加入集群时，还会要求 locale 和 charset 参数的取值也一致。
:::
