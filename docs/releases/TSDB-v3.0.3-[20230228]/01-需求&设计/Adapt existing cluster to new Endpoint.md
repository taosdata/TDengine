# Adapt existing cluster to new Endpoint

在 2.0 版本， TDengine 就支持 endpoint 的修改，但这属于内部功能，没有把使用方法公开给最终用户，仅公司内部客户支持人员了解。在 3.0，这个功能也 应该属于内部功能，不对开源用户发布。
对于已经部署的 TDengine 集群，在每个 dnode 的数据文件夹中，都使用 dnode.json 文件记录集群中所有 endpoint 的列表，默认位置在  /var/lib/taos/dnode/dnode.json，内容如下
```json
{
        "dnodeId":      1,
        "dnodeVer":     "8",
        "clusterId":    "977832821669508240",
        "dropped":      0,
        "dnodes":       [{
                        "id":   1,
                        "fqdn": "localhost",
                        "port": 7100,
                        "isMnode":      1
                }, {
                        "id":   2,
                        "fqdn": "localhost",
                        "port": 7200,
                        "isMnode":      0
                }]
}
```

当某个或者某几个 dnode 的 endpoint 发生变化时，由于此时网络已经不通畅，不能通过 taos shell 修改，但可以增加一个额外的 ep.json 文件完成变更，所在位置为 /var/lib/taos/dnode/ep.json。其中的 id 是 dnode 的 ID，fqdn 和 port 字段是更新前的旧信息，new_fqdn 和 new_port 是更新后的新信息。要求所有 dnode 的 ep.json 文件内容相同，新的 endpoint 不能与 dnode.json 中任何一个旧 endpoint 重复，内容如下
```json
{
        "dnodes":       [{
                        "id":   1,
                        "fqdn": "localhost",
                        "port": 7100,
                        "new_fqdn": "127.0.0.1",
                        "new_port":7300
                }, {
                        "id":   2,
                        "fqdn": "localhost",
                        "port": 7200,
                        "new_fqdn": "127.0.0.1",
                        "new_port":7400
                }]
}
```

对于 endpoint 发生变化的 dnode，其自身配置文件中的 fqdn 和 serverPort 配置项需要进行同样修改。如果集群 firstEp 和 secondEp 的 endpoint 发生变化，则集群所有 dnode 的配置文件、客户端的配置文件，对应的 firstEp 和 secondEp 配置项也要进行重新配置。
在 taosd 重启结束后，ep.json 文件会被修改为 ep.json.bak，dnode.json 文件内容也会被更新，可以进行如下检查，确认 endpoint 修改完成。
1. 检查 ep.json 文件是否被重命名为 ep.json.bak；
2. 检查 dnode.json 文件；
3. 如果节点中有 mnode，检查 mnode/sync/raft_config.json 文件中的 endpoint 信息；
4. 如果节点中有 vnode，检查 vnode/vnode<Id>/vnode.json 和 vnode/vnode<Id>/sync/raft_config.json 文件中的 endpoint 信息；
5. 检查 show dnodes 的执行结果；
6. 检查 show mnodes 的执行结果 ；
7. 如果重启前，有未完成的事务，例如创建 DB，可以查看这个事务是否正常结束
8. 检查数据订阅是否可以正常工作；
9. 如果存在流计算，检查结果是否持续输出；
10. 检查原有的数据库是否可读写；
11. 检查是否可以创建新的数据库表。
