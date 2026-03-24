# TDengine 多级存储配置项支持禁止创建新文件组

## 1. 背景

- 目前的多级存储策略，在落盘新生成文件组时，通过轮询策略选取同一层级的挂载点。如果用户新增多级存储挂载点，该策略会造成新增挂载点剩余空间较大。
- 因此，在多级存储配置中，新增一个配置项，让用户可以控制某个挂载点是否禁止创建新文件组。这样，用户可以更加灵活的控制存储策略，方便运维。
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
提示： 1）优先选取剩余空间较大的策略，会遇到大量新落盘文件组集中分配至某一挂载点的问题，导致该挂载点很快被写满。
</callout>

- JIRA： [TD-30554](https://jira.taosdata.com:18080/browse/TD-30554)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/6/13 | 0.1 | Cary Xu | 初稿 |
| 2024/6/14 | 0.2 | Cary Xu | 将新增配置项由 enable 改为 disable_create_new_file，便于理解。 |
| 2024/6/17 | 0.3 | Cary Xu | 会议事项说明，参照 4.2 和 评论 |
|  |  |  |  |

## 3. 定义

- 挂载点：在 taos.cfg 中配置的 dataDir 数据存储目录。

## 4. 行为说明

### 4.1 新增 `禁止创建新文件组`配置项 disable_create_new_file

- 在 dataDir（在配置文件 /etc/taos/taos.cfg 中）后边，新增可选项 <disable_create_new_file>  表示当前挂载点`是否禁止创建 tsdb 新文件组`。0 表示 `不禁止创建新 tsdb 文件组(默认值)`, 1 表示`禁止创建新 tsdb 文件组`。
```plaintext
dataDir [path] <level> <primary> <disable_create_new_file>
```

- path: 挂载点的文件夹路径
- level: 介质存储等级，取值为 0，1，2。 0 级存储最新的数据，1 级存储次新的数据，2 级存储最老的数据，省略默认为 0。 各级存储之间的数据流向：0 级存储 -> 1 级存储 -> 2 级存储。 同一存储等级可挂载多个硬盘，同一存储等级上的数据文件分布在该存储等级的所有硬盘上。 
- primary: 是否为主挂载点，0（否）或 1（是），省略默认为 1。
- disable_create_new_file: 是否禁止创建新文件组，0（不禁止）或 1（禁止），省略默认为 0。取值为 0 时，允许从该挂载点新建文件组；取值为 1 时，落盘时不会从该挂载点新建文件组，但是，已经生成的文件组，仍然会向该挂载点写入数据。
- 在配置中，只允许一个主挂载点存在（level=0，primary=1），例如采用如下配置方式：
```plaintext
dataDir /mnt/data1 0 1     // 主挂载点(Primary Disk) 默认不禁止创建新文件组
dataDir /mnt/data2 0 0 0   // 挂载点不禁止创建新文件组 
dataDir /mnt/data3 1 0 0   // 挂载点不禁止创建新文件组 
dataDir /mnt/data4 1 0     // 挂载点默认不禁止创建新文件组 
dataDir /mnt/data5 2 0 1   // 挂载点禁止创建新文件组 
dataDir /mnt/data6 2 0     // 挂载点默认不禁止创建新文件组 
```

<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
1. 多级存储不允许跨级配置，合法的配置方案有：仅 0 级，仅 0 级+ 1 级，以及 0 级+ 1 级+ 2 级。而不允许只配置 level=0 和 level=2，而不配置 level=1。
2. 禁止手动移除使用中的挂载盘，挂载盘目前不支持非本地的网络盘。
3. 多级存储目前不支持删除已经挂载的硬盘的功能。
4. 不支持动态配置，修改配置后重启 taosd 生效。
5. 0 级至少存在 1 个挂载点 disable_create_new_file 为 0，1/2 级没有该限制。
</callout>

### 4.2 disable_create_new_file 对 taosd 相关操作的影响

- 所有涉及在 vnode 生成新文件组的操作，均会受影响，即如果挂载点的 disable_create_new_file 为 1，则不会在该挂载点生成新文件组。具体如下：

| Operations | Affects | Comments |
| --- | --- | --- |
| 写入数据 | 落盘时，针对 disable_create_new_file 为 1 的挂载点，不生成新文件组(已生成的文件组落盘不受影响) |  |
| FLUSH DATABASE [dbName] | 落盘时，针对 disable_create_new_file 为 1 的挂载点，不生成新文件组(已生成的文件组落盘不受影响) |  |
| COMPACT DATABASE [dbName] <start> <end> | compact 时，无论新生成文件组的存储层级是否发生变化，新文件组的位置均会在 disable_create_new_file 为 0 的挂载点。 |  |
| TRIM DATABASE [dbName] | 文件组不会被迁移至 disable_create_new_file 为 1 的挂载点。 |  |
| ALTER DATABASE [dbName] REPLICA [replicaValue] | 如果副本数由大变小，只会删除文件，不会生成新文件组，因此不受影响；如果副本数由小变大，则可能涉及生成新文件组，针对 disable_create_new_file 为 1 的挂载点，不会生成新文件组。 |  |
| BALANCE VGROUP | 在 vgroup 平衡的过程中，针对 disable_create_new_file 为 1 的挂载点，不会生成新文件组。 |  |
| MERGE VGROUP(暂不支持) | N/A |  |
| REDISTRIBUTE VGROUP [vgId] DNODE [dnodeId] | 在 vgroup 迁移的过程中，针对 disable_create_new_file 为 1 的挂载点，不会生成新文件组。 |  |
| SPLIT VGROUP [vgId] | 在 vgroup 拆分的过程中，针对 disable_create_new_file 为 1 的挂载点，不会生成新文件组。 |  |
| RESTORE DNODE [dnodeId] | 在 dnode 恢复过程中，其所涉及的所有 vnode，针对 disable_create_new_file 为 1 的挂载点，不会生成新文件组。 |  |
| RESTORE QNODE/MNODE/VNODE on DNODE [dnodeId] BNODE/SNODE(暂不支持) | 在 qnode/mnode 恢复过程中，不涉及在 vnode 生成新文件组，因此，不受影响；在 vnode 恢复过程中，针对 disable_create_new_file 为 1 的挂载点，不会生成新文件组。 |  |
| ... |  |  |

## 5. 性能

- 修改前后，如果允许创建新文件组的挂载点数量和挂载点介质类型发生变化，有可能造成落盘时写文件和查询时读文件的速度发生变化，从而对写入和查询性能产生影响。

## 6. 兼容性

- 支持升级。功能不受影响。
- 支持降级。但是，无论降级前配置项 disable_create_new_file 取 0 还是 1，降级后均为 0。

## 7. 运维

- 升级不受影响，原有的挂载点均默认为`不禁止创建新文件组`；降级后 disable_create_new_file 配置项不起作用，无论取值为 0 或 1，均为 `不禁止创建新文件组`。
- 需要监控已启用及未启用挂载点的存储空间，防止空间不足导致落盘失败。

## 8. 使用场景

- 用户原挂载点存储空间不足，新增挂载点。将原挂载点 disable_create_new_file 设置为 1，新增挂载点 disable_create_new_file 设置为 0（或不设置，默认为 0）。

## 9. 约束和限制

- 无

## 10. 常见错误和排查

- 无

## 11. 可观测性

- 无

## 12. 安装和卸载

- 无特殊要求

## 13. 文档

- 需要修改官网文档 - [多级存储](https://docs.taosdata.com/tdinternal/arch/#%E5%A4%9A%E7%BA%A7%E5%AD%98%E5%82%A8)。
- 需要修改企业版文档。

## 14. 参考文档

- [多级存储](https://docs.taosdata.com/tdinternal/arch/#%E5%A4%9A%E7%BA%A7%E5%AD%98%E5%82%A8)

## 15. 附录

- 无
