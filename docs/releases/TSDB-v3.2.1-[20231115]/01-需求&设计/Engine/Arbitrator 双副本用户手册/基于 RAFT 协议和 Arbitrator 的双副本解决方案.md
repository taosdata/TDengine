# 基于 RAFT 协议和 Arbitrator 的双副本解决方案

## 1. ~~基础方案（无 Arbitrator）~~

1. 允许直接创建双副本数据库 create database replica 2
2. 允许从单副本数据库修改为双副本 alter database replica 2 ，但禁止从三副本数据库修改为 2
3. 在 replica =2 时，仍然遵从 RAFT 的强一致性协议，即 follower 必须投票确认后写入才视为完成。这样 Leader 和 Follower 之间永远保持了数据一致性。
4. 正常场景：Leader 和 Follower 都正常运行
5. 异常场景：任意 dnode 宕机，则所涉及的 vgroup 都将拒绝服务，因为无法获得多数共识
6. 优点：简单，可以快速实现；有基本的容错能力，即容许一个副本的磁盘坏掉。
7. 缺点：没有高可用能力，任意节点宕机系统都会停止服务

## 2. 增强方案（有 Arbitrator）

1. 与基础方案的前四点相同
2. 协议变化
   - 在 RAFT 协议中增加 Assigned_Leader 角色，在 Assigned_Leader 时不需要多数共识，即可以单副本工作
   - Arbitrator 节点不需要从 Leader 复制数据，只承担正常 dnode 宕机时的仲裁者角色
   - 从 Leader 到 Assigned_Leader：当两个 dnode 中的一个宕机时，Arbitrator 将每个 vgroup 中活着的 vnode 强制指定为 Assigned_Leader，因为 leader 和 follower 的强一致性，这个操作不会造成数据丢失
   - 从 Assigned_Leader 到 Leader: 当宕机的 dnode 恢复后，其上所有 vnode 的初始状态都是 candidate，它们会在各自所在的 vgroup 中发起选主，Assigned_Leader 此时会选主成功变成 Leader，系统恢复节点宕机前的正常运行状态
   - mnode 与 vnode 的双副本机制相同
3. 注意事项：在 Assigned_Leader 工作时，为了避免出现后面 Follower 恢复后追不上的情况，此时需要对系统进行写入限流，但读写都能够正常工作
4. 优点
   - 真正的低负载 Arbitrator，它只需要实现和所有 vnode 之间的心跳即可，不需要接收数据，不需要保存各个节点的 index，不需要任何持久化
   - 能够实现单节点宕机的高可用并保证了数据一致性
5. 缺点：
   - 需要单台服务器部署 Arbitrator
   - 相对于三副本来说，无法解决连续宕机的情况，即 dnode 2 宕机又恢复后，在数据追齐之前 dnode 1 宕机
