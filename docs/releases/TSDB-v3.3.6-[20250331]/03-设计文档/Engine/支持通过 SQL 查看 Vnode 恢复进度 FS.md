# 支持通过 SQL 查看 Vnode 恢复进度 FS

## 1. 背景

当宕机节点重启时，客户想要知道数据恢复的进度，例如：
1. 节点发生宕机，消除故障后重启，需要通过数据恢复的进度，才能知道何时恢复“正常”。
2. 通过滚动停机进行版本升级、虚拟机扩容等操作，也需要数据恢复进度作为参考。
当前，show vnodes 显示的 restored 字段等于 true 时，只代表了 vnode 状态正常，并不代表该 vnode 所保存的数据（包括内存+硬盘）已经追上其他节点上 vnode 的进度。
所以希望研发可以增加以下指标：
1. 增加显示 vnode 数据是否恢复正常的指标；
2. 增加显示 follower vnode 落后 leader vnode 程度的指标；
以上指标可以放到 show vgroup、show vnode 中，并加入监控。
**JIRA:**** **[**TS-5584**](https://jira.taosdata.com:18080/browse/TS-5584)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/2/11 | 0.1 | 鲍之骁 | 初稿 |

## 3. 定义

## 4. 行为说明

### 4.1 显示 vnode 数据是否恢复正常的指标

1. **SQL 示例**
  ```sql
  show vnodes;
  ```

1. **结果展示**
  ```sql
  show vnodes;                                                                                                                                                                                                                                                            
    dnode_id   |  vgroup_id  |            db_name             |   status    |        role_time        |       start_time        | restored |   apply_finish_time  |  unapplied  |                                                                                               
  ===============================================================================================================================================================================                                                                                               
             1 |           2 | test                           | follower    | 2025-02-12 10:27:10.663 | 2025-02-12 10:26:42.498 | true     |                      |           0 |                                                                                               
             2 |           2 | test                           | leader      | 2025-02-12 10:27:10.670 | 2025-02-12 10:26:42.491 | true     |                      |           0 |                                                                                               
             3 |           2 | test                           | follower    | 2025-02-12 10:28:42.588 | 2025-02-12 10:28:40.346 | true     | 0:0:12               |         685 |                                                                                               
             1 |           3 | test                           | follower    | 2025-02-12 10:26:46.824 | 2025-02-12 10:26:42.493 | true     |                      |           0 |                                                                                               
             2 |           3 | test                           | leader      | 2025-02-12 10:26:46.831 | 2025-02-12 10:26:42.497 | true     |                      |           0 |                                                                                               
             3 |           3 | test                           | follower    | 2025-02-12 10:28:40.578 | 2025-02-12 10:28:40.351 | true     | 0:0:7                |         777 |        
  ```

在 `show vnodes;`的返回结果中增加以下字段 
1. restored_finish 用于提示用户某一 vnode 完成 restore 可能需要的时间。
2. unapplied 等待被应用到状态机的日志条目数量。
**例如**：21:17:44 代表当前 vnode 还需要 21 小时 17 分钟 44 秒去做 restore。
1. **注意事项**
   - vnode 的 unapplied 为 0 时，apply_finish_time 显示为空。
   - apply_finish_time 并不是一个准确时间，是根据当前数据条目 apply 的速率以及还需要 apply 的数据条目数计算的估计时间。

### 4.2 显示 follower vnode 落后 leader vnode 程度的指标

1. **SQL 示例**
  ```sql
  show vgroups;
  ```

1. **结果展示**
  ```sql
  show vgroups;
    vgroup_id  |            db_name             |   tables    | v1_dnode |  v1_status  |    v1_applied/committed     | v2_dnode |  v2_status  |    v2_applied/committed     | v3_dnode |  v3_status  |    v3_applied/committed     | v4_dnode |  v4_status  |    v4_applied/committed     |  cacheload  | cacheelements | tsma |
  ==============================================================================================================================================================================================================================================================================================================================
             4 | test                           |        5004 |        1 | leader      | 10057/10057                 |        2 | follower    | 10057/10057                 |        3 | follower    | 10057/10057                 | NULL     | NULL        | NULL                        |           0 |             0 |    0 |
             5 | test                           |        4996 |        1 | leader      | 10041/10041                 |        2 | follower    | 10041/10041                 |        3 | follower    | 10041/10041                 | NULL     | NULL        | NULL                        |           0 |             0 |    0 |
  Query OK, 2 row(s) in set (0.008092s)
  ```

在 `show vgroups;` 的返回结果中增加字段  v1_applied/committed (从v1到v4) 用于提示用户，在一个 vgroup 中,各个 vnode 已经应用到状态机的 index （写入TSDB）。
1. **注意事项**
   - v1_applied 表示 vnode 应用到状态机的数据条目索引，基于 TDengine 对 raft 的实现， follower 并不是一定落后 leader ，这取决于 vnode 所在 dnode 的网络延迟，负载均衡以及磁盘性能等硬件因素。
   - 统计方式是从各个 dnode 汇总到 mnode ，存在汇总时间间隔。所以您有可能看到follower  committed 略大于leader。

## 5. 性能

无

## 6. 兼容性

无

## 7. 运维

运维人员以及客户在数据恢复时，通过 show vnodes/show vgroups 命令了解数据恢复的进度。

## 8. 使用场景

运维人员以及客户在数据恢复时，需要了解数据恢复的进度。

## 9. 约束和限制

无

## 10. 常见错误和排查

无

## 11. 可观测性

需要在监控界面体现数据恢复进度。

## 12. 安装和卸载

本功能不单独发布，随 TDengine 产品安装包一起发布。

## 13. 文档

## 14. 参考文档

## 15. 附录

[详解 follower 的apply index 为何可能落后 leader？](https://taosdata.feishu.cn/docx/U5PIdLLx5o8UjnxLIGTcf72vnre)
