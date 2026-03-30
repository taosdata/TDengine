# 审计信息不经过 taoskeeper 记录 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-3-4 | 2026-3-6 | 1.0 | 陈东明 | 初始化 |

## 2. 背景

出于安全考虑，要求审计日志不经过 taoskeeper 路由，直接记录。

## 3. 定义

无

## 4. 行为说明

### 4.1 AuditSaveInSelf 开关

增加设置项AuditSaveInSelf。将AuditSaveInSelf设置为1，审计信息将不再发送给taoskeeper，而会直接发给本集群的vnode。所以在AuditSaveInSelf为1的情况下，monitorFQDN和monitorPort将不再起作用。类似，monitorComp， auditHttps， auditUseToken 这3个参数同样不再起作用。将审计信息发给vnode的通讯机制，使用的是集群内部的RPC机制，所以通讯的安全机制（比如是否使用加密等）采用RPC的整体设置，没有单独设置。
同时，在auditSaveInSelf为1的情况下，不再支持将审计记录保存到其他集群的功能。
auditSaveInSelf默认值为0。

### 4.2 创建审计库

审计功能需要提前创建审计库，使用如下SQL，
```plaintext
CREATE DATABASE [IF NOT EXISTS] db_name [database_options] IS_AUDIT 1 VGROUPS 1;
```

vgroups数量限制为1，目前这是个技术实现的限制，无法将审计库的vgroups数量增加。但是这个行为仍然保持与使用taoskeeper时的行为一致。taoskeeper也是只创建了vgroups为1的审计库。
另外，为了保持与taoskeeper的行为一致，在创建审计库的同时，在创建审计库的事务中，会同时创建operations超级表。在使用taoskeeper时，operations是taoskeeper自动创建的。创建出来的operatiions表，与执行下面的SQL保持一致。
```sql
create stable if not exists operations 
    (ts timestamp, user_name varchar(25), operation varchar(20), db varchar(65), 
    resource varchar(193), client_address varchar(64), details varchar(50000), 
    affected_rows bigint unsigned, `duration` double) 
    tags (cluster_id varchar(64))
```

## 5. 性能

不使用taoskeeper记录审计信息的性能与使用taoskeeper记录审计信息的性能没有差异。

## 6. 安全

审计信息不再经过外部的taoskeeper，不再存在模拟taoskeeper截获审计信息的可能。
当taoskeeper宕机时，审计记录不会出现丢失。

## 7. 兼容性

新功能兼容由taoskeeper创建的审计表，也即带有is_audit标志的审计库，并且taoskeeper已经创建了operations表，可以直接升级到新版本，无需删除、重建审计库。

## 8. 运维

无。

## 9. 使用场景

无。

## 10. 约束和限制

约束：审计库的vgroup数量只能为1。
限制：无

## 11. 常见错误和排查

| Audit database is not allowed to keep multiple vgroups | 创建库时vgroups数量不能大于1 |
| --- | --- |

## 12. 可观测性

无

## 13. 安装和卸载

无

## 14. 文档

增加设置项AuditSaveInSelf的说明

## 15. 参考文档

无

## 16. 附录

无。
