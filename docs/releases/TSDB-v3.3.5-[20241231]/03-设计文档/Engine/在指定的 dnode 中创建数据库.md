# 在指定的 dnode 中创建数据库

## 1. 背景

玉溪项目构建了 18 个节点的集群，该集群约有 6 个数据库，其中的某个数据库有 12 个 3 副本的 vgroup。当该数据库的查询压力很大时，会导致其他数据库读写变得缓慢。过往的技术讨论中，考虑采用大集群专区使用的策略，回避资源限制的问题。因此，准备支持类似 create database db1 dnodes '1,2,3' 这样的语法，让 db1 的所有 vnode 都分布在给定的 1/2/3 号 dnode 中。对于已经存在的 db，交付团队可通过 redistribute 语法移动 vnode 到目标 dnode 上。
JIRA：[TS-5575](https://jira.taosdata.com:18080/browse/TS-5575)

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024-10-31 | 1.0 | 关胜亮 | 新建 |

## 3. 定义

无

## 4. 行为说明

### 4.1 SQL 语法

```sql

CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 
database_options:
    database_option ...
 
database_option: {
    VGROUPS <vgroups_num>
  | ……
  | dnodes <dnode_list>
}
```

**参数说明：**增加 dnodes 关键字，输入 dnode_list。dnode_list 格式为逗号分割的字符串。
**行为说明：**
1. 当用户输入 dnode_list 时，新建数据库的所有 vnode 都分布在这些 dnode 中。
2. 如果 dnode 数目不足，或者 dnode 的资源不足（support_vnodes、内存余量）时，创建数据库失败。

### 4.2 使用示例

```sql
-- 单副本
create database d10 vgroups 1 dnodes '1';
create database d11 vgroups 1 dnodes '2';
create database d12 vgroups 2 dnodes '3,4';
create database d15 vgroups 2 dnodes '1,4,3';

-- 两副本
create database d20 replica 2 vgroups 1 dnodes '1,2';
create database d21 replica 2 vgroups 3 dnodes '1,2,3';

-- 三副本
create database d30 replica 3 vgroups 3 dnodes '1,2,3';
create database d33 replica 3 vgroups 5 dnodes '4,2,3,1,5';
```

## 5. 性能

无

## 6. 兼容性

无兼容性问题
1. 在创建数据库消息（SCreateDbReq）中增加一个字段 dnodeListStr，解析消息时判断消息结尾是否有数据
2. 客户端不传入 dnodeList 时，创建数据库仍然采用原有行为
3. 增加的 dnodes 关键字在老版本中已经存在

## 7. 运维

1. tsma：不一定涉及 vnode 创建，行为不变
2. stream：不一定涉及 vnode 创建，行为不变
3. balance vgroup 命令：行为不变
4. alter database：副本修改的情况更为复杂，检查项目更多，暂时没有相关需求，行为不变
5. drop dnode：交付团队可事先通过 redistribute 移动 vnode，行为不变 

## 8. 使用场景

大集群分区使用的场景，交付团队在创建库时规划 vnode  分布

## 9. 约束和限制

仅企业版提供

## 10. 常见错误和排查

| 报错信息 | 含义 |
| --- | --- |
| Duplicate items in the dnode list | dnode_list 须严格按照 “1,2,3” 的写法，不允许空格，不允许数字和逗号外的其他字符，最后一个字符不能是空格 |
| Invalid dnode list format | dnode_list 中不能用重复的 dnode |
| Dnode does not exist | dnode_list 中有不存在的 dnode |
| Out of dnodes | dnode_list 不足以创建 database |

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

需更新用户手册，在 PR 中体现

## 14. 参考文档

无

## 15. 附录

无
