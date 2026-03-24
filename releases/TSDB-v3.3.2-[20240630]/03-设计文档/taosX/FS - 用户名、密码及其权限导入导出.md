# FS - 用户名、密码及其权限导入导出

## 1. 背景

在多集群场景或运维场景中，备份 TDengine 的用户名、密码和相关权限是一项合理需求。此文档中主要说明该功能在用户可见层面（即： taosx 命令行和 Explorer 管理页）的使用方式，并在附录中说明其内部实现的部分要求。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/05/31 | 0.1 | @霍琳贺 | 初稿，命令行 + Explorer 使用方式 |
| 2024/06/04 | 0.2 | @霍琳贺 | 根据 Reivew 意见，修改命令行使用方式和 UI，并给出运维建议； 附录中更新了 SQL 语句实现和 Explorer API 。 |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

- 用户名：指 TDengine 自身所管理的数据库用户。
- 密码：每个 TDengine 用户对应一个密码。密码可以被修改。
- 权限：每个 TDengine 用户对应一组管理权限，包括但不限于：数据库、表、流、订阅等。

## 4. 行为说明

### 4.1 命令行使用方式

命令行方式支持用户名、密码、权限和白名单信息的在线迁移以及备份和导入。
基本的命令行使用方式如下：
```bash

## 1. From one to another cluster

taosx privileges -f "taos://root:taosdata@localhost" \
  -t "taos://other"

## 2. Export to a single file.

taosx privileges -f "taos+ws://root:taosdata@localhost:6041" \
  -o ./path/to/file

## 3. Import from backup file.

taosx privileges -i ./path/to/file -t "taos://other"
```

连接方式支持原生连接与 websocket.
仅 root 帐号或可导出或导入。
支持额外的选项以选择对什么内容生效：
- `-u`：表示仅对用户名和密码进行导入导出。
- `-p`：表示仅对权限进行导入导出。
`-u` 和 `-p` 同时使用时与默认情况（无 `-u` 和 `-p`）一致。

### 4.2 Explorer 

Explorer 只支持在线迁移。
在菜单栏 - Management -> User 页面下添加 "导入/Import" 按钮。
![](./images/img_XH0xbus8lo5K5IxDHJTc0meOnPb.png)

点击 "Import"，弹出导入对话框如下：
![](./images/img_H7OkbMwAWoVpyPxcu8YclCwpnue.png)

其具体选项包括：
- Server: 从指定集群导入（taosAdapter 访问地址，如 `http://2.168.2.10:6041`）。
- Password：源集群 root 密码。
- Items：导入数据信息类型。
  - User & Password：用户名和密码，（实际包含 sysinfo/super 等用户基本信息）
  - Privileges：权限
  - Hosts Whitelist：白名单
  默认选中 Passwords 和 Privileges。
  当选中白名单时默认勾选 Passwords。

导入完成后，在 “导入” 框下方展示导入结果：
1. 全部导入成功，输出如下：
  ```plaintext
  M 项用户信息、N 项权限信息已导入。
  ```

1. 部分导入成功，输出如下：
  ```plaintext
  M 项用户名和密码、N 项权限信息已导入，部分项导入失败：
  
  - 用户 <username> 导入失败: <reason>
  - 用户 <username> 权限(`xxx`)导入失败: <reason>
  
  请检查导入失败原因。
  ```

  在上述操作的任意阶段，错误提示方式与整体框架保持一致。

## 5. 性能

无。

## 6. 兼容性

导入和导出都仅支持 3.3.1.0 、3.1.1.x (待定)及以上版本（需要 TDengine 支持）。

## 7. 运维

使用此功能进行导入时，如果指定的用户权限超出待导入的权限，不会进行 revoke ，即：用户和权限信息导入时只新增不删除，而不是**同步**。
在命令行下使用时，请参考以下步骤：
1. 确认源端和目标端的服务端访问正常。
2. 确认源端和目标端的数据库、表、订阅等是否正常（如果不存在可能导致权限信息导入失败）。
3. 执行 `taosx privileges` 命令，及时处理错误信息。

## 8. 使用场景

### 8.1 迁移用户和权限信息

使用命令：`taosx privileges -f "taos://``localhost``" -t "taos://other"`。
有：
集群 A：root 用户，密码 `taosdata`，host： `hosta`, 原生连接，使用端口: `6030`；
集群 B：root 用户，密码 `abcdef`，host：`hostb`，WebSocket 连接，使用端口：`16041`；
则同步命令如下：
```bash
taosx privileges \
  -f "taos://root:taosdata@hosta:6030" \
  -t "taos+ws://root:abcdef@hostb:16041"
```

### 8.2 备份用户和权限信息

使用命令：`taosx privileges -f "taos://``localhost``" -o ./path/to/file`。
有：
集群 A：root 用户，密码 `taosdata`，host： `hosta`, 原生连接，使用端口: `6030`；
备份到文件：`/data/20240531.security`
则备份命令如下：
```bash
taosx privileges \
  -f "taos://root:taosdata@hosta:6030" \
  -o /data/20240531.security
```

### 8.3 从已有备份中恢复用户和权限信息

使用命令：`taosx privileges -i ./path/to/file -t "taos://other"`
有：
备份文件：`/data/20240531.security`
集群 A：root 用户，密码 `taosdata`，host： `hosta`, 原生连接，使用端口: `6030`；
则恢复命令如下：
```bash
taosx privileges \
  -i /data/20240531.security
  -t "taos://root:taosdata@hosta:6030"
```

## 9. 约束和限制

约束：
- 对于 3.1 版本，仅支持 3.1.1.x 版本的数据（源或备份文件）导入到 3.1 版本（且目标端版本 >= 3.1.1.x）
- 对于 3.3 版本，仅支持 3.3.1.x 版本的数据（源或备份文件）导入到 3.3 版本（且目标端版本 >= 3.3.1.x）

## 10. 常见错误和排查

导入错误分为两类：
一类是 taosx 错误：
- 无导入导出权限：仅 root 用户可进行导入导出操作。
- 不支持版本 X 的备份文件恢复到 Y 版本：请按 **“约束和限制”** 中的版本约束说明使用。 
- 指定了导入项目（用户名密码或权限），但文件中不存在该信息。
一类是 taosc 错误：
- 导入用户名和密码时用户已存在（`import user` 报错）。
- 单独导入权限时，用户不存在。
- 导入的权限已在用户的权限列表中。
- 导入权限时权限所关联的对象不存在，包括数据库、Topic、表。

## 11. 可观测性

- 导入和导出操作都比较快，APi 和前端均为同步操作，前端无可观测性参数。
- 日志中须记录导入导出操作、失败的上下文信息和错误来源。
- 导入和导出操作应在 audit log 中体现。

## 12. 安装和卸载

无变化。

## 13. 文档

需要修改企业版文档以添加此功能说明，不需要修改官网文档。
- 
  TD-30358

## 14. 附录

### 14.1 SQL 实现

#### 14.1.1 Show users full

为已有 SQL 命令 `show users` 命令新增参数 `full` 以输出用户的详细信息。
```plaintext
show users full;
```

返回数据的表结构：
| Column | Data Type | Description |
| --- | --- | --- |
| 1 | name | VARCHAR(24) |
| 2 | super | TINYINT |
| 3 | enable | TINYINT |
| 4 | sysinfo | TINYINT |
| 5 | createdb | TINYINT |
| 6 | encrypted_pass | VARCHAR(100) |
| 7 | allowed_host | VARCHAR(49152) |

#### 14.1.2 Create User 

为 `create user` 命令新增参数 `encrypted_pass` 来导入用户名和密码。
```sql
CREATE USER user_name PASS 'encrypted_pass'
  SYSINFO {1|0} CREATEDB {1|0} IS_IMPORT 1 [HOST 'allowed_host'];
```

allowed_host 传入从 `show users full` 命令中对应的字段，格式需要转换：
Show users full显示的是：127.0.0.1,127.0.0.2 
HOST接受的格式是：'127.0.0.1','127.0.0.2'
也就是说show显示的是个字符串，create中host接受的是个字符串组

返回的错误：
1. Fields are not allowed to be empty.
Name, encryptedPass 不能为空
1. The field is too long.
Name, encryptedPass 字段超长
1. User already exist.
已存在指定的用户名

通过 alter user命令修改enable参数
```plaintext
ALTER USER user_name ENABLE 0
```

#### 14.1.3 Show user privileges (已有)

```sql
taos> show user privileges;
         user_name          |  privilege   |            db_name             |           table_name           |           condition            |             notes              |
================================================================================================================================================================================
 t1                         | read         | log                            |                                |                                |                                |
 t1                         | read         | test                           |                                |                                |                                |
 t1                         | read         | cyjia_tmq1                     |                                |                                |                                |
 t1                         | read         | audit                          |                                |                                |                                |
 t1                         | write        | cyjia_tmq1                     |                                |                                |                                |
 t1                         | subscribe    | select_meters                  |                                |                                |                                |
 root                       | all          | all                            |                                |                                |                                |
Query OK, 7 row(s) in set (0.001884s)
```

恢复或导入时，使用 `grant` 命令：
```sql
GRANT {privilege} ON `{db_name}`.`{table_name || '*'}`
  [with {contition}]  TO `{user_name}`;
```

将权限数据导入。

### 14.2 Explorer 服务端 API

Explorer 后端提供 API 进行导入导出操作：

|  | Endpoint | Method | Input | Response |
| --- | --- | --- | --- | --- |
| 导入 | /api/-/import | POST | JSON 格式。 - `server`：源 TDengine 服务端访问地址（Adapter 路径，通常是 `http://host:6041` URL 地址；注意：支持 HTTP/HTTPS 连接） - `passwords`: bool 值，表示是否包含该信息。 - `privileges`: bool 值，表示是否包含权限信息。 - `whitelist`: bool 值，表示是否包含白名单信息。 其中，`passwords`,`privileges`,`whitelist` 默认均为 false。 | 成功：Status 200. JSON 格式： ```json { "success": { "passwords": Number, "privileges": Number, }, "fails": { "passwords": [ { "user": String, "reason": String } ], "privileges": [ { "user": String, "privilege": String, "reason": String } ] } } ``` 失败： Status 500. JSON 格式： ```json { "code": 65536, "message": "Error reason" } ``` |

## 15. 参考文档
