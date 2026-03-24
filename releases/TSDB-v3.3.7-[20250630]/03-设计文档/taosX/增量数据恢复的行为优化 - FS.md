# 增量数据恢复的行为优化 - FS

## 1. 背景

在穿网闸场景中，使用增量数据恢复遇到以下问题：
1. 数据恢复成功后，已处理成功的数据文件会保留在原地并不断累积。
2. 在测试中发现，网闸传输软件有时不能正确传输备份文件，会造成少量数据丢失。
为解决上述问题，需要对 taosX 增量数据恢复进行优化。
相关的 JIRA：
https://jira.taosdata.com:18080/browse/TS-6456
https://jira.taosdata.com:18080/browse/TS-6578

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/5/9 | 0.1 | @杨志宇 | 初稿 |
|  |  |  |  |

## 3. 定义

无

## 4. 行为说明

### 4.1 恢复成功后的文件操作

增量数据恢复成功后，支持自动删除备份文件或者将备份文件移动到指定文件夹。

#### 4.1.1 配置参数

| 参数名称 | 说明 | 值 | 是否必填 | 默认值 |
| --- | --- | --- | --- | --- |
| post_action | 恢复成功后的操作。 1. 如果不设置，不执行任何操作。 1. 如果设置，执行 delete/move 操作。 | 合法的字符串 1. 删除：delete/del/rm/remove 1. 移动：move/mv 1. 不处理：None/none | 否 | 不处理 |
| move_to | 恢复成功后，移动到的目录。仅当`post_action = move`时生效。 | 一个合法的 dir pattern，支持使用带日期时间模版。例如： `move_to=/data/tmp/%Y-%m-%d`，数据文件名称为：`x85c20042893-1749192676769-498-2.z`，则，数据文件会被移动到：`/data/tmp/2025-06-06/x85c20042893-1749192676769-498-2.z` | 当`post_action = move`时，必填 | 无 |

下面是一个示例：
```shell {wrap}
taosx run -f "local:/root/taosx/backup?post_action=del" -t "taos://127.0.0.1:6030/test"
```

#### 4.1.2 异常处理

1. post_action 配置的值无效：报错，任务启动失败。
2. move_to 配置的目录不存在：尝试自动创建，如果创建失败，报错，任务启动失败。

### 4.2 数据文件支持 checksum 校验

#### 

## 5. 性能

无

## 6. 兼容性

无

## 7. 运维

无

## 8. 使用场景

1. 数据恢复成功，直接删除备份文件。
```bash {wrap}
taosx run -f "local:/root/taosx/backup?post_action=del" -t "taos://127.0.0.1:6030/test"
```

1. 数据恢复成功，移动到其他目录
```bash {wrap}
taosx run -f "local:/root/taosx/backup?post_action=move&move_to=/root/taosx/arch" -t "taos://127.0.0.1:6030/test"
```

## 9. 约束和限制

无

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

无

## 14. 参考文档

无

## 15. 附录

### 15.1 Zstandard Frame

| Magic number | Frame header | Data Block | [more data blocks] | [content checksum] |
| --- | --- | --- | --- | --- |
| 4 bytes | 2 - 14 bytes | n bytes |  | 0 - 4 bytes |

Magic number: 4 Bytes, little-endian format. Value : 0xFD2FB528
Frame header: 

| Frame_Header_Descriptor | Window_Descriptor | Dictionary_ID | Frame_Content_Size |
| --- | --- | --- | --- |
| 1 byte | 0 - 1 byte | 0 - 4 bytes | 0 - 8 bytes |
