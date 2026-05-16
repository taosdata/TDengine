# 定时触发vnode commit FS

## 1. 背景

Jira:
TS-5837

启动恢复时间比较长，需要缩短恢复时间。

## 2. 变更历史


| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/2/7 | 0.1 | 陈东明 |  |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

无

## 4. 行为说明

为create database 和 alter database 2个命令添加新的option：
```bash
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]

database_options:
    database_option ...

database_option: {
    FLUSH_INTERVAL value
}
```

FLUSH_INTERVAL: 数据定时执行flush的时间间隔。单位为 s。默认为 0，表示不会自动执行flush。
```bash
ALTER DATABASE db_name [alter_database_options]

alter_database_options:
    alter_database_option ...

alter_database_option: {
    FLUSH_INTERVAL value  
}
```


## 5. 性能

设置FLUSH_INTERVAL后，所在的db在taosd重启后恢复的时长等于或者小于设置interval时长。

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

### 8.1 创建带有flush interval的db

### 8.2 修改db的flush interval

## 9. 约束和限制

约束：无
限制：无

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

修改https://docs.taosdata.com/reference/taos-sql/database/中的create database

## 14. 参考文档

## 15. 附录
