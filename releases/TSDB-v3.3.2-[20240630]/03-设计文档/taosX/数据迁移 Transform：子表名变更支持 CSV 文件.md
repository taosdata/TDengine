# 数据迁移 Transform：子表名变更支持 CSV 文件

## 1. 背景

需求来自交付：。
TS-5040

长庆油田子表名的命名：设备名_超级表名，设备名统一通过A2系统指定。最近集团对A2系统变更了命名规则，客户需要统一变更子表名。数量有几百万的规模，而TDengine是不支持子表名的变更操作的，为满足客户要求，需要使用 taosx 做一次数据迁移来同时完成子表名的变更。
但目前taosx无法支持每个表的转换规则（当前支持前缀、后缀、表名模板、正则表达式替换），本文档约定提供一种新的转换规则（映射/map），使用 CSV 文件表示表名映射关系，使用方法沿用之前的表名转换规则。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/06/26 | 0.1 | @霍琳贺 | Draft |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

### 3.1 表名映射关系文件

格式为两列无 Header 的 CSV 文件，形如：
```plaintext
d0,n0
d1,n1
```

表示将表名 `d0` 映射为 `n0`，`d1` 映射为 `n1`。

## 4. 行为说明

仅支持命令行，沿用之前配置表名重命名规则参数 `-T`：
```bash
taosx run -f "taos:///db1" -t "taos:///db2?assert" \
  -T rename-child-table:prefix:p1
```

使用表名映射关系文件，`-T` 参数形如 `rename-child-table:map:@<文件名>`，示例如下：
```bash

## 5. Prepare rename map

echo "d0,n0" > ./rename.csv

## 6. Run migration command

taosx run -f "taos:///db1" -t "taos:///db2?assert" \
  -T rename-child-table:map:@./rename.csv
```

## 7. 性能

使用表名映射关系文件时，性能相对无重命名时有所下降，符合预期。

## 8. 兼容性

无。

## 9. 运维

无。

## 10. 使用场景

1. 在进行迁移时需要修改表名，此时可以使用表名映射关系文件进行自定义配置

## 11. 约束和限制

无。

## 12. 常见错误和排查

- 指定表名映射文件不存在：
  - `Rename parse error: Invalid csv input: No such file or directory (os error 2)`
- 表名映射文件格式错误，使用 `old,new` 正确的 CSV 文件，且包含两列：
  - `Rename parse error: Invalid csv content, expect `old,new` pair, but got `d1``

## 13. 可观测性

无

## 14. 安装和卸载

无

## 15. 文档

无

## 16. 参考文档

**Note: 用户手册中尽量不出现设计方案或实现相关的内容。**
