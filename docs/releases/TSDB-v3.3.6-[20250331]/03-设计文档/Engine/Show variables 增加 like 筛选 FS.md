# Show variables 增加 like 筛选 FS

## 1. 背景

[TS-5650](https://jira.taosdata.com:18080/browse/TS-5650)
参考：[show variables 支持模糊匹配 RS](https://taosdata.feishu.cn/wiki/F1kSwc0yCiY6AlkITp8cs2Vwne3)
本次仅实现：支持 like pattern ，可使用 like pattern 根据 name 进行筛选

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025-03-06 | 1.0 | 任新胜 | 新建 |

## 3. 定义

无

## 4. 行为说明

以下 sql ，增加可选项 [like pattern]， 只支持通过 name 列进行筛选
1. show variables;  
2. show cluster variables;
3. show local variables;
4. show dnode 1 variables;
示例：
1. show variables like "build%"   查看 "build" 开头的变量配置
2. show cluster variables like "%max%"  查看包含 "max" 子串的变量
LIKE 条件使用通配符字符串进行匹配检查，规则如下：
1. '%'（百分号）匹配 0 到任意个字符；'_'（下划线）匹配单个任意 ASCII 字符。
2. 如果希望匹配字符串中原本就带有的 _（下划线）字符，那么可以在通配符字符串中写作 _，即加一个反斜线来进行转义。
3. 通配符字符串最长不能超过 100 字节。不建议使用太长的通配符字符串，否则将有可能严重影响 LIKE 操作的执行性能。

## 5. 性能

无

## 6. 兼容性

无兼容性问题

## 7. 运维

无

## 8. 使用场景

查看配置参数时使用，可快速过滤

## 9. 约束和限制

无

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

官方文档 SHOW 命令介绍章节相关段落增加 说明

## 14. 参考文档

无

## 15. 附录

无
