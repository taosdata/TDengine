# 订阅支持 token 登录 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-28 | 2026-01-28 | 1.0 | 王明明 | 初稿 |

## 2. 背景

## 3. 行为说明

1. 订阅增加参数 td.connect.token 的配置，来支持设置token。
2. token 规则为：
  - 通过 create token 生成的 token，可以设置该参数 td.connect.token。
  - token 的优先级，高于账户名密码（即如果 consumer 同时设置 token 和 user/pass，优先使用token 创建consumer)
  - 如果 token 设置不正确，会报 token 不存在，不合法，无效等。具体可通过  taos_errstr(NULL) 获取错误信息（错误码存储在 terrno 里）
  - 如果正在 poll 数据的过程中，token 到期，被删除，或者被设置为disable，则poll 到数据为 NULL，错误信息获取同上一条。如果token 又变为有效，则可继续获取数据。
1. 实现逻辑：
   - tmq_consumer_new 函数创建 consumer 时，如果指定 token ，则优先使用 token 创建连接。
   - tmq_t 结果新增字段 tokenCode，记录 token 是否有效。在 consumer 的心跳里定期检测 token 是否有效（发送给mnode 的消息，如果是token 连接，会自动检测token 是否有效），如果无效，设置tokenCode。
   - tmq_consumer_poll 接口里检测 tokenCode 是否无效，如果无效，则设置 terrno，并返回 NULL。
   - tokenCode 的操作为原子操作，保证多线程安全。 

## 4. 性能

对性能无明显影响。

## 5. 安全

不涉及。

## 6. 兼容性

无。

## 7. 运维

无

## 8. 约束和限制

见行为说明。

## 9. 常见错误和排查

在开发调试过程中补充。

## 10. 可观测性

不涉及。

## 11. 安装和卸载

不涉及。

## 12. 文档

需要修改官网使用手册。

## 13. 参考文档

无

## 14. 附录

无。
