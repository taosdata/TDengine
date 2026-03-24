# 安全函数 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-03 | 2025-12-03 | 1.0 | 金明磊 | 新建 |
| 2026-1-27 | 2026-1-27 | 1.1 | 关胜亮 | 修订格式 |

## 2. 测试目标

安全函数常规测试及性能测试，验证正确性，测试性能情况。
1. 测试范围
   - 有 null 存在情况下的正确性
   - 空字符串输入情况下的正确性
   - 单字符串输入情况下的正确性
   - 异常情况下的正确性
   - 基础性能测试，所有函数循环执行，统计查询性能并分析
2. 测试数据
   - 多种类型的超级表和子表

## 3. 参考文档

[安全函数 FS](https://taosdata.feishu.cn/wiki/SVnqw7lEaiSntgkj6udcnk3MnQg)

## 4. 测试结论

1. 功能测试正确
2. 性能测试符合预期

## 5. 测试环境

Linux：ubuntu 20.4

## 6. 功能测试

### 6.1 功能

测试脚本：test_scalar_crypto.py
函数列表：
1. aes_encrypt
2. aes_decrypt
3. sm4_encrypt
4. sm4_decrypt
5. md5
6. sha1/sha
7. sha2
8. from_base64
9. to_base64
10. mask_full
11. mask_partial
12. mask_none
覆盖的测试场景及用例：

| 测试场景 | 用例名称 | 预期结果 | 测试结果 |
| --- | --- | --- | --- |
| 基本用例 | smoke | 每个函数在常见输入情况下，查询或写入语句中使用，检查获得的结果符合预期 | 通过 |
| Null 输入 | null | 空输入情况下获得正确输出 | 通过 |
| 空字符串输入 | empty | 空字符串情况下获得正确输出 | 通过 |
| 单字符输入 | single | 单字符情况下获得正确输出 | 通过 |
| 异常情况 | error | 能够得到预期错误信息 | 通过 |

### 6.2 可用性

无

### 6.3 可靠性

重复测试无异常

## 7. 易用性测试（可选）

## 8. 长期稳定性测试（可选）

## 9. 性能测试

#### 9.0.1 测试方法

使用 taosBenchmark 对下列语句并发 10，进行 100 次查询：
```sql
select mask_full('mytext', '***')
select mask_partial('mytext', 1, 2, '*')
select mask_none('mytext')
select sha2('mytext', 512)
select sha2('mytext', 384)
select sha2('mytext', 256)
select sha2('mytext', 224)
select sha1('mytext')
select sha('mytext')
select md5('mytext')
select from_base64(to_base64('mytext'))
select aes_decrypt(aes_encrypt('mytext', 'mykeystring'), 'mykeystring')
select sm4_decrypt(sm4_encrypt('mytext', 'mykeystring'), 'mykeystring')
```

查询使用以下 json 文件：
```plaintext {wrap}
{
        "filetype": "query",
        "cfgdir": "/etc/taos",
        "host": "127.0.0.1",
        "port": 6030,
        "user": "root",
        "password": "taosdata",
        "confirm_parameter_prompt": "no",
        "continue_if_fail": "yes",
        "databases": "test",
        "query_times": 10,
        "query_mode": "taosc",
        "specified_table_query": {
                "query_interval": 1,
                "threads": 3,
                "sqls": [
                        {
                                "sql": "select mask_full('mytext', '***')",
                                "result": "./query_res0.txt"
                        },
                        {
                                "sql": "select mask_partial('mytext', 1, 2, '*')",
                                "result": "./query_res1.txt"
                        },
                        {
                                "sql": "select mask_none('mytext')",
                                "result": "./query_res2.txt"
                        },
                        {
                                "sql": "select sha2('mytext', 512)",
                                "result": "./query_res3.txt"
                        },
                        {
                                "sql": "select sha2('mytext', 384)",
                                "result": "./query_res4.txt"
                        },
                        {
                                "sql": "select sha2('mytext', 256)",
                                "result": "./query_res5.txt"
                        },
                        {
                                "sql": "select sha2('mytext', 224)",
                                "result": "./query_res6.txt"
                        },
                        {
                                "sql": "select sha1('mytext')",
                                "result": "./query_res7.txt"
                        },
                        {
                                "sql": "select sha('mytext')",
                                "result": "./query_res8.txt"
                        },
                        {
                                "sql": "select md5('mytext')",
                                "result": "./query_res9.txt"
                        },
                        {
                                "sql": "select from_base64(to_base64('mytext'))",
                                "result": "./query_res10.txt"
                        },
                        {
                                "sql": "select aes_decrypt(aes_encrypt('mytext', 'mykeystring'), 'mykeystring')",
                                "result": "./query_res11.txt"
                        },
                        {
                                "sql": "select sm4_decrypt(sm4_encrypt('mytext', 'mykeystring'), 'mykeystring')",
                                "result": "./query_res12.txt"
                        }
                ]
        }
}
```

#### 9.0.2 测试结果

1. 查询平均耗时   avg: 0.006423s 
2. 查询最小耗时   avg: 0.007068s 
3. 查询最大耗时   avg: 0.008126s 

#### 9.0.3 结果说明

以上均为新函数，没有历史性能数据供对比。符合预期

## 10. 安全测试

本代码就是安全相关功能，此章节不需填写

## 11. 兼容性测试

无兼容性问题

## 12. 已知问题和限制

已知的限制
1. AES 目前只支持 128-bit ECB and CBC
2. 除 base64 外，其它函数仅支持 varchar，不支持 nchar
3. 不支持上述之外的其他类型
