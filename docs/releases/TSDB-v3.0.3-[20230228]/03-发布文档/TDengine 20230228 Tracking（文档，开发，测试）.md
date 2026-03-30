# TDengine 20230228 Tracking（文档，开发，测试）

## 1. Part I: taosd/taosc

| 功能 | JIRA | User Manual等文档的链接 | Dev Owner | 提测时间 | 开发自测测试报告链接 | Test Owner | QA测试报告链接 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Tag Index | TD-20384 | [Tag Index](https://taosdata.feishu.cn/wiki/wikcn9NP290i7pXGocUSCnVAQlh) | @邓怡豪 | 2023.02.10 | [report test ](https://taosdata.feishu.cn/wiki/wikcncrsm9eFCIkLB7kbe0JuP3b) | @段宽军 | [TD-21965 Tag 列索引测试报告](https://taosdata.feishu.cn/wiki/wikcnEMrnkofVcfFUIxoanxWvKg) |
| 行转列之后与旧版本 WAL的兼容性 | TD-21900 | [WAL 兼容性](https://taosdata.feishu.cn/wiki/wikcnRCrMeKu0URMx3vNcOFveXb) | @程洪泽 | 2023.2.15 |  | @段宽军 | [TD-21971 WAL 格式变化后的兼容性测试](https://taosdata.feishu.cn/wiki/wikcndtwg3umt8ECHcgJ40hj4Rh) |
| 行转列之后 taosX 所用API的兼容性保证 | TD-21899 | TODO | @王明明 | 2023.2.13 |  | @贾晨阳 | [（进行中）TD-22972 taosX所用API支持行转列格式](https://taosdata.feishu.cn/wiki/wikcnqUEZ6OLXOH4pdSRal93aAg) |
| TDengine OEM 机制 | TD-18799 | [OEM Release](https://taosdata.feishu.cn/wiki/wikcnl8Hhazm18xvbZgBhe3Beme) | @Shuduo @任新胜 | 2023.2.13 |  | @肖平 |  |
| 流计算结果可以写入已经存在的超及表 | TD-21454 | [Stream Processing](https://taosdata.feishu.cn/wiki/wikcnnK7Gs2bWGx7tjOt1i5dcne) | @Jicong | 2023.2.7 |  | @贾靖斌 | [TD-21966 流计算-写入已存在的超级表](https://taosdata.feishu.cn/wiki/wikcnTDHbqAwNcdB2AQVa8T0ong) |
| 流计算支持自定义 tag | TD-21455 | [Stream Processing](https://taosdata.feishu.cn/wiki/wikcnnK7Gs2bWGx7tjOt1i5dcne) | @刘垚 | 2023.2.10 |  | @贾靖斌 | [TD-21973 流计算-自定义 tag](https://taosdata.feishu.cn/wiki/wikcnIr8Gra1poHhXIaql4qGDUg) |
| 服务端滚动升级 | TD-15931 | N/A | @Benguang | N/A |  | @陈浩然 | 验证从3.0.2.4/5 到 3.0.3.0的滚动升级或全量升级 |
| 支持修改 dnode 的 End point | TD-20047 | [Adapt existing cluster to new Endpoint](https://taosdata.feishu.cn/wiki/wikcnV1lzkg5gwqIINkzVsv89Gf) | @关胜亮 | 2023.2.6 |  | @贾晨阳 | [TD-21967 Adapt existing cluster to new Endpoint](https://taosdata.feishu.cn/wiki/wikcnqqrtKbJBvPK1iSuQu24t3b) |
| 长查询不阻塞写入 | TD-19335 | N/A | @金明垒 | 2023.1.30 |  | @贾靖斌 | [TD-21960 长查询不能阻塞写入](https://taosdata.feishu.cn/wiki/wikcnvyYey3ipQjvfLQY7K1YBHf) |
| 副本移动后的查询性能优化 | TD-21474 | N/A | @程洪泽 | N/A |  | @李珲 | [TD-21474 优化副本移动后的查询性能验证报告](https://taosdata.feishu.cn/wiki/wikcnC2FBBq9vYVeMT81v8x6MVf) |
| Event Window for batch processing | TD-20266 | [Event Window](https://taosdata.feishu.cn/wiki/wikcnYfolqvThTHzqhm0hYpCb9b) | @Xiaoyu | 2023/2/1 | [Test Report](https://taosdata.feishu.cn/wiki/wikcnGBSgYiTuLEgxfeVxdHB3bc) | @郭向阳 | [TD-21963 完成 event window for batch processing功能的测试](https://taosdata.feishu.cn/wiki/wikcn7YJlVgb4W928ovnk02cGdd) |
| Data compact | TD-18797 | [Data Compact](https://taosdata.feishu.cn/wiki/wikcnv8joh1pPYMH9GUEEZdhFFb) | @程洪泽 | 2023.2.15 |  | @贾靖斌 | [TD-21959 完成数据compact功能的测试](https://taosdata.feishu.cn/wiki/wikcnQDrirJ1GBDrTNFxUlpvYAf) |
| Topic Sharing privilege control | TD-20611 | 该功能在 3.0.2.1 已经完成测试，但作为大版本的宣传亮点可以加入 3.0.3.0 release notes | @关胜亮 |  |  | @贾晨阳 | [用户权限功能测试](https://taosdata.feishu.cn/wiki/wikcnSWMw4f6SigRQJ4FA59wkJh) |
| 企业版授权支持修改过期时间和测点数 | TD-21649 | [Grant 优化](https://taosdata.feishu.cn/wiki/wikcnHx95c8XdoFLeMfRIiQIteb) | @徐开礼 | 2023/2/7 | [TD-21649 [3.0][grant] 企业版授权支持"过期时间/测点数"等改大改小](https://taosdata.feishu.cn/docx/EINWdHabVoh4CYxgePqcP5wBnXf) | @贾晨阳 | [3.0版本中支持grant授权变化的验证](https://taosdata.feishu.cn/wiki/wikcnsrSuHrlCuqKp3eV8TLIQTb) |
| 企业版授权支持基于 cluster id | TD-21650 | [Grant 优化](https://taosdata.feishu.cn/wiki/wikcnHx95c8XdoFLeMfRIiQIteb) | @徐开礼 | 2023/2/7 | [Grant 优化](https://taosdata.feishu.cn/wiki/wikcnHx95c8XdoFLeMfRIiQIteb) | @贾晨阳 | [3.0版本中支持grant授权变化的验证](https://taosdata.feishu.cn/wiki/wikcnsrSuHrlCuqKp3eV8TLIQTb) |
| 强制Fill | TS-2502 | [强制Fill](https://taosdata.feishu.cn/wiki/wikcn6wSajdH3LUrNkz2zHht4jb) | @潘魏 | 2023/2/7 | [强制Fill自测报告](https://taosdata.feishu.cn/wiki/wikcnDF1Y48u4iOzsZHO2V24sBf) | @郭向阳 | [TD-22213 完成 强制fill的功能测试](https://taosdata.feishu.cn/wiki/wikcnglJRbzdo7jNYCxnVqYPalh) |

## 2. Part II: Connectors & Tools

| 功能 | JIRA | Dev Owner | User Manual等文档的链接 | 提测时间 | 开发自测测试报告链接 | Test Owner | 测试报告链接 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| taosX 增加授权机制 | TD-21586 | @霍琳贺 | [taosX 企业版授权机制](https://taosdata.feishu.cn/wiki/wikcnAzzYqV3OWE9vz4qgbgLHd4) | 2/1 |  | @贾晨阳 | [TD-22155 taosX企业版授权功能](https://taosdata.feishu.cn/wiki/wikcne3pvzgyzaW1CPJgH4DA6xf) |
| taosX 支持 Data transformer | TD-22298 | @霍琳贺 | [# Transform in taosX](https://taosdata.feishu.cn/wiki/wikcngRpSaZ2dSKpBHfwXtLh7qf) | 2/22 |  | @贾晨阳 | 与霍琳贺交流，本月就只有文档，下个月才完成开发。 测试任务顺延到下个月。 |
| taos Explorer Phase one | TD-21258 | @姜亚利 | [taos Explorer](https://taosdata.feishu.cn/wiki/wikcnQ6lZKZeXGQ3HI0yK1bt0td) | 2/17 |  | @肖平 |  |
| Java connector TMQ |  | @霍立波 | [Connectors](https://taosdata.feishu.cn/wiki/wikcncpnjmIi0ZwAYQ8Ws6bSdod) | 2/15 |  | null |  |
| Python connector TMQ | TD-21600 | @Peng | [Connectors](https://taosdata.feishu.cn/wiki/wikcncpnjmIi0ZwAYQ8Ws6bSdod) | 2/1 |  | null |  |
| taosBenchmark 支持写入数据到指定的子表段 | TD-20424 | @Shuduo | [taosBenchmark](https://taosdata.feishu.cn/wiki/wikcnYR4NNEvmAfoBLvcJ9VamDh) | 2/10 |  | @陈浩然 |  |
| taosBenchmark 支持在命令行指定写入到特定 vgroup | TD-21806 | @Shuduo | [taosBenchmark](https://taosdata.feishu.cn/wiki/wikcnYR4NNEvmAfoBLvcJ9VamDh) | 2/6 |  | @陈浩然 |  |
| Grafana multiple dimesion |  | @Peng | [Grafana Plugin](https://taosdata.feishu.cn/wiki/wikcnKacXSSVm5l2snZkY4r9oEE) | 2/6 |  |  |  |
