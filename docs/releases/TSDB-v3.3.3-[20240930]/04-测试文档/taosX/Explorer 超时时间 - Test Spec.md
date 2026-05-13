# Explorer 超时时间 - Test Spec

## 1. 测试目标

确认本次迭代符合 [Explorer 超时时间](https://taosdata.feishu.cn/wiki/IHgewGmYMiS3HSkHXhQcjJqjnwb)

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
|  |  |  |  |
|  |  |  |  |

## 3. 测试范围

- /ds 接口的超时时间
- 不同配置场景下的超时优先级

## 4. 测试结论

<quote-container>
测试结论中包含结论和关键数据，但不需罗列过多细节，此处需要把把握信息的详细程度，原则上是外部 Reviewer 能够获得清晰的测试结论且尽量没有冗余信息为标准（这个标准是一句正确的废话，具体实行中需要大家 case by case 来处理）
</quote-container>

## 5. 开发质量报告

结论：本特性/优化的开发质量是（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 |  |
| 基础测试用例不通过 |  |
| Bug 总数 |  |
| 严重 Bug 总数 |  |

## 6. 已知问题和限制

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
- aaa
- bbb

## 7. 测试环境

- OS: Windows, Linux, macOS
- Browser: Chrome

## 8. 测试数据 (Optional)

这里用于描述性能、稳定性测试时的数据准备工作，包括但不局限于：
- field的数量、类型
- tag的数量、类型
- 数据量的大小

## 9. 测试用例

### 9.1 功能

| 分类 | 测试步骤 | 预期结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 默认的 timeout | 创建不同类型的任务，检查连通性检查 GET /ds/in/validate 中的 timeout | 请求在 30s 正常返回 |  |  |
|  | 创建不同类型的任务，模拟网络故障然后，检查连通性检查GET /ds/in/validate中的 timeout | 所有数据源均为在超过 30s 后返回超时错误 |  |  |
|  | ```sql time curl 'http://192.168.2.14:6060/api/x/ds/in/validate?timeout=5' \ -H 'Accept: application/json, text/plain, */*' \ -H 'Accept-Language: q=0.8, en' \ -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' \ -H 'Cache-Control: no-cache' \ -H 'Connection: keep-alive' \ -H 'Content-Type: application/json' \ -H 'Cookie: login_TDC=true; AppID={}; sidebarStatus=1; TDengine-Token=Basic%20cm9vdDp0YW9zZGF0YQ==' \ -H 'Origin: http://192.168.2.14:6060' \ -H 'Pragma: no-cache' \ -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36' \ --data-raw '{"from":"avevaHistorian://aaAdmin:aaAdmin@192.168.2.17?mode=synchronize&tags=%2a&tagListSize=10&timeWindow=1d&retrieveInterval=10s&tolerance=0ms&read_concurrency=0&batch_size=10000&keep_raw_data=false&keep_raw_data_days=1","to":"taos://root:taosdata@localhost:6030/test2"}' \ --insecure ``` |  |  |  |
|  | 创建不同类型的任务，检查连通性检查 POST /ds/in/validate 中的 timeout | 请求在 30s 正常返回 |  |  |
|  | 创建不同类型的任务，模拟网络故障然后，检查连通性检查 POST /ds/in/validate 中的 timeout | 所有数据源均为在超过 30s 后返回超时错误 |  |  |
|  | 创建不同类型的任务，检查连通性检查 GET /ds/in/sample 中的 timeout | 请求在响应时间内正常返回 |  |  |
|  | 创建不同类型的任务，模拟网络故障然后，检查连通性检查 GET /ds/in/sample 中的 timeout | 数据源分别在超过 xxs 后返回超时错误： 30s：kafka、mqtt、mongodb 120s：histoiran、mysql、postgres、oracle 、MSSQL |  |  |
|  | 创建OPC CSV的任务，检查文件合法性校验 GET /ds/in/point/file/is_valid 中的 timeout | 请求在响应时间内正常返回 |  |  |
|  | 创建OPC CSV的任务，模拟网络故障然后，检查文件合法性校验 GET /ds/in/point/file/is_valid 中的 timeout | 30s 后返回超时错误 |  |  |
| explorer 请求 | 在 explorer 上进行不同数据源的连通性检查，检查 request 是否有 timeout 参数 | 所有数据源均没有 timeout 参数 |  |  |
|  | 创建OPC CSV的任务，检查文件合法性校验 GET /ds/in/point/file/is_valid 检查 request 是否有 timeout 参数 | 没有 timeout 参数 |  |  |
|  | 创建histoiran、mysql、postgres、oracle、MSSQL的任务，获取示例数据时 检查 request 是否有 timeout 参数 | 均有 timeout 参数，其中 historian 120 秒，其它数据源 30 秒 |  |  |
|  | 创建histoiran、mysql、postgres、oracle、MSSQL的任务，检查 explorer ui 中获取示例数据是否有 timeout 设置 | 均有 timeout 设置，其中 historian 120 秒，其它数据源 30 秒 |  |  |
|  | 创建除了 histoiran、mysql、postgres、oracle、MSSQL的任务，获取示例数据时 检查 request 是否有 timeout 参数 | 没有 timeout 参数 |  |  |
|  | 创建除了 histoiran、mysql、postgres、oracle、MSSQL的任务，检查 explorer ui 中获取示例数据是否有 timeout 设置 | 没有 timeout 设置 |  |  |
| timeout 优先级 | 环境变量超时 25 ，配置文件超时 20，模拟网络故障，检查请求超时时间 | timeout=20 |  |  |
|  | 环境变量超时 25，模拟网络故障，检查请求超时时间 | timeout=25 |  |  |
|  | 环境变量超时 25 ，配置文件超时 20，模拟网络故障，检查请求带?timeout=60后的超时时间 | timeout=60 |  |  |
|  |  |  |  |  |
|  |  |  |  |  |


### 9.2 可用性

测试用例包括但不局限于：
- UI是否美观？
- 交互是否合理？
- 字体、字号是否合适？
- 是否存在错别字？

### 9.3 可靠性

这里用于描述稳定性测试相关的内容。

### 9.4 性能

这里用于描述性能测试相关的内容。

### 9.5 安全性

测试用例包括但不局限于：
- 日志中是否包含敏感信息？

### 9.6 兼容性

测试用例包括但不局限于：
- 升级安装后，老版本（上一个版本）下创建的任务，能否继续执行？
- 升级安装后，未写入任何数据（未创建任何新任务），是否能够降级并继续运行
- 升级安装后，写入新数据（或创建新的任务）， 是否能够降级并继续运行

### 9.7 本地化

测试用例包括但不局限于：
- 点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示？

## 10. 待讨论(Optional)

这里用于记录在测试或用例编写过程中想到的需要讨论的问题：
- aaa
- bbb

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: abc

## 12. 测试计划 (Optional)

这里用于计划此 feature 测试的开始和结束时间。

## 13. 风险评估

用户记录这个需求的潜在风险，例如：对于功能复杂，开发时间长的功能，是否需要分期提测？

## 14. 测试备忘 (Optional)

```sql {wrap}
mysql
./blade create network delay --time 4200 --interface ens160 --remote-port 3306
time curl 'http://192.168.2.14:6060/api/x/ds/in/sample?dsn=mysql%3A%2F%2Froot%3A123456%40192.168.1.45%3A3306%2Ftest_ci%3Fcharset%3Dutf8%26ssl_mode%3DPREFERRED%26sql%3Dselect%2520%252a%2520from%2520test_ci%252etb_test_ci%2520where%2520ts%2520%253E%2520%2524%257Bstart%257D%2520and%2520ts%2520%253C%2520%2524%257Bend%257D%26start%3D2023-09-01T00%253A00%253A00%252B08%253A00%26read_concurrency%3D0%26batch_size%3D10000%26sample_data_limit%3D100000&timeout=50' \
>   -H 'Accept: application/json, text/plain, */*' \
>   -H 'Accept-Language: q=0.8, en' \
>   -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' \
>   -H 'Cache-Control: no-cache' \
>   -H 'Connection: keep-alive' \
>   -H 'Cookie: login_TDC=true; AppID={}; sidebarStatus=1; TDengine-Token=Basic%20cm9vdDp0YW9zZGF0YQ==' \
>   -H 'Pragma: no-cache' \
>   -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36' \
>   --insecure

histoiran
./blade create network delay --remote-port=1433-1883 --interface=ens160 --time=20000
time curl 'http://192.168.2.14:6060/api/x/ds/in/sample?dsn=avevaHistorian%3A%2F%2FaaAdmin%3AaaAdmin%40192.168.2.17%3Fmode%3Dsynchronize%26table%3DRuntime%252edbo%252eHistory%26tags%3D%252a%26tagListSize%3D10%26beginDateTime%3D2024-09-22T00%253A00%253A00%252B08%253A00%26timeWindow%3D1d%26retrieveInterval%3D10s%26tolerance%3D0ms%26read_concurrency%3D0%26batch_size%3D10000%26keep_raw_data%3Dfalse%26keep_raw_data_days%3D1%26sample_data_limit%3D5&timeout=120' \
  -H 'Accept: application/json, text/plain, */*' \
  -H 'Accept-Language: q=0.8, en' \
  -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Cookie: login_TDC=true; AppID={}; sidebarStatus=1; TDengine-Token=Basic%20cm9vdDp0YW9zZGF0YQ==' \
  -H 'Pragma: no-cache' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36' \
  --insecure

pg
./blade create network delay --time 6000 --interface ens160 --remote-port 5432
time curl 'http://192.168.2.14:6060/api/x/ds/in/sample?dsn=postgres%3A%2F%2Fpostgres%3Atbase125%2521%40192.168.1.45%3A5432%2Ftest%3Fssl_mode%3DPREFER%26sql%3Dselect%2520sint%2520%252Cconcat%2528%2527%2524%257BF%257D%2527%252C%2520%2527T%2527%252C%2520ttnozone%252C%2520%2527%252B08%253A00%2527%2529%2520as%2520ts%2520from%2520public%252epg_ci_%2524%257BYmd%257D%2520where%2520ttnozone%2520%253E%253D%2524%257Bstart_time%257D%2520and%2520ttnozone%2520%253C%2524%257Bend_time%257D%26start%3D2024-04-22T00%253A00%253A00%252B08%253A00%26end%3D2024-09-30T00%253A00%253A00%252B08%253A00%26read_concurrency%3D0%26batch_size%3D10000%26sample_data_limit%3D10&timeout=120' \
  -H 'Accept: application/json, text/plain, */*' \
  -H 'Accept-Language: q=0.8, en' \
  -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Cookie: login_TDC=true; AppID={}; sidebarStatus=1; TDengine-Token=Basic%20cm9vdDp0YW9zZGF0YQ==' \
  -H 'Pragma: no-cache' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36' \
  --insecure
  
oracle
./blade create network delay --time 8000 --interface ens160 --remote-port 1521
time curl 'http://192.168.2.14:6060/api/x/ds/in/sample?dsn=oracle%3A%2F%2Ftest_user%3A123456%40192.168.1.45%3A1521%2FORCLPDB%3Fsql%3Dselect%2520%252a%2520from%2520taosx_test_ci_%2524%257BYmd%257D%2520where%2520t_time%253E%2524%257Bstart%257D%2520and%2520t_time%253C%2524%257Bend%257D%26start%3D2024-05-25T00%253A00%253A00%252B08%253A00%26read_concurrency%3D0%26batch_size%3D10000%26sample_data_limit%3D100&timeout=10' \
  -H 'Accept: application/json, text/plain, */*' \
  -H 'Accept-Language: q=0.8, en' \
  -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Cookie: login_TDC=true; AppID={}; sidebarStatus=1; TDengine-Token=Basic%20cm9vdDp0YW9zZGF0YQ==' \
  -H 'Pragma: no-cache' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36' \
  --insecure

sql server
./blade create network delay --time 20000 --interface ens160 --remote-port 3433
time curl 'http://192.168.2.14:6060/api/x/ds/in/sample?dsn=mssql%3A%2F%2Ftest%3Atbase125%2521%40192.168.1.66%3A3433%2Fci_test%3Fencryption%3DNotSupported%26trust_cert%3Dtrue%26sql%3Dselect%2520%252a%2520from%2520ci_test%252edbo%252eTestTable%2520where%2520dDateTimeOffset%2520%253E%2520%2524%257Bstart%257D%2520and%2520dDateTimeOffset%2520%253C%2520%2524%257Bend%257D%26start%3D2024-07-01T00%253A00%253A00%252B08%253A00%26read_concurrency%3D0%26batch_size%3D10000%26sample_data_limit%3D1000&timeout=120' \
  -H 'Accept: application/json, text/plain, */*' \
  -H 'Accept-Language: q=0.8, en' \
  -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' \
  -H 'Cache-Control: no-cache' \
  -H 'Connection: keep-alive' \
  -H 'Cookie: login_TDC=true; AppID={}; sidebarStatus=1; TDengine-Token=Basic%20cm9vdDp0YW9zZGF0YQ==' \
  -H 'Pragma: no-cache' \
  -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36' \
  --insecure
```


## 15. 参考文档 (Optional)

这里用于添加对该需求测试有帮助的文档链接：
- [link to functional spec]
- aaa
- bbb
