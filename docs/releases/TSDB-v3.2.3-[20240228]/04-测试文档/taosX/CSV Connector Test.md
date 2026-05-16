# CSV Connector Test 

## 1. Backgroud

主要针对改动： [CSV - 配置参数](https://taosdata.feishu.cn/wiki/YWY2w7lWTiPyocki2vHcnGwnnFf)

## 2. 测试结论

1. CSV 配置参数的功能改动基本实现
2. 多个CSV文件的上传目前还不支持 
  TD-28675

1. 由于目前taosx 服务返回错误信息的本地化没有提供， 导致以下问题目前无法解决
  TD-28657

## 3. 测试用例

| Case | Result |  |
| --- | --- | --- |
| 支持有header的数据导入 | pass |  |
| 支持无header的数据导入 | pass |  |
| 支持单个csv文件上传导入 | pass |  |
| 支持多个csv文件上传导入 | fail | [TD-28675](https://jira.taosdata.com:18080/browse/TD-28675) |
| 支持单个CSV大文件(大于1GB)的处理 | pass |  |
| 支持一个文件夹内指定一个CSV文件地址 | pass | [TD-28663](https://jira.taosdata.com:18080/browse/TD-28663) |
| 支持一个文件夹内的多个csv文件导入 | pass | [TD-28663](https://jira.taosdata.com:18080/browse/TD-28663) |
| 支持一个文件夹内有嵌套文件夹时多个csv文件导入 | pass | [TD-28663](https://jira.taosdata.com:18080/browse/TD-28663) |
| 支持指定csv文件中的分隔符Delimiter | pass |  |
| 支持使用skipRows参数跳过某些行数据 | pass | [TD-28655](https://jira.taosdata.com:18080/browse/TD-28655) |
| 支持使用quoteChar引用符号 | pass |  |
| 支持使用commentPrefix注释前缀符 | pass |  |
| 空的CSV文件处理 | fail | [TD-28657](https://jira.taosdata.com:18080/browse/TD-28657) |
| 只包含Header的不包含数据的CSV文件处理 | pass | [TD-28646](https://jira.taosdata.com:18080/browse/TD-28646) |
| 查看以及编辑已完成的CSV任务 | pass | [TD-28652](https://jira.taosdata.com:18080/browse/TD-28652) |

## 4. 发现的问题

TD-28641


TD-28642


TD-28646


TD-28652


TD-28655


TD-28657


TD-28659


TD-28660


TD-28663


TD-28674


TD-28675


TD-28701


TD-28699
