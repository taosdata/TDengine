# TDgpt & TDasset 授权项控制 RS

## 1. 引言

### 1.1 术语与缩写名词

1. 机器码 (machineCode)：根据服务器的 CPU ID/核数，主板或 MAC 信息生成的固定字符串，用于授权时标识某一台固定的服务器。
2. 授权码 (activeCode)：对可授权的部分或所有功能项加密生成的字符串，用于辅助完成对 TDengine 可授权功能项的控制。

### 1.2 相关文档资料

1. JIRA 
   - [TS-6414](https://jira.taosdata.com:18080/browse/TS-6414)
   - [TD-6446](https://jira.taosdata.com:18080/browse/TS-6446)
2. 参考 
   - [TDgpt 授权需求](https://taosdata.feishu.cn/wiki/LA1XwwBKVid17okns2ccd6m5ndh)
   - [TDasset 基本概念](https://taosdata.feishu.cn/wiki/KESPwK2VpiMbGrkmSa8crHoinIc)
   - [按功能授权 RS](https://taosdata.feishu.cn/wiki/SqccwbvkkibacFkvXi9c3TYOn6b)
   - [按功能授权 FS](https://taosdata.feishu.cn/wiki/OydKwSf1jidC04ki9V2c65NvnKe)

### 1.3 优先级要求

高

### 1.4 版本要求

企业版

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/05/6 | 1.0 | 关胜亮 | 新建 |

## 3. 需求目标

1. 支持对 TDasset、TDgpt 进行授权，授权方法与 TDengine 一致
2. 单一授权码控制 TDasset、TDgpt、TDengine、taosX 的基本功能和各项可选功能

## 4. 功能需求

2024 年开发的“License 独立控制”功能（见[需求](https://taosdata.feishu.cn/wiki/SqccwbvkkibacFkvXi9c3TYOn6b)和[实现](https://taosdata.feishu.cn/wiki/OydKwSf1jidC04ki9V2c65NvnKe))中，已经实现“单一授权码控制 TDengine 的基本功能和各项可选功能”。在此基础上，需要做以下开发工作。
1. TDengine 依据细化后的 TDasset、TDgpt 的授权项，更改授权工具(taosGrant)，在 taosd 中新增授权信息，可通过 SQL 读取
2. TDasset、TDgpt 从已经部署的 TDengine 中读取授权信息，在自己的程序中新增控制逻辑
如下为 TDasset、TDgpt 的授权项。在下表中，有如下简称。
1. CT（Create time）：集群创建时间、集群从老版本升级至新版本的时间，取最大值
2. ET（Expire time）：集群进入授权过期状态的时间
3. RT（Revoke time）：集群进入授权回收状态的时间
4. LV（Last value）：集群上次处于已授权状态时各授权项的取值，如从未进入已授权状态，使用未授权状态的值

| 授权项 | 授权子项 | 必选 | 未授权状态取值 | 授权过期状态取值 | 授权回收状态取值 | 授权叠加方法 | 授权失效的行为 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 过期时间 | 必选 | CT+10d | ET | RT+7d | 本次授权取值 | 不能调用 gpt 相关函数 |
| Anode 数量 | 必选 | 1 | LV | LV | 本次授权取值 | 不能新建 anode |
| 过期时间 | 必选 | CT+10d | ET | RT+7d | 本次授权取值 | 不能使用基础功能 |
| 时序数据的属性总数 | 必选 | 1000 | LV | LV | 本次授权取值 | 不能新建属性 |
| 非时序数据的属性总数 | 必选 | 1000 | LV | LV | 本次授权取值 | 不能新建属性 |
| 元素总数 | 必选 | 1000 | LV | LV | 本次授权取值 | 不能新建元素 |
| 服务器数量 | 必选 | 1 | LV | LV | 本次授权取值 | 不能新增节点 |
| 计算机核数 | 必选 | 256 | LV | LV | 本次授权取值 | 不能新增节点 |
| 总的用户数 | 必选 | 1 | LV | LV | 本次授权取值 | 不能新增用户 |
| TDasset - 版本控制 | 过期时间 | 可选 | CT+10d | ET | RT+7d | 本次授权取值 | 待定 |
| TDasset - 时序数据预测 | 过期时间 | 可选 | CT+10d | ET | RT+7d | 本次授权取值 | 待定 |
| TDasset - 时序数据检测 | 过期时间 | 可选 | CT+10d | ET | RT+7d | 本次授权取值 | 待定 |
| TDasset - 数据质量 | 过期时间 | 可选 | CT+10d | ET | RT+7d | 本次授权取值 | 待定 |
| TDasset - AI chat/生成 | 过期时间 | 可选 | CT+10d | ET | RT+7d | 本次授权取值 | 待定 |

## 5. 性能需求

无

## 6. 安全需求

如需考虑授权安全性，需要 taosAdapter 启用 https 选项；对于原生链接，未来会支持 taosc -> taosd 的安全证书

## 7. 其他需求

无
