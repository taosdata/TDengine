# MQTT ClientID 和 Kafka GroupID 生成规则优化 - Test Spec

## 1. 测试目标

- 确认MQTT ClientID 和 Kafka GroupID 生成规则优化变动符合FS [MQTT ClientID 和 Kafka GroupID 生成规则优化](https://taosdata.feishu.cn/wiki/WAm0wjCR9ikrZYkwwKDcGCi2nkc)
- 确认该新功能开发没有影响到现有功能

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-7-24 | 0.1 | 智勇 | 初稿 |
|  |  |  |  |

## 3. 测试范围

- MQTT任务的增删改查看，以及对旧版本任务的兼容
- Kafka 任务的增删改查看，以及对旧版本任务的兼容

## 4. 测试结论

测试通过。

## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 1 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

旧版本的 kafka 任务没有 client id，在新版本上进行编辑时展示的内容是 placeholder

## 7. 测试环境

- OS: Linux
- Browser: Chrome

## 8. 测试数据 (Optional)

无

## 9. 测试用例

### 9.1 功能

在提测时，开发应保证基础用例全部通过。

#### 9.1.1 MQTT ClientID

| 分类 | 测试步骤 | 预期结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 创建任务页面 | 检查页面Client ID项 | 值包括：文本taox、用户输入user_input编辑框、是否填充task_id switch | ok |  |
|  | 检查user_input默认值 | 空 | ok |  |
|  | 检查switch默认状态 | 关闭 | ok |  |
|  | 检查switch hover提示 | “是否将当前任务的 ID 填充到 Client ID 中” | ok |  |
|  | 检查switch hover提示 英文 | “Whether to fill the Client ID with the current task ID” | ok |  |
|  | 打开switch | 任务创建成功后将task_id添加到taosx后面 | ok |  |
|  | 关闭switch | Client ID不包括task_id | ok |  |
| 创建任务 | Client ID 中task_id switch打开，user_input输入mqtt1，选择已存在的主题，其他选项默认 | 创建成功，创建的任务信息与输入保持一致，生成的Client ID为 taosx{task_id}mqtt1 | ok |  |
|  | Client ID 中task_id switch关闭，user_input输入mqtt2，选择已存在的主题，其他选项默认 | 创建成功，创建的任务信息与输入保持一致，生成的Client ID为 taosxmqtt2 | ok |  |
|  | task_id switch关闭，user_input输入纯数字提交，如123456789012345678 | 生成的Client ID为 taosx123456789012345678 | ok |  |
|  | task_id switch关闭，user_input输入纯字母提交，如abcdefghijklmnopqr | 生成的Client ID为 taosxabcdefghijklmnopqr | ok |  |
| 创建异常流程 | 不填写user_input进行提交 | 给出相应提示，比如user_input必须输入 | ok |  |
|  | ~~user_input输入非数字字母字符进行提交，如~!@#，特别是下划线~~ | ~~给出相应提示，比如user_input输入不合法~~ |  |  |
|  | ~~user_input输入的数字字符长度超过23个，例如12345678901234567890abcd~~ | ~~给出相应提示，比如user_input超长~~ |  |  |
|  |  |  |  |  |
| 复制任务页面 | 参考 *创建任务页面* |  | ok |  |
| 复制任务 | 从现有的任务中复制任务 | Client ID 置空，其他信息从来源任务中复制 | ok |  |
|  | 其他用例参考 *创建任务 创建异常流程* |  | ok |  |
|  |  |  |  |  |
| 修改任务页面 | 检查各项数据 | 各项数据均带入到修改页面，且展示正确 | ok |  |
|  | 修改mqtt地址 | mqtt地址置灰不可修改 | ok |  |
|  | 修改mqtt端口 | mqtt端口置灰不可修改 | ok |  |
|  | 修改Client ID | Client ID 置灰不可修改 | ok |  |
|  | 修改订阅主题 | 订阅主题置灰不可修改 | ok |  |
|  | 修改除以上配置以外的其他配置 | 成功修改并保存，数据库中该配置数据更新 | ok |  |
|  |  |  |  |  |
| 任务详情页面 | 检查Client ID | Client ID 正确展示 | ok |  |
|  | 检查其他配置 | 其他配置正确展示 | ok |  |
|  |  |  |  |  |
| 兼容 | 1. 选择老任务进行修改 1. 提交修改 | 1. 修改页面信息展示正确，Client ID 为老任务的Client ID 1. 修改信息正确保存 | ok |  |
|  | 1. 选择老任务进行复制 1. 填写Client ID user_input后提交 | 1. Client ID 包括文本taosx、用户输入user_input编辑框，且置空，其他信息复制自老任务 1. 保存成功 | ok | 首次获取数据报错，不能重现 |
|  | 1. 选择老任务进行复制 1. 不填写Client ID user_input提交 | 1. Client ID 包括文本taosx、用户输入user_input编辑框，且置空，其他信息复制自老任务 1. 提交失败，返回对应的提示 | ok |  |
|  | 检查老任务的执行 | 正确执行 | ok |  |

#### 9.1.2 Kafka GroupID

| 分类 | 测试步骤 | 预期结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 创建任务页面 | 检查页面Group ID项 | 值包括：文本taox、用户输入user_input编辑框、是否填充task_id switch | ok |  |
|  | 检查页面Client ID项 | 值包括：文本taox、用户输入user_input编辑框、是否填充task_id switch | ok |  |
|  | 其他case参见*MQTT ClientID 创建任务页面 创建任务 创建异常流程 复制任务页面 复制任务* |  | ok |  |
|  |  |  |  |  |
| 修改任务页面 | 检查各项数据 | 各项数据均带入到修改页面，且展示正确 | ok |  |
|  | 修改连接配置 | 连接配置置灰不可修改 | ok |  |
|  | 修改主题 | 主题置灰不可修改 | ok |  |
|  | 修改Group ID | Group ID置灰不可修改 | ok |  |
|  | 修改除以上配置以外的其他配置 | 成功修改并保存，数据库中该配置数据更新 | ok |  |
|  |  |  |  |  |
| 任务详情页面 | 检查Group ID | Group ID正确展示 | ok |  |
|  | 检查其他配置 | 其他配置正确展示 | ok |  |
|  | 检查Group ID中英文表述 |  | ok |  |
|  |  |  |  |  |
| 兼容 | 1. 选择老任务进行修改 1. 填写Group ID 后提交 | 1. 修改页面信息展示正确，Group ID展示当前任务 id 1. 修改信息正确保存 | ok |  |
|  | 1. ~~选择老任务进行修改~~ 1. ~~不填写Group ID 提交~~ | 1. ~~ ~~ 1. ~~提交失败，返回对应的提示~~ |  |  |
|  | 查看老任务配置 | 跟修改页面保持一致 | ok |  |
|  | 1. 选择老任务进行复制 1. 填写Group ID user_input后提交 | 1. Group ID 包括文本taosx、用户输入user_input编辑框，且置空，其他信息复制自老任务 1. 保存成功 | ok |  |
|  | 1. 选择老任务进行复制 1. 不填写Group ID user_input提交 | 1. Group ID 包括文本taosx、用户输入user_input编辑框，且置空，其他信息复制自老任务 1. 提交失败，返回对应的提示 | ok |  |
|  | 检查老任务的执行 | 正确执行 | ok |  |

### 9.2 可用性

无

### 9.3 可靠性

无

### 9.4 性能

无

### 9.5 安全性

无

### 9.6 兼容性

升级安装后，老版本（上一个版本）下创建的任务，能否正确展示及执行，详见 9.1 兼容用例。

### 9.7 本地化

点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示

## 10. 待讨论(Optional)

无

## 11. Jira

TD-31354

## 12. 测试计划 (Optional)

## 13. 风险评估

无

## 14. 测试备忘 (Optional)

无

## 15. 参考文档 (Optional)

[MQTT ClientID 和 Kafka GroupID 生成规则优化](https://taosdata.feishu.cn/wiki/WAm0wjCR9ikrZYkwwKDcGCi2nkc)
