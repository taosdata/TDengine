# taosadapter 新技术引入评估申请表

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-04 | 2026-01-04 | 1.0 | 谭雪峰 | 编写文档 |

## 2. 基础信息

### 2.1 文件变更监控

| 技术名称 | github.com/fsnotify/fsnotify |
| --- | --- |
| 技术类型 | 库 |
| 提案部门 | 研发部门 |
| 申请人 | 谭雪峰 |
| 计划应用的组件/产品/场景 | taosadapter 用于监控文件变动 |

### 2.2 strptime 格式解析

| 技术名称 | github.com/ncruces/go-strftime |
| --- | --- |
| 技术类型 | 库 |
| 提案部门 | 研发部门 |
| 申请人 | 谭雪峰 |
| 计划应用的组件/产品/场景 | taosadapter json 写入支持 `strptime` 时间格式解析 |

### 2.3 strptime 格式解析

| 技术名称 | github.com/blues/jsonata-go github.com/bytedance/sonic github.com/minio/simdjson-go github.com/tidwall/gjson |
| --- | --- |
| 技术类型 | 库 |
| 提案部门 | 研发部门 |
| 申请人 | 谭雪峰 |
| 计划应用的组件/产品/场景 | taosadapter 测试使用 json 解析库，用于对比标准库解析 |

## 3. 引入理由

1. 业务/技术价值：
   - fsnotify 用于监控配置文件变更实现配置文件修改的自动更新
   - go-strftime 用于 json 写入时扩充时间格式支持范围
   - 多种 json 解析库引入用于对比标准库的解析结果
2. 与现有技术方案对比优势：
   - 相比其他文件监控方案，fsnotify 具有以下优势：跨平台支持，一次编码多平台运行；高性能与低延迟，基于操作系统原生 API 实现，避免轮询带来的性能开销
   - 相比 Go 标准库的 time 包，go-strftime 提供了与 C 语言 strftime 兼容的格式化方案，对于熟悉 strftime 占位符的开发者更加友好
3. 初步选型调研结论：
   - fsnotify 是 Go 语言生态中文件系统监控的标杆库，被 Gin 框架、Hugo 静态网站生成器等知名项目广泛使用，建议在需要跨平台文件监控的场景下采用 fsnotify
   - go-strftime 是处理日期时间格式化需求的理想选择，特别适合需要与 strftime 兼容格式的场景。对于已经熟悉 strftime 占位符的开发者，该库可以降低学习成本。建议在需要高性能时间格式化且希望使用 strftime 格式的场景下采用 go-strftime。

## 4. 技术评估自评

1. 技术/社区健康度自评分：
   - fsnotify 8/10
  fsnotify 项目在 GitHub 上拥有较高的关注度和星标数量，社区活跃度较高。
   - go-strftime 7/10
  项目在 GitHub 上有持续的工作流运行记录，显示项目处于活跃维护状态
1. 安全特性自评分：
   - fsnotify 7/10
  fsnotify 基于各操作系统原生文件监控机制实现，安全性较高。但需要注意历史漏洞.
   - go-strftime 9/10
  go-strftime 作为纯 Go 实现的日期时间格式化库，不依赖外部 C 库，减少了潜在的安全风险。目前未发现已知的安全漏洞，目前版本测试覆盖率 100%
1. 合规性自评结论：
   - fsnotify 采用 BSD 开源许可证，遵循开源协议要求
   - go-strftime 采用 MIT 开源许可证，遵循开源协议要求
2. 集成成本分析：
   - fsnotify 集成成本低，通过 `go get ``github.com/fsnotify/fsnotify`即可安装。API 设计简洁
   - go-strftime 集成成本低，通过 `go get ``github.com/lestrrat-go/strftime`即可安装。API 设计简洁

## 5. 安全团队评估意见

1. 安全评估得分：90
2. 主要风险点：
3. 建议（批准/附带条件批准/否决）：批准

## 6. 审批意见

1. 技术总监审批：批准
2. 安全负责人审批：批准
3. CTO审批：批准
