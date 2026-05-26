---
name: tsdb-doc-review-record
description: "Generate standardized review records (评审记录) for TDengine TSDB releases. Supports three types: requirements review (需求评审/RS), design review (设计评审/FS), and test report review (测试报告评审/TS). Triggers on: 评审记录, review record, 需求评审, RS评审, 设计评审, FS评审, 测试评审, TS评审, 测试报告评审, write minutes, 编写评审记录"
metadata:
  author: Simon Guan
  version: 1.0.0
  owner_team: engine
---

# Review Record Generator (评审记录生成器)

## 概览

为 TDengine TSDB 版本发布过程中的三类评审生成标准化评审记录：

| 类型 | 目录 | 触发词 |
|---|---|---|
| 需求评审 (RS) | `09-会议纪要和评审记录/04-产品开发/01-需求/` | 需求评审, RS评审, RS review |
| 设计评审 (FS) | `09-会议纪要和评审记录/04-产品开发/02-设计/` | 设计评审, FS评审, FS review |
| 测试报告评审 (TS) | `09-会议纪要和评审记录/04-产品开发/03-功能测试/` | 测试评审, TS评审, 测试报告评审 |

## 触发示例

- "编写 XXX 的设计评审记录"
- "帮我写 XXX 的测试报告评审记录"
- "生成 XXX 的需求评审记录"
- "编写评审记录，参照 YYY"

## 执行流程

1. **确定评审类型**：根据用户指令或被评审文档的路径/类型判断属于 RS/FS/TS 中的哪一种。
2. **读取源文档**：读取被评审的文档全文，提取关键信息。
3. **读取参考模板**（可选）：若用户指定了参照文件，读取该文件作为格式/人员参考。
4. **生成评审记录**：按对应类型的格式规范生成评审记录。
5. **校验链接**：验证评审文档中的相对路径链接指向正确。

## 格式规范

根据评审类型，读取对应的 references 文件获取详细格式规范：

- **需求评审 (RS)**：读取 `references/rs-review.md`
- **设计评审 (FS)**：读取 `references/fs-review.md`
- **测试报告评审 (TS)**：读取 `references/ts-review.md`

## 硬约束

- 文件名格式：`YYYYMMDD {Feature Name} {评审类型}评审记录.md`
- 日期取当天日期（即 `{{current_date}}`）
- 链接必须使用相对路径，空格用 `%20` 编码
- 评审记录内容必须基于源文档实际内容生成，不得编造技术细节
- 评审人员：若用户未指定，参考同目录下最近的评审记录获取人员信息
- 评审结论：除非有明确问题指出，默认为通过
- 后续行动项：默认为 `无`，除非用户明确指出需要行动项

## 通用文档结构

所有三种评审记录共享相同的四级结构：

```markdown
# YYYYMMDD {Feature Name} {评审类型}评审记录

## 1. 评审信息
## 2. 评审记录
## 3. 评审结论
## 4. 后续行动项
```

## 评审信息（通用）

所有类型共享同一 8 字段编号列表格式：

```markdown
1. 评审目的：{根据类型不同而不同}
2. 评审文档：[{文档名}]({相对路径})
3. 会议主持：{name}
4. 会议人员：{comma-separated names}
5. 会议时间：YYYY-MM-DD HH:MM - HH:MM
6. 会议形式：线下 | 线上
7. 会议地点：{location}
8. 会议记录人：{name}
```

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-doc-review-record version=1.0.0 author=Simon Guan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。
