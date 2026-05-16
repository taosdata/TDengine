# Transformer 支持 UDT 和动态更新 Test Spec

## 1. 测试目标

- 用户可以通过编写 rhai 脚本的方式，自定义对数据的解析
- 用户自定义的 Parser 可以通过调用 API 的方式动态更新

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.05.24 | 0.1 | @王旭 | 初稿 |
|  |  |  |  |

## 3. 测试范围

- Explorer: 可以上传、编辑、修改、查看自定义的 Parser
- API: 可以在不停止任务的前提下，动态更新自定义 Parser

## 4. 测试结论

测试结论中包含结论和关键数据，但不需罗列过多细节，此处需要把把握信息的详细程度，原则上是外部 Reviewer 能够获得清晰的测试结论且尽量没有冗余信息为标准（这个标准是一句正确的废话，具体实行中需要大家 case by case 来处理）

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

## 8. 测试数据

## 9. 测试用例

### 9.1 功能

提测时，请确保高亮显示的基础用例执行通过。
| 类型 | 测试目的 | 测试步骤 | 预期结果 | 基础用例自测 | 测试结果 | 备忘 |
| --- | --- | --- | --- | --- | --- | --- |
| 基本功能 | 上传正确的 UDT 文件后，数据可按照预期同步 |  | 1. 文件上传成功
1. 文件内容可在编辑窗口展示
2. 数据预览正确
3. 数据同步正确 |  | Pass |  |
|  | 可在 UDT 编辑窗口直接录入 UDT |  |  |  | Pass |  |
|  | 可在 UDT 编辑窗口编辑 UDT |  |  |  | Pass |  |
|  | 在 UDT 编辑窗口点击预览按钮后，可正确预览数据 |  |  |  | Pass |  |
|  | 编辑时，使用的 parser 可由 json 切换至 udt |  |  |  | Pass |  |
|  | 查看任务配置时，可查看当前使用的 UDT |  |  |  | Pass |  |
|  | 当 UDT 输出的 JSON array 中包含单条数据时，可正确处理 |  |  |  | Pass |  |
|  | 当 UDT 输出的 JSON array 中包含多条数据时，可正确处理 |  |  |  | Pass |  |
|  | 当 UDT 输出的 JSON array 为空时，可正确处理 |  |  |  | Pass |  |
|  | 可以在 UDT 中添加过滤条件 |  |  |  | Pass |  |
|  | UDT 中的过滤条件与 UI 上”3.过滤”部分的过滤条件可以组合使用 |  |  |  | Pass |  |
| 语法检查 | 上传有语法错误的 UDT 文件后，有错误提示 |  |  |  | Pass |  |
|  | 在 UDT 编辑窗口，直接输入有语法错误的 UDT，有错误提示 |  |  |  | Pass |  |
|  | 当 UDT 的输出非 JSON array 时，有错误提示 |  |  |  | Pass | Parse error for field `value`: Output type incorrect: i64 (expecting array) |
|  | UDT 脚本可支持注释行 |  |  |  | Pass |  |
|  | 脚本错误，无法结束 |  |  |  |  |  |
| API | 调用 API 可对 UDT 进行动态添加 |  | 接口调用成功
UDT 立即生效
任务执行未受影响 |  |  |  |
|  | 调用 API 可对 UDT 进行动态修改 |  |  |  |  |  |
|  | 调用 API 时没有认证，则返回 401 错误 | 调用 API 时，在 HTTP header 中没有传秘钥 |  |  |  |  |
|  | 调用 API 时，传入 不存在的 task id |  |  |  |  |  |
|  | 调用 API 时，传入有语法错误的 UDT |  | 返回状态码 400 |  |  |  |

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

### 9.7 本地化

- 对 UI 的改动，应支持中英文

## 10. 待讨论(Optional)

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: udt
<!-- Unsupported block type: 999 -->

## 12. 测试计划 (Optional)

## 13. 测试备忘

可以在这里调试 rhai 脚本：https://rhai.rs/playground/stable/
```rust {wrap}
let data = #{
    DATA_ITEM_ID: "aaa-0123456",
    MONITOR_OBJ_TYPE: "bbb",
    MONITOR_OBJ_CODE: "ccc",
    PRO_MGT_ORG_CODE: "hebei",
    MGT_ORG_CODE: "ddd",
    PUSH_DATE: "2024-3-20 12:23:30",
    U2358: "223",
    U2359: "219",
    PHASE_FLAG: "1",
    DATA_POINT_FLAG: "3",
    DATA_DATE: "2024-3-20",
    CMD_TYPE: "eee",
    PRODUCT_CODE: "fff",
    DEV_ID:"xxx-1",
    TERMINAL_ID:"zzz"
};

if (!data["DEV_ID"].starts_with("xxx")) {
    return []
}

let result = [];
let share_data = #{};

for (k, i) in data.keys() {
    if (k.len == 5 && (k.starts_with("U0") || k.starts_with("U1") || k.starts_with("U2"))) {
        let ymd = data["DATA_DATE"].split('-');
        if ymd[1].len == 1 { ymd[1] = "0" + ymd[1] } 
        if ymd[2].len == 1 { ymd[2] = "0" + ymd[2] } 
        let item = #{"_ts": `${ymd[0]}-${ymd[1]}-${ymd[2]} ${k.sub_string(1,2)}:${k.sub_string(3,2)}:00`, "_value": data[k]};
        result.push(item);
    } else if (k != "DATA_DATE") {
        share_data.set(k, data[k]);
    }
}

for (item, i) in result {
    result[i] += share_data;
    print(result[i]);
}

reuslt
// print(result);
```


## 14. 参考文档

这里用于添加对该需求测试有帮助的文档链接：
- [Transformer parser 支持 UDT 和动态更新](https://taosdata.feishu.cn/wiki/DgzewP9SLiT0hYkzA0GcyWR9nPg)
- https://rhai.rs/book/
