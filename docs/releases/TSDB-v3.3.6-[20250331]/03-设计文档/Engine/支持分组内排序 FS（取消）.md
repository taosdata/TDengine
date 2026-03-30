# 支持分组内排序 FS（取消）

## 1. 会议信息

会议主题：分组内排序 FS 评审
会议时间：Dec 30 (Mon) 13:32 - 13:35 (GMT+08)
参会人：@潘魏 @关胜亮 @李明军

## 2. 智能纪要

<quote-container>
智能会议纪要由 AI 生成，可能不准确，请谨慎甄别后使用
</quote-container>

<callout emoji="page_facing_up" background-color="light-blue">

### 2.1 **总结**

会议讨论了为数据库添加分组内排序功能的相关事宜，包括语法、应用场景和实现难度等。主要内容包括：
- **功能缺失现状**：当前数据库不支持组内排序功能，包括 TS 里写的投影查询等所有排序都无此功能，致使用户无法控制组内输出顺序，尤其在利用 limit 限制组内输出条数时无法实现。
- **功能添加计划**：此次打算将分组内排序作为通用功能加上，因有相关需求。
- **语法相关疑问**：探讨了 partition by 加括号与不加括号在语法和功能上的区别，加括号为组内排序，不加则为全局排序，此语法参考标准的 over 语法。
- **功能实现难度**：功能描述简单但实现复杂，工作量大。
- **后续工作安排**：确定使用相关语法关键字，李明军负责写群并举例，潘魏负责确认并再举几个例子。
</callout>

<add-ons component-id="" component-type-id="blk_605344f606400001a416289a" record="{"bizType":"ai_notes","docTenantID":"6899306859979718657","extra":"","meetingID":"7454072952176902172","sourceChapter":"","sourceSummary":"3","sourceTodo":""}"/>

## 3. 会议议程

## 4. 会议信息

会议主题：分组内排序 FS 评审
会议时间：Dec 30 (Mon) 13:29 - 13:31 (GMT+08)
参会人：@潘魏 @关胜亮 @李明军

## 5. 会议议程

## 6. 背景

https://jira.taosdata.com:18080/browse/TS-5452?src=confmacro
目前 TDengine 查询不支持分组内排序，因此用户没有手段控制每个分组内的输出顺序，尤其是需要通过 limit 限制每个分组的输出记录条数的场景。

## 7. 变更历史


| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/12/26 | 0.1 | 潘魏 | 初始版本 |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 8. 定义

文档中所使用的一些并非众所周知的术语的定义。

## 9. 行为说明

本次拟通过支持分组内排序的功能来更好的满足用户需求，具体行为变更在下面描述。

### 9.1 功能说明

- 分组内排序是指每个分组内按照指定的一个或多个列进行输出排序，分组包括 GROUP BY 和 PARTITION BY 产生的分组，排序列的支持范围与全局排序相同。
- 当存在全局排序时，最终输出排序以全局排序为准，每个分组内的排序影响聚合函数的结果。
- 拟通过支持窗口函数的方式来支持分组内排序，本期只实现基础功能，不支持新的排序函数，不支持 WINDOW 子句。
- OVER 子句只支持聚合函数、分组列、常量及其运算，不支持投影、选择等其它函数及运算，暂不支持与时序窗口混用。
- OVER 子句只能用在 SELECT 和 ORDER BY 子句中，不支持嵌套。
- 聚合函数支持范围：

### 9.2 语法定义

```sql {wrap}
SELECT select_list FROM ... [WHERE ...] ... [window_clause] [order_by_clasue] ...

select_list:
    select_expr [, select_expr] ...

select_expr: {
    *
  | query_name.*
  | [schema_name.] {table_name | view_name} .*
  | t_alias.*
  | expr [[AS] c_alias]
  | over_clause [[AS] c_alias]
} 

over_clause:
    agg_func OVER ([sorder_by_clasue])  

sorder_by_clasue:
    ORDER BY sorder_expr [, sorder_expr] ...

sorder_expr:
    {expr | position | c_alias} [DESC | ASC] [NULLS FIRST | NULLS LAST] 
    
order_by_clasue:
    ORDER BY order_expr [, order_expr] ...    

order_expr:
    {expr | position | c_alias | over_clause} [DESC | ASC] [NULLS FIRST | NULLS LAST]     
```

## 10. 性能

无。

## 11. 兼容性

无。

## 12. 运维

无。

## 13. 使用场景

分组内聚合查询排序（待举例）
分组内排序被忽略场景（待举例）

## 14. 约束和限制

无。

## 15. 文档

不需要修改企业版文档
需要修改官网文档

## 16. 参考文档

无
