# 字符串标签支持指定多个值 RS

## 1. 引言

### 1.1 术语与缩写名词

字符串类型的标签：指 varchar、nchar 类型

### 1.2 相关文档资料

JIRA [TS-7127](https://jira.taosdata.com:18080/browse/TS-7127)

### 1.3 优先级要求

高：需求来自 Jeff，为 IDMP 服务

### 1.4 版本要求

企业版和社区版都支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/08/25 | 1.0 | 关胜亮 | 新建，见 [第一版](https://taosdata.feishu.cn/wiki/OYN9w90MviDqI1kpskecupB4nxs?edition_id=3ODRdB&ccm_open_type=from_version_link) 链接 |
| 2025/08/25 | 1.1 | 关胜亮 | 需求简化 |
| 2025/08/25 | 1.2 | 关胜亮 | 增加方案二 |

## 3. 需求目标

支持为子表的字符串类型标签指定多个值，并按顺序匹配过滤。

## 4. 功能需求

使用特殊字符（串）作为区分多个标签值的间隔，该特殊字符（串）为两个连续分号 `;;`。
1. 单值标签：不存在特殊字符串的标签值，运算符、函数、索引的行为皆不变
2. 多值标签：存在特殊字符串的标签值

### 4.1 方案一

对于多值标签，其行为变化如下
1. 运算符
   - `like`、`=`：以 `"branch1;;branch2;;branch3"` 为例，执行 `WHERE location = 'branch2'` 时
      - 按顺序扫描值列表（`branch1`→ `branch2`→ `branch3`）
      - 匹配即短路返回​（命中`branch2`即停止扫描）
      - 若全不匹配，则跳过
   - `<>` 、`!=`、`not like`：相当于 `like`、`=` 的取反操作，同样考虑多值标签
   - 其他运算符，不考虑多值标签，当做单一字符串处理，例如 `>` `<` `>=` `<=` `is [not] null` `in` `match` `nmatch` `regexp` `not regexp`
2. 函数：无行为变化
3. 索引：使用多值标签中的每个取值，构建索引

### 4.2 方案二

搜索了 PG、MySQL、SQL Server 解决类似问题的方法，MySQL 的函数 FIND_IN_SET 更加适用这个场景
**语法**：
```sql {wrap}
FIND_IN_SET(search_str, strlist)
```

**参数**：
- `search_str`：要查找的字符串。
- `strlist`：逗号分隔的字符串（如 `'a,b,c'`）。
**返回值​：**
- 匹配时返回位置（从 `1` 开始），未匹配返回 `0`，参数为 `NULL` 时返回 `NULL`。
**示例**​：
```sql {wrap}
SELECT FIND_IN_SET('b', 'a,b,c,d'); -- 返回 2（'b' 在第二个位置）
```

**建议：**
- 实现函数 `FIND_IN_SET`，和标准 SQL 实现一致，避免了打补丁方式存在的性能、运算符歧义问题。
- 对函数 `FIND_IN_SET` 进行扩展，增加第三个参数，用来自定义分隔符。
```sql {wrap}
FIND_IN_SET(search_str, strlist, split_str)
-- split_str 默认值为逗号（,），在 IDMP 中可以将 split_str 设置为分号（;）或连续分号（;;）

select * from supertable where Find_IN_set(taga, 'location1') > 0;
select * from supertable where taga ='location1';
```

## 5. 性能需求

### 5.1 方案一

每次匹配，都需要查看是否包含特殊字符，性能会有下降。因此，对字符类型的 `=`、`<>`、`!=`、`like`、`not like` 的性能影响，请给出一个典型场景的下降比例。

### 5.2 方案二

无

## 6. 安全需求

无

## 7. 其他需求

无
