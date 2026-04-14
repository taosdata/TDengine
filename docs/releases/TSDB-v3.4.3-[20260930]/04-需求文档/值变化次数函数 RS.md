# 值变化次数函数 RS

## 1. 引言

### 1.1 术语与缩写名词

### 1.2 相关文档资料

参见 [原始需求](https://taosdata.feishu.cn/wiki/I35VwVN8LiIZhXkLMRhcMG8tnPh)
JIRA [TS-6485](https://jira.taosdata.com:18080/browse/TS-6485)

### 1.3 优先级要求

中

### 1.4 版本要求

企业版和社区版都支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/04/28 | 1.0 | 关胜亮 | 新建 |

## 3. 需求目标

计算某个测点的取值的变化次数

## 4. 功能需求

1. 功能说明
统计表中特定列与之前行的特定列的变化，该行变化则计数为 1，否则计数为 0， 输出计数累加值。统计查询范围内的所有行，<text color="red">排除第一行</text>。语法参考
```plaintext
value_change(expr, ignore_null)
```

类似于
```plaintext
select count(*) from (
    select diff(val) diff_val from tb
) where diff_val != 0
```

1. 参数说明
   - expr：
   - ignore_null：默认值为 1，表示忽略 NULL 值，为 0 时，当 NULL 和非 NULL 值比较时，认为发生了变化
2. 适用数据类型：全部类型字段
3. 返回数据类型：BIGINT
4. 使用限制
   - 适用于表、超级表
   - 支持窗口查询
5. NULL、None 值处理
   - 忽略 None，<text color="red">跳过该行</text>
   - 处理 NULL，<text color="red">NULL 和非 NULL 值比较时，认为发生了变化</text>

## 5. 性能需求

无

## 6. 安全需求

无

## 7. 其他需求

无
