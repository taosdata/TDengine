# 位持续时长函数 RS

## 1. 引言

### 1.1 术语与缩写名词

### 1.2 相关文档资料

参见 [原始需求](https://taosdata.feishu.cn/wiki/I35VwVN8LiIZhXkLMRhcMG8tnPh)
JIRA [TS-6487](https://jira.taosdata.com:18080/browse/TS-6487)

### 1.3 优先级要求

中

### 1.4 版本要求

企业版和社区版都支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/05/10 | 1.0 | 关胜亮 | 新建 |

## 3. 需求目标

计算某个测点的合格率，统计二进制位变化的持续时长

## 4. 功能需求

1. 功能说明
统计表中特定列与之前行的特定列的二进制位的变化持续时长。统计查询范围内的所有行，对于<text color="red">第一行数据，比较时需要向前查找记录；如果未找到任何记录，认为第一行状态从查询范围的 tstart 开始持续</text>。语法参考
```plaintext
bit_duration(expr[, mask, flag])
```

1. mask 说明
   -  取值范围为 0-INT64_MAX（注意 NULL 对可用 mask 的影响）
   - 默认为 1，表示仅比较第一位
2. flag 说明 
   - flag=0，表示统计从 `非0到非0`, `非0到0` 的持续时长，默认值为 0
   - flag=1，表示统计从 `0到0`，`0到非0` 的持续时长
   - flag=2，表示统计第一条记录到最后一条记录的时长
3. 适用数据类型：全部整型字段，包括 bigint、int、smallint、tinyint、bool
4. 返回数据类型：BIGINT
5. 使用限制
   - 适用于表、超级表
   - 支持窗口查询
6. NULL、None 值处理
   - 忽略 None，<text color="red">跳过该行</text>
   - 忽略 NULL，<text color="red">跳过该行</text>

## 5. 性能需求

无

## 6. 安全需求

无

## 7. 其他需求

无
