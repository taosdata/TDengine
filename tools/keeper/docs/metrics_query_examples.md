# TDengine Write Metrics 查询示例

本文档展示如何通过多种方式查询 TDengine 写入指标数据。

## 1. 通过 TaosKeeper HTTP API 查询

### 1.1 时序数据查询
```bash
# 查询特定 VGroup 的时序数据
curl "http://localhost:6043/metrics/query?vgroup_id=1&start_time=2024-01-01T00:00:00Z&end_time=2024-01-01T23:59:59Z&interval=5m"

# 查询所有 VGroup 的最近1小时数据
curl "http://localhost:6043/metrics/query?start_time=2024-01-01T10:00:00Z&interval=1m&limit=60"
```

### 1.2 汇总数据查询  
```bash
# 获取最近1小时的汇总指标
curl "http://localhost:6043/metrics/summary?time_range=1h"

# 获取最近24小时的汇总指标
curl "http://localhost:6043/metrics/summary?time_range=24h"
```

### 1.3 VGroup 列表查询
```bash
# 获取所有 VGroup 信息
curl "http://localhost:6043/metrics/vgroups"
```

## 2. 通过 TDengine SQL 查询

### 2.1 基础查询

```sql
-- 查询最新的 100 条 metrics 记录
SELECT * FROM log.write_metrics ORDER BY ts DESC LIMIT 100;

-- 查询特定 VGroup 的数据
SELECT * FROM log.write_metrics WHERE vgroup_id = 1 ORDER BY ts DESC LIMIT 50;

-- 查询最近1小时的数据
SELECT * FROM log.write_metrics WHERE ts >= now - 1h ORDER BY ts DESC;
```

### 2.2 聚合查询

```sql
-- 按 VGroup 分组的汇总统计
SELECT 
    vgroup_id,
    max(total_requests) as max_total_requests,
    max(total_rows) as max_total_rows,
    avg(avg_write_size) as avg_write_size,
    avg(cache_hit_ratio) as avg_cache_hit_ratio,
    max(memory_table_rows) as max_memory_table_rows,
    avg(avg_commit_time) as avg_commit_time
FROM log.write_metrics 
WHERE ts >= now - 1h 
GROUP BY vgroup_id 
ORDER BY vgroup_id;

-- 最近24小时每小时的写入趋势
SELECT 
    _wstart as hour,
    sum(total_requests) as total_requests,
    sum(total_rows) as total_rows,
    sum(total_bytes) as total_bytes,
    avg(cache_hit_ratio) as avg_cache_hit_ratio
FROM log.write_metrics 
WHERE ts >= now - 24h 
INTERVAL(1h) 
ORDER BY hour;
```

### 2.3 性能分析查询

```sql
-- 查找提交时间最高的 VGroup
SELECT 
    vgroup_id,
    avg(avg_commit_time) as avg_commit_time,
    max(avg_commit_time) as max_commit_time,
    count(*) as sample_count
FROM log.write_metrics 
WHERE ts >= now - 1h AND avg_commit_time > 0
GROUP BY vgroup_id 
ORDER BY avg_commit_time DESC 
LIMIT 10;

-- 查找缓存命中率最低的 VGroup
SELECT 
    vgroup_id,
    avg(cache_hit_ratio) as avg_cache_hit_ratio,
    min(cache_hit_ratio) as min_cache_hit_ratio,
    count(*) as sample_count
FROM log.write_metrics 
WHERE ts >= now - 1h 
GROUP BY vgroup_id 
ORDER BY avg_cache_hit_ratio ASC 
LIMIT 10;

-- 内存使用量分析
SELECT 
    vgroup_id,
    max(memory_table_rows) as max_memory_rows,
    max(memory_table_size) as max_memory_size,
    avg(memory_table_rows) as avg_memory_rows
FROM log.write_metrics 
WHERE ts >= now - 1h 
GROUP BY vgroup_id 
ORDER BY max_memory_size DESC;
```

### 2.4 WAL 和同步分析

```sql
-- WAL 写入性能分析
SELECT 
    vgroup_id,
    sum(wal_write_bytes) as total_wal_bytes,
    sum(wal_write_time) as total_wal_time,
    case when sum(wal_write_time) > 0 
         then sum(wal_write_bytes) * 1000.0 / sum(wal_write_time) 
         else 0 end as wal_throughput_bytes_per_ms
FROM log.write_metrics 
WHERE ts >= now - 1h 
GROUP BY vgroup_id 
ORDER BY wal_throughput_bytes_per_ms DESC;

-- 同步性能分析  
SELECT 
    vgroup_id,
    sum(sync_bytes) as total_sync_bytes,
    sum(sync_time) as total_sync_time,
    sum(apply_bytes) as total_apply_bytes,
    sum(apply_time) as total_apply_time,
    case when sum(sync_time) > 0 
         then sum(sync_bytes) * 1000.0 / sum(sync_time) 
         else 0 end as sync_throughput_bytes_per_ms
FROM log.write_metrics 
WHERE ts >= now - 1h 
GROUP BY vgroup_id 
ORDER BY sync_throughput_bytes_per_ms DESC;
```

### 2.5 异常检测查询

```sql
-- 检测阻塞的提交
SELECT 
    ts,
    vgroup_id,
    blocked_commits,
    commit_count,
    case when commit_count > 0 
         then blocked_commits * 100.0 / commit_count 
         else 0 end as blocked_ratio
FROM log.write_metrics 
WHERE ts >= now - 1h AND blocked_commits > 0 
ORDER BY blocked_ratio DESC;

-- 检测异常的写入大小
SELECT 
    ts,
    vgroup_id,
    avg_write_size,
    total_bytes,
    total_rows
FROM log.write_metrics 
WHERE ts >= now - 1h 
  AND (avg_write_size > 10000 OR avg_write_size < 10)
ORDER BY ts DESC;
```

## 3. 监控告警查询

### 3.1 性能告警

```sql
-- 提交时间过长告警 (>100ms)
SELECT 
    vgroup_id,
    avg(avg_commit_time) as avg_commit_time
FROM log.write_metrics 
WHERE ts >= now - 5m 
GROUP BY vgroup_id 
HAVING avg(avg_commit_time) > 100;

-- 缓存命中率过低告警 (<80%)
SELECT 
    vgroup_id,
    avg(cache_hit_ratio) as avg_cache_hit_ratio
FROM log.write_metrics 
WHERE ts >= now - 5m 
GROUP BY vgroup_id 
HAVING avg(cache_hit_ratio) < 0.8;
```

### 3.2 资源告警

```sql
-- 内存表行数过多告警 (>1000000)
SELECT 
    vgroup_id,
    max(memory_table_rows) as max_memory_rows
FROM log.write_metrics 
WHERE ts >= now - 5m 
GROUP BY vgroup_id 
HAVING max(memory_table_rows) > 1000000;
```

## 4. 常用查询模板

### 4.1 实时监控仪表板查询

```sql
-- 实时写入速率 (最近1分钟)
SELECT 
    _wstart,
    sum(total_requests) as requests_per_min,
    sum(total_rows) as rows_per_min,
    sum(total_bytes) as bytes_per_min
FROM log.write_metrics 
WHERE ts >= now - 5m 
INTERVAL(1m) 
ORDER BY _wstart DESC 
LIMIT 5;

-- VGroup 状态概览
SELECT 
    vgroup_id,
    count(*) as records,
    avg(cache_hit_ratio) as cache_hit_ratio,
    avg(avg_commit_time) as avg_commit_time,
    max(memory_table_rows) as memory_rows
FROM log.write_metrics 
WHERE ts >= now - 10m 
GROUP BY vgroup_id 
ORDER BY vgroup_id;
```

### 4.2 历史趋势分析

```sql
-- 每日写入趋势 (最近7天)
SELECT 
    to_date(_wstart) as date,
    sum(total_requests) as daily_requests,
    sum(total_rows) as daily_rows,
    sum(total_bytes) as daily_bytes
FROM log.write_metrics 
WHERE ts >= now - 7d 
INTERVAL(1d) 
ORDER BY date;

-- 小时级性能趋势
SELECT 
    _wstart as hour,
    avg(avg_commit_time) as avg_commit_time,
    avg(cache_hit_ratio) as cache_hit_ratio,
    max(memory_table_rows) as max_memory_rows
FROM log.write_metrics 
WHERE ts >= now - 24h 
INTERVAL(1h) 
ORDER BY hour;
```

## 5. 响应格式

### 5.1 HTTP API 响应格式

```json
{
  "code": 0,
  "message": "success", 
  "data": [
    ["2024-01-01T10:00:00Z", 1, 1000, 50000, 2048000, 40.96, 0.85, 25000, 10, 15.5],
    ["2024-01-01T10:01:00Z", 1, 1050, 52500, 2150400, 40.96, 0.87, 26000, 12, 14.2]
  ],
  "rows": 2
}
```

### 5.2 SQL 查询结果格式

```
ts                     | vgroup_id | total_requests | total_rows | cache_hit_ratio | avg_commit_time
2024-01-01 10:00:00.000|    1      |     1000       |   50000    |      0.85       |     15.5
2024-01-01 10:01:00.000|    1      |     1050       |   52500    |      0.87       |     14.2
```

## 6. 性能优化建议

1. **索引使用**: 查询时优先使用 `ts` 和 `vgroup_id` 作为过滤条件
2. **时间范围**: 限制查询的时间范围以提高性能
3. **聚合查询**: 使用 `INTERVAL` 进行时间窗口聚合
4. **分页**: 对大结果集使用 `LIMIT` 和 `OFFSET`
5. **缓存**: 对频繁查询的结果进行缓存 