# Bug: FULL JOIN timetruncate(ts,1d) = timetruncate(ts,1d,0) returns 16 instead of 8

## 现象
```sql
select sta1.ts, sta2.ts from sta1 full join sta2
on timetruncate(sta1.ts, 1d) = timetruncate(sta2.ts, 1d, 0)
order by timetruncate(sta1.ts, 1s) desc, timetruncate(sta2.ts, 1s);
```
期望 8 行（全不匹配，FULL JOIN 两侧各 4 行带 NULL），实际返回 16 行（全匹配）。

## 验证
```sql
-- SELECT 路径（标量函数）：两侧值不同
select timetruncate(ts, 1d), timetruncate(ts, 1d, 0) from sta1;
-- t1d = 2023-10-16 00:00:00 (CST midnight = 1697385600000 ms)
-- t1d0 = 2023-10-16 08:00:00 (UTC midnight = 1697414400000 ms)
-- 不相等 ✓
```

## 根因分析

### 两条代码路径
**标量函数路径** (`timeTruncateFunction` in sclfunc.c):
- `timetruncate(ts, 1d)`: 注入 timezone="Asia/Shanghai", IANA 检测激活 → `truncateDayUnit(tz=Asia/Shanghai)` → CST midnight ✓
- `timetruncate(ts, 1d, 0)`: useCurrentTz=false → `timeVal/unit*unit` → UTC midnight ✓
- 两侧不同 → SELECT 结果正确

**Hash Join 快速路径** (`hJoinLaunchPrimExpr` in hashjoinoperator.c):
```c
// 用 timezoneUnit 做截断
if (0 != pCtx->timezoneUnit) {
    pPrimOut[i] = pPrimIn[i] - (pPrimIn[i] + timezoneUnit) % truncateUnit;
} else {
    pPrimOut[i] = pPrimIn[i] / truncateUnit * truncateUnit;  // UTC epoch
}
```
`timezoneUnit` 在 `hJoinInitPrimExprCtx` 中用 `offsetFromTz(timezoneStr, factor)` 计算。

### 关键 Bug：`offsetFromTz` 无法解析 IANA 名
`offsetFromTz` 只能解析固定偏移格式如 `"+0800"`：
```c
int64_t offsetFromTz(char *timezoneStr, int64_t factor) {
    char *minStr = &timezoneStr[3];               // 取 [3] 以后作分钟串
    int64_t minutes = taosStr2Int64(minStr, ...); // "+0800" → 0
    memset(minStr, 0, ...);                       // 截断字符串！
    int64_t hours = taosStr2Int64(timezoneStr, ...); // "+08" → 8
    return seconds * factor;                       // 28800 * 1000 = 28800000
}
```
但 `translateTimeTruncate` (builtins.c) 调用 `addTimezoneNameParam` 注入 **IANA 名** `"Asia/Shanghai"`，而不是 `"+0800"`：
- `offsetFromTz("Asia/Shanghai", ...)` → 解析 `"Asi"` 为 0 小时 → **返回 0**
- 两侧 `timezoneUnit` 都是 0 → 都走 UTC epoch truncation → 结果相同 → 16 行全匹配

### 变更引入时机
`p4 part1` (commit `5ee69079572`) 等 timezone 相关提交将 `addTimezoneParam`（注入固定偏移串）改为 `addTimezoneNameParam`（注入 IANA 名），以支持 DST-aware 截断。标量路径因此正确，但 Hash Join 的 `hJoinInitPrimExprCtx` 仍用 `offsetFromTz` 解析，导致 IANA 名被解析为 0。

## 已施加的修复
**文件**: `source/libs/executor/src/hashjoinoperator.c`

1. 添加 `#include "ttime.h"`
2. 在 `hJoinInitPrimExprCtx` 中，当 timezone 为 IANA 名时改用 `taosValidateTimezone` + `taosGetTimezoneOffsetAtSeconds(t=0, tz)` 获取偏移量：

```c
pCtx->truncateUnit = pUnit->typeData;
if ((NULL == pCurrTz || 1 == pCurrTz->typeData) &&
    pCtx->truncateUnit >= (86400 * TSDB_TICK_PER_SECOND(...))) {
    char *tzStr = varDataVal(pTimeZone->datum.p);
    int64_t factor = TSDB_TICK_PER_SECOND(...);
    if (strchr(tzStr, '/') != NULL || strcmp(tzStr, "UTC") == 0) {
        timezone_t tz = NULL;
        if (taosValidateTimezone(tzStr, &tz) == TSDB_CODE_SUCCESS) {
            int64_t offsetSeconds = 0;
            if (taosGetTimezoneOffsetAtSeconds((time_t)0, tz, &offsetSeconds) == TSDB_CODE_SUCCESS) {
                pCtx->timezoneUnit = offsetSeconds * factor;
            }
            tzfree(tz);
        }
    } else {
        pCtx->timezoneUnit = offsetFromTz(tzStr, factor);
    }
}
```

**预期效果**:
- `timetruncate(ts,1d)`: Asia/Shanghai → offset=28800s → timezoneUnit=28800000 → CST midnight
- `timetruncate(ts,1d,0)`: useCurrentTz=false → timezoneUnit=0 → UTC midnight
- 两侧不同 → 无匹配 → FULL JOIN 返回 8 行 ✓

## 状态
- [x] 代码修改完成
- [ ] 编译验证（make -j8 executor + make install）
- [ ] 测试验证（pytest cases/14-JoinQueries/test_join_full.py --clean）

## 注意事项
- `taosGetTimezoneOffsetAtSeconds(t=0, ...)` 用 epoch 0 作参考时间，对无 DST 时区（如 Asia/Shanghai）精确。对有 DST 的时区仍有潜在问题（全历史单一偏移），但这是 `hJoinLaunchPrimExpr` 本身架构局限，预先存在。
- 此修复不影响 `timeTruncateFunction`（标量路径），也不影响其他函数。
