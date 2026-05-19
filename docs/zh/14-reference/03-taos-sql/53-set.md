---
sidebar_label: SET 命令
title: SET 命令
description: SET 命令的完整列表
---

SET 命令用于设置当前连接或当前客户端进程的运行行为。

## SET TIMEZONE

```sql
SET TIMEZONE '<timezone_string>';
```

该语句仅影响当前连接。

- Windows 平台不支持该命令；执行时返回 `TSDB_CODE_NOT_SUPPORTTED_IN_WINDOWS`（`0x8000237`）。
- 支持 IANA 时区名与固定偏移格式 `[z/Z, +/-hh, +/-hhmm, +/-hh:mm]`。
- 有歧义的时区缩写（如 `CST`）会被拒绝。
- 非法值返回 `TSDB_CODE_PAR_INVALID_TIMEZONE`（`0x800026B2`）。

## SET FIRST_DAY_OF_WEEK

```sql
SET FIRST_DAY_OF_WEEK <0..6>;
```

该语句仅影响当前连接。

- `0` 表示周日，`1` 表示周一，...，`6` 表示周六。
- 超出 `0..6` 的值返回 `TSDB_CODE_PAR_INVALID_FIRST_DAY_OF_WEEK`（`0x800026B3`）。

`firstDayOfWeek` 的初始化顺序如下（仅说明当前实现行为）：

1. 如果在客户端配置中显式设置了 `firstDayOfWeek`（例如 `taos.cfg`、环境变量或命令行），优先使用该值。
2. 如果未显式设置，客户端在启动时尝试读取操作系统的“每周起始日”设置。
3. 如果操作系统值不可用，回退到默认值 `4`（周四）。

如果希望修改默认行为，可按平台先调整操作系统设置；需要只影响当前连接时，再使用 `SET FIRST_DAY_OF_WEEK`：

- Linux：通过 locale 的 `LC_TIME` 配置每周起始日。
- macOS：通过系统设置修改每周起始日；如需脚本化调整，可修改系统偏好项 `AppleFirstWeekday`。若两者不一致，优先读取 `AppleFirstWeekday`；只有该项不可用时，才回退到当前系统日历设置。
- Windows：通过系统区域设置修改每周起始日。

排查初始化结果时，可按如下方式检查系统侧设置：

- Linux：`locale -k LC_TIME | grep first_weekday`
- macOS：`defaults read -g AppleFirstWeekday`
