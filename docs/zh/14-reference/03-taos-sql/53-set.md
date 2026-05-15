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

Linux 与 macOS 的初始化来源：

- Linux（glibc）：从当前 locale 的 `LC_TIME` 中读取首日配置（内部使用 `_NL_TIME_FIRST_WEEKDAY`）。
- macOS：优先读取系统偏好 `AppleFirstWeekday`；若该项不可用，再回退到当前系统日历的首日设置。

可按如下方式检查系统侧设置（用于排查初始化结果）：

- Linux：`locale -k LC_TIME | grep first_weekday`
- macOS：`defaults read -g AppleFirstWeekday`

说明：Windows 路径当前尚未完成手册级验证，后续补充。
