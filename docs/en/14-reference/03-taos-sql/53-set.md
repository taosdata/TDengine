---
sidebar_label: SET Commands
title: SET Commands
description: Complete list of SET commands
---

SET commands are used to adjust runtime behavior for the current connection or current client process.

## SET TIMEZONE

```sql
SET TIMEZONE '<timezone_string>';
```

This statement affects the current connection only.

- This command is not supported on Windows. Executing it returns `TSDB_CODE_NOT_SUPPORTTED_IN_WINDOWS` (`0x8000237`).
- Supports IANA timezone names and fixed offsets `[z/Z, +/-hh, +/-hhmm, +/-hh:mm]`.
- Ambiguous abbreviations (for example `CST`) are rejected.
- Invalid values return `TSDB_CODE_PAR_INVALID_TIMEZONE` (`0x800026B2`).

## SET FIRST_DAY_OF_WEEK

```sql
SET FIRST_DAY_OF_WEEK <0..6>;
```

This statement affects the current connection only.

- `0` means Sunday, `1` means Monday, ... , `6` means Saturday.
- Values outside `0..6` return `TSDB_CODE_PAR_INVALID_FIRST_DAY_OF_WEEK` (`0x800026B3`).

The initialization order of `firstDayOfWeek` is as follows (current implementation behavior):

1. If `firstDayOfWeek` is explicitly configured on the client side (for example via `taos.cfg`, environment variables, or command-line options), that value is used first.
2. If not explicitly configured, the client tries to read the operating system's first-day-of-week setting at startup.
3. If the OS value is unavailable, it falls back to the default value `4` (Thursday).

If you want to change the default behavior, first adjust the operating system setting for your platform. If you only want to affect the current connection, use `SET FIRST_DAY_OF_WEEK`:

- Linux: configure the first day of the week through locale `LC_TIME`.
- macOS: change the first day of the week in System Settings. For scripted changes, you can modify the `AppleFirstWeekday` system preference. If the two sources differ, `AppleFirstWeekday` is preferred; only when it is unavailable does the client fall back to the current system calendar setting.
- Windows: change the first day of the week through Regional settings.

To troubleshoot initialization results, you can inspect the operating system settings as follows:

- Linux: `locale -k LC_TIME | grep first_weekday`
- macOS: `defaults read -g AppleFirstWeekday`
