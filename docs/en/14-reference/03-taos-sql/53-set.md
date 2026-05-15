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

Initialization sources on Linux and macOS:

- Linux (glibc): reads the first-day setting from `LC_TIME` in the current locale (internally via `_NL_TIME_FIRST_WEEKDAY`).
- macOS: first tries the system preference `AppleFirstWeekday`; if unavailable, falls back to the current system calendar setting.

You can check the OS-side settings as follows (for troubleshooting initialization results):

- Linux: `locale -k LC_TIME | grep first_weekday`
- macOS: `defaults read -g AppleFirstWeekday`

Note: The Windows path has not been fully verified for user documentation yet and will be added later.
