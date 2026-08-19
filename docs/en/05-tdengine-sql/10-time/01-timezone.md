---
sidebar_label: Timezone and Natural Time Units
title: Timezone and Natural Time Units
description: TDengine timezone semantics and natural time unit behavior
toc_max_heading_level: 4
---

This document describes TDengine timezone semantics and natural time unit behavior. Features are delivered across versions, marked inline:

| Mark | Meaning |
| --- | --- |
| (unmarked) | Supported since v3.4.1 |
| **[v3.4.2]** | Supported since v3.4.2 |
| **[v3.4.3]** | Supported since v3.4.3 (not available in v3.4.2) |

---

## Timezone Overview

TDengine stores all time data internally as UTC timestamps (`int64`). Timezones only affect conversions between time strings and UTC: on writes, local time strings are converted to UTC; on reads, UTC is formatted back to local time strings for display.

### Supported Timezone Formats

#### IANA Name

Recommended. Examples: `'Asia/Shanghai'`, `'America/New_York'`.

- **DST aware**: Yes. DST transitions are handled automatically.

#### POSIX Fixed Offset

Examples: `'+08:00'`, `'-0500'`, `'Z'`, `'+10'`, `'UTC+08:00'`, and
`'UTC8'`.

- **DST aware**: No. A constant offset is used year-round.
- **Supported range**: `-14:00` to `+14:00`.
- **Sign convention**: Entry points `SET TIMEZONE`, `ALTER LOCAL`, `taos.cfg`, `TO_CHAR`, and `TIMETRUNCATE` all follow POSIX sign convention (`+` = west of UTC, i.e. `local = UTC − offset`). **Exception**: `TO_ISO8601`'s fixed-offset parameter uses [ISO 8601 sign convention](#to_iso8601). See below for details.

#### POSIX Fixed-Offset Format Details

TDengine's fixed-offset timezone format follows a subset of the **POSIX `TZ` environment variable specification**. The full POSIX specification defines the following format:

```text
STD offset [ DST [ dstoffset ] [ , rule ] ]
```

where `STD` is the standard-time abbreviation, `offset` is the offset from UTC, `DST` and `dstoffset` define daylight saving time, and `rule` defines DST transition rules.

**TDengine supported subset**: TDengine only supports the `STD offset` portion, and `STD` only accepts `UTC` as its value. Manual configuration of `DST`, `dstoffset`, and `rule` is not supported. For DST support, use IANA timezone names. The accepted fixed-offset syntax is:

```text
UTC offset
```

**`offset` field format**: With the `UTC` prefix, the hour can contain one or
two digits, and the minutes, if present, must contain two digits. The plus sign
for a westward offset can be omitted: `UTC8` is equivalent to `UTC+8`, and
`UTC8:30` is equivalent to `UTC+8:30`. Without the `UTC` prefix, the sign is
required; the accepted forms are `±HH`, `±HHMM`, and `±HH:MM`. Offsets have
minute-level precision. The `UTC` prefix is case-insensitive, so `utc+8` and
`UTC+8` are equivalent. The special value `Z` is equivalent to `+00:00`.

**POSIX sign convention**: The POSIX standard defines the relationship between local time and UTC as:

```text
local_time = UTC - offset
```

Therefore: a positive `+` sign means **west of UTC** (western timezone, local time is behind UTC), and a negative `-` sign means **east of UTC** (eastern timezone, local time is ahead of UTC). This is the **opposite** of the ISO 8601 sign convention. For example:

| Notation | POSIX meaning | Equivalent IANA timezone |
| --- | --- | --- |
| `'+08:00'` or `'UTC+08:00'` | 8 hours west of UTC | Near `Pacific/Pitcairn` (Pacific) |
| `'-08:00'` or `'UTC-08:00'` | 8 hours east of UTC | Near `Asia/Shanghai` (Beijing time) |
| `'+05:30'` or `'UTC+05:30'` | 5.5 hours west of UTC | Near `America/Bogota` (Colombia) |
| `'-05:30'` or `'UTC-05:30'` | 5.5 hours east of UTC | Near `Asia/Kolkata` (India) |
| `'Z'` | UTC itself | `Etc/UTC` |

When the `UTC` prefix is omitted (e.g., `'+08:00'`), TDengine still parses the
offset using POSIX rules, identical to the prefixed form. In this case, the
sign cannot be omitted.

**Supported range**: The valid offset range is `-14:00` to `+14:00` (corresponding to 14 hours east of UTC to 14 hours west of UTC).

**Difference from IANA timezones**: POSIX fixed offsets contain no DST information and use a constant offset year-round. If the target region observes DST (e.g., US, Europe), use an IANA name for correct automatic DST transitions.

### Timezone Priority

TDengine uses a five-level timezone priority model, where higher levels override lower levels:

| Priority | Level | How to set | Description |
| --- | --- | --- | --- |
| Highest | SQL level | Function timezone parameters (e.g., `TO_ISO8601(ts, '+09:00')`); `TO_ISO8601` IANA parameters **[v3.4.2]**; stream `TIMEZONE` clause **[v3.4.3]** | Affects only the current SQL statement or stream task |
| High | Connection level | C API `taos_options_connection`; `SET TIMEZONE` **[v3.4.2]** | Affects all SQL statements on the current connection |
| Medium | Client global | `timezone` in client-side `taos.cfg` | Affects only client-side local time formatting |
| Low | Server global | `timezone` in server-side `taos.cfg` | Fallback for server-side calculations when the connection timezone is unset |
| Lowest | System default | Automatically detected from the operating system | Final fallback |

**Important**: The client global timezone only affects client-side display (e.g., `SELECT ts` output formatting), not server-side calculations. Connections without a connection-level timezone fall back to the server global timezone for server-side calculations.

## Set Timezone

### SET TIMEZONE [v3.4.2] {#set-timezone-v342}

Set the timezone for the current connection:

```sql
SET TIMEZONE 'Asia/Shanghai';
SET TIMEZONE '-08:00';             -- POSIX: local = UTC+8, same as Beijing time
SET TIMEZONE '+08:00';             -- POSIX: local = UTC-8, NOT Beijing time
SET TIMEZONE 'America/New_York';
```

The sign of fixed offsets follows the POSIX sign convention. See "POSIX Fixed-Offset Format Details" above.

After setting, the current connection uses this timezone for:

- Timestamp column display (`SELECT ts`)
- `SELECT NOW()` / `SELECT NOW`
- Time-formatting functions such as `TO_ISO8601(ts)`
- `TODAY()`
- Natural-boundary calculations such as `TIMETRUNCATE(..., 1d/1w/1n...)` and `INTERVAL`

You can also set the timezone via C API `taos_options_connection` when establishing a connection, which has the same effect as `SET TIMEZONE`.

To use Beijing time on the current connection, the simplest approach is:

```sql
SET TIMEZONE 'Asia/Shanghai';
```

### Query Current Timezone

```sql
SELECT TIMEZONE();
```

Returns the single effective timezone string for the current connection. The fallback chain is: connection-level `SET TIMEZONE` / C API value → client-global timezone snapshotted when the connection was created → system default timezone.

### Configuration File

Configure the global timezone in `taos.cfg`:

```text
timezone Asia/Shanghai
timezone UTC-8
timezone UTC8
timezone +08:00
```

Accepts IANA names, Windows standard timezone names (e.g.,
`China Standard Time`), and fixed-offset forms `Z`, `±HH`, `±HHMM`,
`±HH:MM`, `UTCH[:MM]`, `UTC+H[:MM]`, and `UTC-H[:MM]`, where `H` contains
one or two digits and the `UTC` prefix is case-insensitive. `GMT` / `GMT±...`
is not supported. If not configured, the timezone is detected from the
operating system.

- **Server-side** `taos.cfg`: used as the fallback for server-side calculations when the connection timezone is not set through `SET TIMEZONE`.
- **Client-side** `taos.cfg`: affects only client-side local time formatting (e.g., `SELECT ts` output), not server-side calculations.

**Note**: Fixed-offset strings follow the POSIX sign convention (see "POSIX Fixed-Offset Format Details"). The sign meaning is consistent across all entry points (`SET TIMEZONE`, `ALTER LOCAL`, `taos.cfg`). Use IANA names to avoid confusion.

Differences between `ALTER LOCAL 'timezone ...'` and `SET TIMEZONE ...`:

- `SET TIMEZONE` only affects the current connection. It is lost on reconnect.
- `ALTER LOCAL 'timezone ...'` modifies the global config of the current client process. It only affects connections created after the change; existing connections are not affected.

## First Day of Week

### SET FIRST_DAY_OF_WEEK [v3.4.2] {#set-first_day_of_week-v342}

Set the first day of the week for the current connection:

```sql
SET FIRST_DAY_OF_WEEK 0;  -- Sunday
SET FIRST_DAY_OF_WEEK 1;  -- Monday
```

**Note**: The client configuration default for `firstDayOfWeek` is `4` (Thursday). See the configuration table below. The SQL above only sets the current connection.

Valid range is 0-6: 0=Sunday, 1=Monday, ..., 6=Saturday.

### Query Current First Day of Week [v3.4.2]

```sql
SELECT FIRST_DAY_OF_WEEK();
```

Returns the current first-day-of-week setting for the connection as an integer `0..6`, where `0=Sunday`, `1=Monday`, ..., `6=Saturday`.

### Configuration File [v3.4.2]

Configure in client-side `taos.cfg`:

```text
firstDayOfWeek 4
```

Can also be changed dynamically within the current client process with `ALTER LOCAL 'firstDayOfWeek' '<0..6>'`. This only affects connections created after the change; existing connections keep their snapshot values.

The default value is `4` (Thursday), for backward compatibility with the historical Unix epoch modulo week alignment. If not explicitly configured, the client attempts to read the OS first-day-of-week setting at startup; if that fails, it falls back to `4`.

To have weekly aggregations start on Monday:

```sql
SET FIRST_DAY_OF_WEEK 1;
```

For Sunday, set it to `0`.

### Scope of Effect [v3.4.2]

`firstDayOfWeek` affects all operations using `w` (week) as the time unit:

- `TIMETRUNCATE(ts, 1w)` alignment day
- `INTERVAL(1w)` window start day
- `PERIOD(1w)` trigger day **[v3.4.3]**
- `SLIDING(1w)` trigger day **[v3.4.3]**

## Time Functions

### TO_ISO8601

```sql
SELECT TO_ISO8601(ts) FROM t;                        -- uses connection timezone
SELECT TO_ISO8601(ts, '+09:00') FROM t;              -- specified fixed offset (ISO 8601 sign)
SELECT TO_ISO8601(ts, 'UTC+09:00') FROM t;           -- equivalent, 'UTC' prefix is stripped
SELECT TO_ISO8601(ts, 'America/New_York') FROM t;    -- specified IANA timezone [v3.4.2]
```

**Sign convention**: `TO_ISO8601` is the only entry point that uses the ISO 8601 sign convention — `local = UTC + offset`, meaning `'+08:00'` denotes east-8 (Beijing time). The following forms are fully equivalent: `'+0800'`, `'+08:00'`, `'UTC+8'`, `'UTC+0800'`, `'UTC+08:00'`. All other entry points (`SET TIMEZONE`, `taos.cfg`, `TO_CHAR`, `TIMETRUNCATE`, etc.) use POSIX sign convention (`+` = west).

When using an IANA timezone, the output offset varies automatically with DST:

```sql
SET TIMEZONE 'America/New_York';           -- [v3.4.2]
SELECT TO_ISO8601('2026-01-15 12:00:00');  -- ...T12:00:00-05:00 (EST, winter)
SELECT TO_ISO8601('2026-07-15 12:00:00');  -- ...T12:00:00-04:00 (EDT, summer)
```

### TIMETRUNCATE

Truncate a timestamp to the specified unit boundary.

```sql
SELECT TIMETRUNCATE(ts, 1d) FROM t;                          -- truncate to 00:00:00 of the day
SELECT TIMETRUNCATE(ts, 1w) FROM t;                          -- truncate to first day of week 00:00:00
SELECT TIMETRUNCATE(ts, 1n) FROM t;                          -- truncate to 1st of month [v3.4.2]
SELECT TIMETRUNCATE(ts, 1q) FROM t;                          -- truncate to 1st of quarter [v3.4.2]
SELECT TIMETRUNCATE(ts, 1y) FROM t;                          -- truncate to Jan 1st [v3.4.2]
SELECT TIMETRUNCATE(ts, 1d, 'America/New_York') FROM t;      -- specify timezone [v3.4.2]
```

**Supported natural time units**:

| Unit | Meaning | Truncation rule | Version |
| --- | --- | --- | --- |
| `d` | Day | Align to 00:00:00 of the day | Supported |
| `w` | Week | Align to 00:00:00 of the first day of week (determined by `firstDayOfWeek`) | Supported; respects firstDayOfWeek since v3.4.2 |
| `n` | Month | Align to 00:00:00 of the 1st of the month | **v3.4.2** |
| `q` | Quarter | Align to 00:00:00 of the 1st month of the quarter (Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct) | **v3.4.2** |
| `y` | Year | Align to 00:00:00 of January 1st | **v3.4.2** |

**Examples**:

```sql
SELECT TIMETRUNCATE('2026-03-15', 1n);   -- 2026-03-01 00:00:00 [v3.4.2]
SELECT TIMETRUNCATE('2026-05-15', 1q);   -- 2026-04-01 00:00:00 [v3.4.2]
SELECT TIMETRUNCATE('2026-08-15', 1y);   -- 2026-01-01 00:00:00 [v3.4.2]
```

**Third parameter** (timezone):

| Value | Behavior | Version |
| --- | --- | --- |
| `0` | Use UTC (legacy) | Supported |
| `1` | Use connection timezone (legacy) | Supported |
| `'Asia/Shanghai'` | Use specified IANA timezone | **v3.4.2** |
| `'+08:00'` | Use specified fixed offset | **v3.4.2** |
| Omitted | Use connection timezone | Supported |

### TIMEZONE()

```sql
SELECT TIMEZONE();
```

Returns the single timezone string currently in use by the connection.

- If `SET TIMEZONE` has been executed, returns the connection-level timezone.
- Otherwise, returns the client-global timezone snapshotted when the connection was created; if not configured, falls back to the system default.
- `ALTER LOCAL 'timezone'` only affects connections created afterward; it does not change the `TIMEZONE()` result of existing connections.

To verify whether `SET TIMEZONE` has taken effect:

```sql
SELECT TIMEZONE();
SELECT TO_ISO8601(NOW());
```

Examples:

- After `SET TIMEZONE 'Asia/Shanghai'`, `TIMEZONE()` returns `Asia/Shanghai`.
- Without `SET TIMEZONE`, returns the client-global timezone snapshotted at connection creation.
- After `ALTER LOCAL 'timezone Asia/Shanghai'`, existing connections are unaffected; only new connections use the new config.

## INTERVAL Queries

`INTERVAL` supports natural time unit window splitting:

```sql
SELECT _wstart, COUNT(*) FROM meters
  INTERVAL(1n)                      -- monthly windows [v3.4.2]
  FILL(PREV);

SELECT _wstart, AVG(voltage) FROM meters
  INTERVAL(1q)                      -- quarterly windows [v3.4.2]
  FILL(NULL);

SELECT _wstart, SUM(energy) FROM meters
  INTERVAL(1w)                      -- weekly windows (respects firstDayOfWeek) [v3.4.2]
  FILL(LINEAR);
```

**Supported natural time units**:

| Unit | Window boundary | Version |
| --- | --- | --- |
| `d` | Local timezone 00:00:00 each day | Supported |
| `w` | Local timezone 00:00:00 on the first day of week (determined by `firstDayOfWeek`) | **v3.4.2** |
| `n` | Local timezone 00:00:00 on the 1st of each month | **v3.4.2** |
| `q` | Local timezone 00:00:00 on the 1st of the first month of each quarter | **v3.4.2** |
| `y` | Local timezone 00:00:00 on January 1st of each year | **v3.4.2** |

**Multi-interval windows**:

```sql
INTERVAL(2q)   -- half-year window: [Jan, Jul), [Jul, next Jan) [v3.4.2]
INTERVAL(3n)   -- quarterly window (equivalent to 1q): Jan/Apr/Jul/Oct [v3.4.2]
INTERVAL(2w)   -- bi-weekly window [v3.4.2]
```

**DST handling**: Windows always align to local wall-clock time. On DST transition days, the physical duration of a window may change (e.g., a `1d` window on spring-forward day is 23 hours). This is correct behavior. For write/query caveats around DST gaps and overlaps, see [DST Usage](./02-dst.md).

**Leap year / variable-length months**: Window widths automatically adapt to actual day counts (e.g., a February window is 28 or 29 days). `FILL` boundaries advance month-by-month / quarter-by-quarter.

## Stream Timezone

### Stream TIMEZONE Clause [v3.4.3]

Before v3.4.3, stream trigger-side natural time boundary alignment always used the server global timezone, with no way to specify an independent timezone for individual stream tasks. Starting from v3.4.3, a `TIMEZONE` clause is available for all trigger types:

```sql
-- PERIOD trigger: Tokyo timezone, weekly
CREATE STREAM weekly_tokyo TRIGGER PERIOD(1w) TIMEZONE 'Asia/Tokyo'
  INTO tokyo_weekly AS SELECT AVG(current) FROM meters;

-- SLIDING trigger: New York timezone, quarterly
CREATE STREAM slide_ny TRIGGER SLIDING(1q) TIMEZONE 'America/New_York'
  FROM meters
  INTO ny_quarterly AS SELECT _tprev_ts, _tcurrent_ts, AVG(current) FROM %%trows;

-- INTERVAL trigger: London timezone, monthly window
CREATE STREAM monthly_uk TRIGGER INTERVAL(1n) SLIDING(1w) TIMEZONE 'Europe/London'
  FROM meters
  INTO uk_monthly AS SELECT _wstart, _wend, AVG(current) FROM %%trows;

-- EVENT trigger: computation side uses Tokyo timezone
CREATE STREAM event_tokyo TRIGGER EVENT_WINDOW(START WITH voltage > 220 END WITH voltage <= 220)
  TIMEZONE 'Asia/Tokyo'
  FROM meters PARTITION BY tbname
  INTO event_out AS SELECT _twstart, _twend, AVG(current) FROM %%trows;
```

**Frozen behavior**: `TIMEZONE` is frozen into stream metadata at creation time. Subsequent changes to the global timezone do not affect existing stream tasks.

**When TIMEZONE is not specified**: Resolved in order of connection timezone → server global timezone → OS timezone, then frozen.

### Stream Timezone Effects

| Affected area | Description |
| --- | --- |
| Trigger side (PERIOD/SLIDING/INTERVAL) | Calendar boundary alignment for natural units (d/w/n/q/y) uses the frozen timezone |
| Computation side (AS subquery) | INTERVAL natural unit window splitting uses the frozen timezone and firstDayOfWeek |

### Stream Trigger Natural Unit Support

The following tables list supported time units for PERIOD, SLIDING, and INTERVAL trigger types along with their versions:

**PERIOD trigger**:

| Unit | Meaning | Version |
| --- | --- | --- |
| `a` | Millisecond | Supported |
| `s` | Second | Supported |
| `m` | Minute | Supported |
| `h` | Hour | Supported |
| `d` | Day | Supported |
| `w` | Week | Supported |
| `n` | Month | Supported |
| `y` | Year | Supported |
| `q` | Quarter | **v3.4.3** |

**Offset examples**:

```sql
PERIOD(1w, 1d)       -- trigger every Tuesday 00:00:00
PERIOD(1n, 14d)      -- trigger on the 15th of each month 00:00:00
PERIOD(1y, 31d)      -- trigger on February 1st each year 00:00:00
PERIOD(1q)           -- trigger on the 1st of each quarter 00:00:00 [v3.4.3]
PERIOD(1q, 15d)      -- trigger on the 16th day of each quarter [v3.4.3]
```

**SLIDING trigger**:

| Unit | Meaning | Version |
| --- | --- | --- |
| `a` | Millisecond | Supported |
| `s` | Second | Supported |
| `m` | Minute | Supported |
| `h` | Hour | Supported |
| `d` | Day | Supported |
| `w` | Week | Supported |
| `n` | Month | **v3.4.3** |
| `q` | Quarter | **v3.4.3** |
| `y` | Year | **v3.4.3** |

```sql
SLIDING(1n)          -- monthly sliding trigger [v3.4.3]
SLIDING(1q)          -- quarterly sliding trigger [v3.4.3]
SLIDING(1y)          -- yearly sliding trigger [v3.4.3]
SLIDING(1q, 15d)     -- trigger on the 16th day of each quarter [v3.4.3]
```

**INTERVAL window trigger** (applies to both interval_val and sliding_val):

| Unit | Meaning | Version |
| --- | --- | --- |
| `a` | Millisecond | Supported |
| `s` | Second | Supported |
| `m` | Minute | Supported |
| `h` | Hour | Supported |
| `d` | Day | **v3.4.3** |
| `w` | Week | **v3.4.3** |
| `n` | Month | **v3.4.3** |
| `q` | Quarter | **v3.4.3** |
| `y` | Year | **v3.4.3** |

```sql
INTERVAL(1n) SLIDING(1w)    -- monthly window, weekly sliding [v3.4.3]
INTERVAL(1q) SLIDING(1n)    -- quarterly window, monthly sliding [v3.4.3]
INTERVAL(1y) SLIDING(1q)    -- yearly window, quarterly sliding [v3.4.3]
INTERVAL(1w) SLIDING(1d)    -- weekly window, daily sliding [v3.4.3]
```

### View Stream Timezone [v3.4.3]

```sql
SELECT stream_name, timezone, first_day_of_week FROM information_schema.ins_streams;
```

## Timezone Source Quick Reference

| Scenario | Timezone source | Version notes |
| --- | --- | --- |
| Write `INSERT` | Connection → Server global → OS | Converts time strings to UTC; supported |
| Read `SELECT ts` | Connection → Client global → OS | Formats UTC to local time; connection-level fallback supported since **v3.4.2** (previously OS timezone only) |
| Functions (`TO_ISO8601` etc.) | SQL parameter → Connection → Server global → OS | Fixed-offset parameter supported; IANA parameter since **v3.4.2** |
| `TIMETRUNCATE` | SQL parameter → Connection → Server global → OS | `d`/`w` supported; `n`/`q`/`y` since **v3.4.2**; timezone string parameter since **v3.4.2** |
| `INTERVAL` query windows | Connection → Server global → OS | `d` supported; `w`/`n`/`q`/`y` since **v3.4.2** |
| `SHOW` / `EXPLAIN` | Connection → Client global → OS | Connection-level fallback supported since **v3.4.2** (previously OS timezone only) |
| Stream trigger and computation | Server global → OS; **[v3.4.3]** supports `TIMEZONE` clause → Connection → Server global → OS (frozen at creation) | Before v3.4.3 uses server timezone; v3.4.3 supports freezing |

## Configuration Parameters

| Parameter | Config file | Type | Default | Description | Version |
| --- | --- | --- | --- | --- | --- |
| `timezone` | Server/client-side `taos.cfg` | String | OS detected | Global timezone | Supported |
| `firstDayOfWeek` | Client-side `taos.cfg` | Integer 0-6 | 4 (Thursday) | First day of week; can also be changed dynamically via `ALTER LOCAL` | **v3.4.2** |

## Error Messages

| Error scenario | Error message |
| --- | --- |
| Invalid timezone string | `[0x26B2] Invalid timezone: '<value>'` |
| firstDayOfWeek out of range | `[0x26B3] Invalid firstDayOfWeek: <value>, must be 0-6` |

## Version Support Matrix

| Feature | Before v3.4.2 | v3.4.2 | v3.4.3 |
| --- | --- | --- | --- |
| `timezone` config file (server/client) | ✅ | ✅ | ✅ |
| `TO_ISO8601` fixed-offset parameter | ✅ | ✅ | ✅ |
| `TIMETRUNCATE` `d`/`w` truncation | ✅ | ✅ | ✅ |
| `INTERVAL` query `d` window | ✅ | ✅ | ✅ |
| `TIMEZONE()` function | ✅ | ✅ (enhanced) | ✅ |
| PERIOD trigger `a`/`s`/`m`/`h`/`d`/`w`/`n`/`y` | ✅ | ✅ | ✅ |
| SLIDING trigger `a`/`s`/`m`/`h`/`d`/`w` | ✅ | ✅ | ✅ |
| INTERVAL window trigger `a`/`s`/`m`/`h` | ✅ | ✅ | ✅ |
| `SET TIMEZONE` | ❌ | ✅ | ✅ |
| `SET FIRST_DAY_OF_WEEK` | ❌ | ✅ | ✅ |
| `firstDayOfWeek` config parameter | ❌ | ✅ | ✅ |
| `TO_ISO8601` IANA timezone parameter | ❌ | ✅ | ✅ |
| `TIMETRUNCATE` timezone string parameter | ❌ | ✅ | ✅ |
| `TIMETRUNCATE` `n`/`q`/`y` truncation | ❌ | ✅ | ✅ |
| `INTERVAL` query `w`/`n`/`q`/`y` windows | ❌ | ✅ | ✅ |
| Plain column reads use connection timezone | ❌ | ✅ | ✅ |
| SHOW/EXPLAIN use connection timezone | ❌ | ✅ | ✅ |
| Stream `TIMEZONE` clause | ❌ | ❌ | ✅ |
| Stream timezone/firstDayOfWeek freezing | ❌ | ❌ | ✅ |
| PERIOD trigger `q` quarter | ❌ | ❌ | ✅ |
| SLIDING trigger `n`/`q`/`y` | ❌ | ❌ | ✅ |
| INTERVAL window trigger `d`/`w`/`n`/`q`/`y` | ❌ | ❌ | ✅ |
| `ins_streams` timezone/first_day_of_week columns | ❌ | ❌ | ✅ |
