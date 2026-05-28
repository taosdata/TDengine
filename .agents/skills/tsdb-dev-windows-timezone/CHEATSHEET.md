# Windows Timezone Troubleshooting Cheat Sheet

## Quick problem classification

| Symptom | Common root cause | First thing to check | Common fix |
|------|----------|------------|----------|
| Always off by a fixed number of hours | UTC and local time are mixed | Semantics of the input and stored values | Standardize the internal representation and remove duplicate conversions |
| Off by one hour only on certain dates | DST handling is wrong | DST boundary samples | Use timezone rules instead of a fixed offset |
| Windows and Linux produce different results | Windows/IANA mapping mismatch | Source of the timezone ID | Centralize ID mapping |
| Scheduled jobs drift across seasons | An offset was stored, but not the timezone | Scheduling configuration model | Store business time plus timezone ID |
| Historical data is inaccurate | Timezone rules are outdated or the current offset is reused incorrectly | Windows patches and historical samples | Update the rules and add historical regression tests |

## Windows environment commands

```powershell
tzutil /g
Get-TimeZone
Get-Date
w32tm /query /status
Get-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\TimeZoneInformation'
```

Check `TZ` presence in your terminal:

```cmd
REM cmd.exe (typically empty — TZ is not set by default)
echo %TZ%

REM Cygwin / MSYS2 (typically auto-set, e.g. Asia/Shanghai)
echo $TZ
```

## Minimal diagnosis checklist

1. Is this field an absolute instant or a business-local time?
2. Does the input carry an offset or timezone ID?
3. Does the database store UTC, local time, or a raw string?
4. Does the API response carry an offset or timezone ID?
5. Does the issue only occur at DST boundaries or on historical dates?
6. Does the issue occur only on Windows or during cross-platform interactions?

## Review points that are easiest to miss

- Local-time entry points such as `DateTime.Now`, `new Date()`, or `LocalDateTime.now()`
- Parsing time strings with no offset
- Using the current offset to infer historical times
- Treating `UTC+08:00` as a timezone ID
- A frontend interpreting a backend time string with no offset using the browser's local timezone
- Scheduling logic that uses `+24h` instead of calculating the same local time on the next day
- Timezone globals/statics duplicated across multiple Windows DLLs
- One DLL updates timezone state while other DLLs continue to use stale copies
- **Test results differ between cmd.exe and Cygwin**: cmd.exe does not set `TZ` by default, while Cygwin auto-sets `TZ` — this causes taosc to take different internal timezone resolution paths, so always verify under both terminals

## Things not to do during a fix

- Do not manually add or subtract hours as a final solution
- Do not keep introducing new local-time fields
- Do not let each service maintain its own Windows/IANA mapping
- Do not merge the fix before DST regression tests pass
- Do not hide timezone state in mutable globals shared implicitly across DLLs

## Windows multi-DLL checklist

1. Identify timezone-related globals/statics in each DLL.
2. Verify whether each module has its own copy of timezone state.
3. Verify startup order for timezone initialization.
4. Verify how `TZ` is read and normalized.
5. Verify cache invalidation after runtime timezone updates.

## POSIX vs ISO 8601 quick reminder

- TDengine configuration uses POSIX timezone conventions.
- POSIX timezone strings are not equivalent to ISO 8601 offsets.
- Example: POSIX `TZ=UTC-8` matches ISO 8601 `+08:00` semantics.
- Every API contract should state the expected timezone standard explicitly.

## TDengine API modules to inspect

- `ttime.c`: parse/format and conversion interfaces
- `osTime.c` (or `osTIme.c` in some references): OS time access interfaces
- `osTimezone.c`: timezone initialization, `TZ` processing, and offset/DST interfaces

## Recommended test cases

- 10 minutes before and after DST starts
- 10 minutes before and after DST ends
- Timezones with non-hour offsets
- Conversion results for the same data on Windows and Linux
- Historical rule-change dates
- The same API viewed by users in different timezones
- The same test case run under cmd.exe (no `TZ`) and Cygwin (`TZ` auto-set) to verify consistent taosc behavior