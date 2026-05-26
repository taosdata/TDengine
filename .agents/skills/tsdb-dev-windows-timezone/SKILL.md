---
name: tsdb-dev-windows-timezone
description: "Investigate and remediate Windows timezone issues (UTC/local conversion, DST transitions, TZ handling, Windows/IANA mapping, and multi-DLL global-state divergence). Use when timestamps drift or scheduling fails around timezone boundaries. Keywords: timezone, DST, TZ, POSIX, ISO 8601, Windows DLL."
metadata:
  author: wpan
  version: 1.0.0
  owner_team: engine
compatibility: "Windows only; multi-DLL and service environments"
---

# tsdb-dev-windows-timezone

Use this skill to handle Windows timezone issues systematically, including problem diagnosis, remediation, code review, and the current recommended design principles.

## When to use

- Users report that timestamps are off by several hours
- A one-hour error appears only around DST transitions
- Time values are inconsistent across Windows and Linux, browser and backend, or database and application
- Scheduled jobs, reports, audit logs, or retry pipelines fail at local-time boundaries
- You need to review time-handling code for latent timezone defects
- You need to define or align the current design principles for time handling

## Inputs

Collect the following context first:

- Symptoms: how many hours off, which pages or APIs are affected, and whether the issue only affects certain regions
- Expected semantics: whether the value is an absolute instant or a business-local time
- Environment: Windows version, patch level, host or container setup, database type, and backend language
- Timezone details: Windows timezone ID, IANA timezone ID, current UTC offset, and whether DST is involved
- Data flow: what time format is used during ingestion, storage, transport, rendering, and scheduling
- Runtime linkage model: whether multiple DLLs are loaded and whether timezone-related globals/statics can exist as per-module copies
- Reproducible sample: input time, user region, API response, and raw database value

If critical input is missing, ask these highest-priority clarifying questions first:

1. Is the target field an absolute instant or a business-local time?
2. Which timezone standard is configured in this path (POSIX, ISO 8601 offset, Windows ID, or IANA ID)?
3. Is the issue reproducible in a single process, or only when multiple DLLs/modules are involved?
4. Is `TZ` configured explicitly, and if yes, where and when is it initialized?

## Workflow

### 1. Identify the problem category first

Classify the issue based on the symptom pattern:

- **Fixed offset error**: usually UTC is treated as local time, or local time is converted twice
- **Only fails at DST boundaries**: usually a fixed offset is mistaken for a timezone, or nonexistent/duplicated local times are not handled
- **Cross-platform inconsistency**: usually Windows and IANA IDs are mapped differently, or one side depends on the system default timezone
- **Scheduler drift**: usually only an offset is stored instead of a timezone ID, or the next trigger time is rolled forward by a fixed 24 hours
- **Historical data error**: usually caused by outdated Windows timezone rules or by incorrectly reusing the current offset

### 2. Diagnose the issue

Diagnose in the order of semantics -> data -> environment -> code. Do not start by changing code.

#### 2.1 Clarify the time semantics

Establish what the field actually represents:

- An absolute instant, such as order creation time or message write time
- A business-local time, such as run at 9:00 AM every day or close the store at 18:00 local time
- A display time, such as the time shown in the user's region

If this is not defined clearly, later fixes are likely to be temporary compensation only.

#### 2.2 Draw the time flow

Confirm at least the following nodes:

1. Does the input carry UTC, local time, offset-aware time, or a timezone-less string?
2. What type is used after the backend parses it?
3. What format is stored in the database?
4. Does the API response include an offset or timezone ID?
5. How does the frontend or reporting layer interpret the value?

Recommended flow format:

```text
input value -> parsed type -> internal representation -> storage format -> API serialization -> display/scheduling
```

#### 2.3 Inspect the Windows environment

Verify whether the operating system itself is part of the problem:

- `tzutil /g`
- `Get-TimeZone`
- `Get-Date`
- `w32tm /query /status`
- `Get-ItemProperty 'HKLM:\SYSTEM\CurrentControlSet\Control\TimeZoneInformation'`
- `echo %TZ%` (in cmd.exe) or `echo $TZ` (in Cygwin/bash)

Focus on:

- Whether the system timezone is correct
- Whether the system clock is synchronized with NTP
- Whether automatic DST adjustment is disabled
- Whether Windows patches are recent enough, especially for regions with recent timezone rule changes
- **Which terminal is being used**: cmd.exe does not set the `TZ` environment variable by default, but Cygwin automatically sets `TZ` from the system timezone. When `TZ` is present, taosc may take a different internal code path (e.g. POSIX-style timezone resolution via `TZ`) than when `TZ` is absent (e.g. Windows API-based timezone resolution). Tests that pass under one terminal may fail under the other due to this divergence.

Use `scripts/collect-timezone-context.ps1` if you need to collect the environment in one pass.

#### 2.4 Reproduce with boundary samples

Do not test only on a normal day. At minimum, add these cases:

- 10 minutes before DST starts, the transition moment, and 10 minutes after it starts
- 10 minutes before DST ends, the transition moment, and 10 minutes after it ends
- Historical rule change dates
- Non-hour offsets such as UTC+05:30 and UTC+09:30
- The same input executed once on Windows and once on Linux
- The same input executed once in cmd.exe (no `TZ`) and once in Cygwin (`TZ` auto-set)

#### 2.5 Determine which layer owns the root cause

Common root-cause layers:

- **OS configuration layer**: system timezone, NTP, DST switch, Windows patches
- **Terminal environment layer**: cmd.exe vs Cygwin differences in `TZ` presence, causing taosc to follow different timezone resolution paths
- **Model layer**: absolute instants and business-local time are mixed into one field
- **Conversion layer**: UTC and local time are converted an incorrect number of times
- **Mapping layer**: Windows IDs and IANA IDs are mixed
- **Serialization layer**: offsets or timezone information are dropped in APIs or messages
- **Scheduling layer**: next execution time is computed from a fixed offset

#### 2.6 Check Windows multi-DLL global-state behavior

On Windows, mutable globals can effectively become duplicated state when maintained separately in different DLLs.

Focus on:

- Whether timezone-related globals/statics are defined in more than one module
- Whether each DLL initializes timezone state independently
- Whether one module mutates timezone state but other modules keep stale copies
- Whether initialization order changes the observed timezone behavior

## Remediation

Fix the root cause whenever possible. Do not use compensation logic such as manually adding 8 hours or subtracting 1 hour.

### 1. OS and runtime environment fixes

Use these when the machine configuration is wrong or timezone rules are outdated:

- Correct the Windows system timezone
- Enable automatic DST adjustment, or explicitly confirm that the business requires it to remain disabled
- Repair NTP synchronization
- Update Windows patches so timezone rules are current
- Define ownership clearly for time configuration on hosts, containers, and scheduled-task runtimes

### 2. Data model fixes

Split the model by semantics:

- **Absolute instant**: store it in UTC
- **Business-local time**: store local date-time plus timezone ID, not just an offset
- **Display-only values**: convert them at the boundary layer, and do not write them back into the source-of-truth data

### 3. Conversion logic fixes

Ensure the conversion happens once and only once:

- Normalize external input on ingress
- Keep one trusted internal representation
- Convert again on egress based on consumer semantics
- Avoid calling `ToUniversalTime` on values that are already UTC
- Avoid treating local times without timezone information as UTC

### 4. Timezone mapping fixes

Cross-platform scenarios must handle Windows/IANA mapping explicitly:

- Do not send `China Standard Time` directly to components that only understand IANA IDs
- Do not assume `Asia/Shanghai` can be used directly in every Windows API
- Centralize the mapping logic instead of letting each service maintain its own copy

### 5. Scheduling fixes

Scheduled jobs, business hours, and report cutoffs must be designed with timezone semantics:

- Store business rules plus timezone ID instead of storing only the next UTC execution time
- Recompute the next trigger time after DST transitions
- Define an explicit policy for nonexistent or duplicated local times

### 6. Windows multi-DLL global-state fixes

When timezone behavior depends on shared state across DLLs:

- Avoid relying on mutable timezone globals as cross-module source of truth
- Prefer the `TZ` environment variable as the process-level timezone configuration channel on Windows
- Initialize and normalize `TZ` early, then load or initialize dependent modules
- Ensure timezone caches are refreshed consistently after `TZ` changes

## Code Review

### 1. Highest-risk review targets

- The code uses the system default timezone without documenting that behavior
- The current offset is used as a substitute for timezone rules
- Time strings carry no offset and no explicit timezone contract
- API responses return time values without an offset or timezone ID
- The database stores local time, but field names, comments, and API documentation do not say so
- Historical time calculations reuse the current timezone rules directly
- Scheduled logic uses `now + 24h` to infer the same local time tomorrow
- Timezone globals/statics are duplicated across DLLs on Windows
- One DLL writes timezone state while other DLLs continue using stale per-module state
- Timezone cache invalidation is missing after `TZ` updates

### 2. Language- and framework-specific review points

#### .NET

- Watch for `DateTime.Now`
- Watch for `DateTimeKind.Unspecified`
- For absolute instants, prefer `DateTimeOffset` or an explicitly UTC `DateTime`
- When timezone rules are involved, verify whether the `TimeZoneInfo` input ID is Windows or IANA

#### Java

- Prefer `Instant`, `OffsetDateTime`, and `ZonedDateTime`
- Watch for `java.util.Date`, `Calendar`, and `LocalDateTime`
- Check whether `ZoneId.systemDefault()` is being treated as a stable dependency

#### JavaScript / TypeScript

- Watch for direct reliance on the local-time behavior of `Date`
- Watch for parsing strings with no offset
- Check whether frontend and backend apply different default timezones to the same field

#### Go

- Check whether `time.Parse` loses `Location`
- Check for incorrect use of `time.Local`
- Check whether serialization keeps only strings and drops semantics

#### Python

- Watch for naive `datetime`
- Prefer aware `datetime` and `zoneinfo`
- Check whether local time and UTC time are mixed in comparisons

#### Windows native / C modules

- Check whether timezone state is process-global or duplicated per DLL
- Check whether `TZ` is read consistently at startup and after runtime updates
- Check whether timezone conversion helpers depend on hidden mutable global state

### 3. Recommended review output

Prefer this structure in a review result:

1. Symptom and root-cause assessment
2. Severity and impact scope
3. Exact code locations and problem types
4. Recommended remediation
5. Required tests

## TDengine API Touchpoints

When available in your codebase, provide a short function-level API overview for these modules:

| Module | Typical API responsibility | What to verify in review |
|------|------|------|
| `ttime.c` | Core time parse/format and time conversion helpers | Input/output semantics, timezone standard assumptions, DST edge handling |
| `osTime.c` (sometimes written as `osTIme.c`) | OS time abstraction (clock retrieval, precision, monotonic vs wall-clock) | Correct clock source, UTC/local semantics, thread safety |
| `osTimezone.c` | Timezone initialization, `TZ` handling, offset and DST resolution | Single source of truth, cache strategy, cross-DLL consistency |

For each public interface function in these files, document:

- Purpose
- Input semantic type (instant, local time, offset, timezone ID)
- Output semantic type
- Timezone standard used (POSIX, ISO 8601, Windows ID, IANA ID)
- Thread-safety and cache behavior

## Current Design Principles

Use the following as the current recommended standard:

### 1. Model absolute time and business time separately

- Absolute time is used for ordering, audit, idempotency, and event timelines
- Business time is used for operating hours, day-cut logic, shifts, notifications, and reporting rules
- One field must not carry both semantics at the same time

### 2. Use UTC for storage and internal system collaboration

- The storage layer should persist absolute instants in UTC by default
- When systems exchange instants, prefer ISO 8601 with offset information
- Do not store local time just because it looks more convenient in the database

### 3. A timezone ID expresses rules; an offset does not

- `UTC+08:00` is only an offset at one moment, not a full timezone
- Any time value with business semantics must carry a timezone ID
- Windows and IANA ID mappings must be centrally governed

### 4. Conversions should happen only at boundaries

- Parse once on ingress
- Render once on egress
- Do not bounce repeatedly between UTC, local time, and string forms in core business logic

### 5. Do not rely on implicit defaults

- Do not rely implicitly on the system default timezone
- Do not rely implicitly on the region of the runtime node
- Do not rely implicitly on default conversion behavior in database drivers or serialization libraries

### 6. Boundary tests are mandatory

- DST start and end
- Historical rule changes
- Collaboration across different user timezones
- Interoperability between Windows and non-Windows environments
- Policies for nonexistent or duplicated local times

### 7. Monitor timezone rule changes

- Track timezone rule changes for business-critical regions
- Regress critical time scenarios after Windows patches or runtime upgrades
- Version the timezone mapping table and scheduling logic

### 8. Avoid mutable cross-DLL timezone globals on Windows

- Assume timezone-related globals/statics can drift when maintained independently across DLLs
- Keep timezone state explicit in interfaces whenever possible
- Treat hidden global timezone state as a reliability risk

### 9. Use `TZ` as the process-level timezone channel on Windows

- Prefer `TZ` to coordinate timezone configuration across modules
- Define startup order so timezone configuration is initialized before dependent logic
- Define runtime update behavior and cache refresh policy

### 10. Distinguish POSIX timezone syntax from ISO 8601 offsets

- TDengine timezone configuration follows POSIX timezone conventions
- Do not assume POSIX `TZ` strings are equivalent to ISO 8601 offset notation
- Example: POSIX `TZ=UTC-8` corresponds to ISO 8601 `+08:00` semantics
- Every timezone field and parser must declare which standard it uses

## Output

Use the following fixed output structure:

```yaml
problem_category: <fixed-offset|dst-boundary|mapping-mismatch|scheduler-drift|historical-rule|multi-dll-global-state>
confirmed_facts:
	- <fact-1>
	- <fact-2>
root_cause_hypothesis: <most-likely-cause>
timezone_standard_decision:
	configured_standard: <POSIX|ISO8601|Windows-ID|IANA-ID>
	evidence: <where-this-was-observed>
remediation_plan:
	- <action-1>
	- <action-2>
required_tests:
	- <test-1>
	- <test-2>
open_risks:
	- <risk-1>
```

Acceptance standard:

- Root-cause layer is explicitly identified
- Timezone standard decision is explicit and evidence-backed
- Multi-DLL and `TZ` behavior is evaluated for Windows paths
- Required boundary tests are listed (DST and historical rule cases at minimum)

## Safety

- Do not change field semantics before confirming what the field means
- Do not treat offset compensation as the final fix
- Do not declare the issue closed before DST boundary regression tests pass
- Do not assume that converting everything to UTC is sufficient for every scenario

High-risk operation confirmation flow:

1. For any write operation (config updates, migration scripts, production scheduler changes), provide a dry-run or read-only preview first.
2. Explicitly state impact scope and rollback plan before execution.
3. Require explicit user confirmation before applying non-read-only changes.

## References

See `CHEATSHEET.md` for a quick troubleshooting checklist and `scripts/collect-timezone-context.ps1` for Windows environment collection.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-windows-timezone version=1.0.0 author=wpan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
