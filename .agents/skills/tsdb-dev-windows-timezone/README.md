# tsdb-dev-windows-timezone

This skill is used to investigate and govern Windows timezone issues across the following areas:

- Problem diagnosis
- Problem remediation
- Code review
- Current design-principle alignment

## Scope

This skill targets common Windows time-related issues:

- UTC and local-time conversion errors
- DST transition failures
- Windows timezone ID and IANA timezone ID mapping issues
- Semantic mismatches across APIs, databases, frontend rendering, and scheduling flows
- Cross-platform inconsistencies

## Windows-specific constraints

- On Windows, mutable globals can become effectively duplicated across multiple DLLs.
- Timezone-related global-state usage must be reviewed carefully for per-module copies and stale state.
- Prefer `TZ` as the process-level timezone configuration channel to reduce cross-DLL divergence.
- **cmd.exe vs Cygwin terminal**: The Windows default terminal (cmd.exe) does not set the `TZ` environment variable by default, whereas Cygwin automatically sets `TZ` based on the system timezone. This difference can cause taosc to take different internal code paths on the same Windows machine, potentially masking or surfacing timezone-related bugs depending on which terminal is used for testing.

## Timezone standards used by TDengine

- TDengine timezone configuration follows POSIX timezone conventions.
- POSIX timezone notation is not the same as ISO 8601 offset notation.
- Reviewers must explicitly identify which standard each field, parser, and API expects.

## Files

- `SKILL.md`: primary skill definition covering diagnosis, remediation, review, and design principles
- `CHEATSHEET.md`: quick diagnosis and review checklist
- `scripts/collect-timezone-context.ps1`: PowerShell script for collecting Windows timezone environment details

## Usage

### Use in an agent workflow

Read `SKILL.md` as context and follow its workflow:

1. Identify the problem type
2. Collect time semantics and data-flow details
3. Collect the Windows environment
4. Determine the root-cause layer
5. Produce remediation and review guidance

### Collect Windows environment details manually

```powershell
powershell -ExecutionPolicy Bypass -File .\wpan\skills\tsdb-dev-windows-timezone\scripts\collect-timezone-context.ps1
```

## Repository layout compliance

This skill follows the project-level layout requirement:

- `wpan/SKILLS.md`
- `wpan/skills/tsdb-dev-windows-timezone/SKILL.md`
- supporting files under `wpan/skills/tsdb-dev-windows-timezone/`

## Current recommended principles

1. Store absolute instants in UTC
2. Any business-local time must carry a timezone ID
3. An offset is not a timezone and cannot replace timezone rules
4. Time conversion should happen only at boundaries
5. DST and historical rule-change tests are mandatory

## TDengine API touchpoints to document

When writing review notes, include short API introductions for:

- `ttime.c`: time parse/format and conversion helpers
- `osTime.c` (or `osTIme.c` in some references): OS time abstraction functions
- `osTimezone.c`: timezone initialization and `TZ` handling functions

For each key interface function, document purpose, input/output semantics, and timezone standard assumptions.

## Useful input to provide with this skill

- Reproduction steps
- Sample time values
- User region
- Windows timezone configuration
- Relevant code snippets
- Raw database values and API responses