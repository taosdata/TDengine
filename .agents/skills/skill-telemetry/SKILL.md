---
name: skill-telemetry
description: "Client-side telemetry reporter for Skill invocations. Sends usage events to telemetry-server when any Skill is activated. Keywords: telemetry, usage report, skill tracking"
metadata:
  author: Linhe Huo
  version: 0.2.0
  owner_team: ai
---

# skill-telemetry

Client-side telemetry reporter. When any Skill is activated, this Skill sends a single invocation event to `skill-telemetry-server` for usage analytics.

> **Privacy**: Only Skill metadata (name, version, author) and machine identifiers (OS, IP) are reported.
> No user input, file content, or secrets are collected. All data stays on the internal network.

## When to Use

Triggered automatically by the `## Telemetry (MUST)` section in every Skill. No user interaction required.

## Input

| Param | Required | Source | Description |
|-------|:--------:|--------|-------------|
| `name` | ✅ | Caller's frontmatter `name` | Skill identifier |
| `version` | ✅ | Caller's `metadata.version` | Skill version |
| `author` | ✅ | Caller's `metadata.author` | Skill author |

## Agent Execution Rule

**Agent MUST run the telemetry command below BEFORE executing the caller Skill's logic.**
Mandatory — must not be skipped. Failure is non-blocking but the attempt is required.

Replace `{{SKILL_NAME}}`, `{{SKILL_VERSION}}`, `{{SKILL_AUTHOR}}` with actual values from the caller Skill's frontmatter.

> The agent MUST resolve the path to `scripts/telemetry.py` (or `scripts/telemetry.ps1` on Windows)
> relative to this SKILL.md file. If the path cannot be resolved, set `SKILL_TELEMETRY_SCRIPT`
> to the absolute path. If both fail, skip telemetry silently.

**Linux / macOS:**

```bash
python3 "${SKILL_TELEMETRY_SCRIPT:-<resolve scripts/telemetry.py>}" "{{SKILL_NAME}}" "{{SKILL_VERSION}}" "{{SKILL_AUTHOR}}" 2>/dev/null &
```

**Windows:**

```powershell
& "${env:SKILL_TELEMETRY_SCRIPT:-<resolve scripts/telemetry.ps1>}" '{{SKILL_NAME}}' '{{SKILL_VERSION}}' '{{SKILL_AUTHOR}}'
```

