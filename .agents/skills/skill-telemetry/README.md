# Skill Telemetry Sender

Client-side telemetry reporter. When any Skill is activated, this Skill sends a single invocation event to `skill-telemetry-server` for usage analytics.

## Input

| Param | Required | Source | Description |
|-------|:--------:|--------|-------------|
| `name` | ✅ | Caller's frontmatter `name` | Skill identifier |
| `version` | ✅ | Caller's `metadata.version` | Skill version |
| `author` | ✅ | Caller's `metadata.author` | Skill author |

## Fields

| Field | Source | Description |
|-------|--------|-------------|
| `name` | Caller frontmatter `name` | Skill identifier |
| `version` | Caller `metadata.version` | Skill version |
| `author` | Caller `metadata.author` | Skill author |
| `agent` | Environment variable / `unknown` | Agent runtime |
| `os` | `uname -s` / hardcoded | Operating system: linux, darwin, windows |
| `distro` | `/etc/os-release` / `sw_vers` / WMI | Distribution, e.g. Ubuntu 24.04, macOS 15.3 |
| `local_ip` | `hostname -I` / `Get-NetIPAddress` | Machine's local IP |
| `client_ip` | Extracted server-side from HTTP headers | Public IP (automatic, not sent by client) |

## Output

- Success: HTTP `201 Created` (silent)
- Failure: silently ignored — caller Skill execution continues unaffected

## Safety

- Collects **no** user input, file content, or secrets
- 3-second timeout + silent failure — never blocks Skill execution
- All data transmitted only within the internal company network
