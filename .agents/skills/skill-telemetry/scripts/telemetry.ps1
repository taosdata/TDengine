# skill-telemetry client. Usage: telemetry.ps1 <name> <version> <author>
param(
    [Parameter(Mandatory)][string]$Name,
    [Parameter(Mandatory)][string]$Version,
    [Parameter(Mandatory)][string]$Author
)

$ErrorActionPreference = 'SilentlyContinue'

# --- Agent ---
$agent = 'unknown'
if      ($env:CLAUDE_AGENT_NAME)   { $agent = $env:CLAUDE_AGENT_NAME }
elseif  ($env:CODEX_AGENT_NAME)    { $agent = $env:CODEX_AGENT_NAME }
elseif  ($env:COPILOT_AGENT_NAME)  { $agent = $env:COPILOT_AGENT_NAME }
elseif  ($env:CLAUDECODE)          { $agent = 'claude-code' }
elseif  ($env:CURSOR_TRACE_ID)     { $agent = 'cursor' }
elseif  ($env:AIDER)               { $agent = 'aider' }
elseif  ($env:WARP_CLIENT_VERSION) { $agent = 'warp' }
elseif  ($env:ZED_TERM)            { $agent = 'zed' }
elseif  ($env:GEMINI_CLI)          { $agent = 'gemini-cli' }
elseif  ($env:TERM_PROGRAM -in @('Windsurf','WindsurfNext')) { $agent = 'windsurf' }

# --- Local IP ---
$lip = 'unknown'
try {
    $ip = Get-NetIPAddress -AddressFamily IPv4 |
        Where-Object { $_.IPAddress -ne '127.0.0.1' } |
        Select-Object -First 1 -ExpandProperty IPAddress
    if ($ip) { $lip = $ip }
} catch {}

# --- Distro ---
$distro = 'unknown'
try { $distro = (Get-CimInstance Win32_OperatingSystem).Caption } catch {}

# --- Send ---
$uri = if ($env:SKILL_TELEMETRY_URL) { $env:SKILL_TELEMETRY_URL } else { 'https://teleskills.tdengine.net' }
try {
    $body = @{
        name     = $Name
        version  = $Version
        author   = $Author
        agent    = $agent
        os       = 'windows'
        distro   = $distro
        local_ip = $lip
    } | ConvertTo-Json -Compress
    Invoke-RestMethod -Uri "$uri/api/v1/skills/telemetry" -Method Post `
        -ContentType 'application/json' -TimeoutSec 3 -Body $body | Out-Null
} catch {}
