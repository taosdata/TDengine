# TDengine CDR Auto-Refresh Extension for PI Vision
# 安装脚本 — 以管理员身份运行
#
# 用法:
#   .\install.ps1                              # 使用默认 PI Vision 路径
#   .\install.ps1 -PIVisionPath "D:\PIVision"  # 指定自定义路径

param(
    [string]$PIVisionPath = "C:\Program Files\PIPC\PIVision"
)

$ErrorActionPreference = "Stop"

$extDir = Join-Path $PIVisionPath "Scripts\app\editor\symbols\ext"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$sourceDir = Join-Path $scriptDir "ext"

# 检查 PI Vision 路径
if (-not (Test-Path $PIVisionPath)) {
    Write-Host "[ERROR] PI Vision not found at: $PIVisionPath" -ForegroundColor Red
    Write-Host "Use -PIVisionPath to specify the correct location." -ForegroundColor Yellow
    exit 1
}

# 确保 ext 目录存在
if (-not (Test-Path $extDir)) {
    New-Item -ItemType Directory -Path $extDir -Force | Out-Null
    Write-Host "[INFO] Created ext directory: $extDir" -ForegroundColor Cyan
}

# 复制文件
$files = @("sym-tdrefresh.js", "sym-tdrefresh-template.html")
foreach ($file in $files) {
    $src = Join-Path $sourceDir $file
    $dst = Join-Path $extDir $file

    if (-not (Test-Path $src)) {
        Write-Host "[ERROR] Source file not found: $src" -ForegroundColor Red
        exit 1
    }

    Copy-Item -Path $src -Destination $dst -Force
    Write-Host "[OK] Copied: $file -> $extDir" -ForegroundColor Green
}

# 重启 IIS
Write-Host ""
Write-Host "[INFO] Restarting IIS..." -ForegroundColor Cyan
iisreset /restart 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "[WARN] IIS restart returned exit code $LASTEXITCODE. Please run 'iisreset' manually." -ForegroundColor Yellow
} else {
    Write-Host "[OK] IIS restarted successfully." -ForegroundColor Green
}

Write-Host ""
Write-Host "=== Installation Complete ===" -ForegroundColor Green
Write-Host "Refresh your PI Vision browser page (Ctrl+F5) to activate." -ForegroundColor Cyan
