# 禁用 Hyper-V 时间同步服务
Stop-Service vmictimesync -Force
Set-Service vmictimesync -StartupType Disabled

# 禁用 Windows Time 服务
Stop-Service w32time -Force
Set-Service w32time -StartupType Disabled

# 禁用注册表中的时间提供者
reg add "HKLM\SYSTEM\CurrentControlSet\Services\W32Time\TimeProviders\VMICTimeProvider" /v Enabled /t REG_DWORD /d 0 /f
reg add "HKLM\SYSTEM\CurrentControlSet\Services\W32Time\TimeProviders\NtpClient" /v Enabled /t REG_DWORD /d 0 /f

# 修改系统时间到许可证有效期内
Set-Date -Date "2025-04-18 07:05:00"
Write-Host "System time set to: $(Get-Date)"

# 重启 PI 服务
Write-Host "Stopping PI services..."
Get-Service PI* | Stop-Service -Force
iisreset /stop

Write-Host "Starting PI services..."
Get-Service PI* | Where-Object {$_.StartType -eq 'Automatic'} | Start-Service
iisreset /start

# 等待 PI Server 完全就绪
Write-Host "Waiting for PI Server to be fully ready..."
$maxRetries = 12
$retryInterval = 5
$connected = $false

Add-Type -AssemblyName 'OSIsoft.AFSDK, Version=4.0.0.0, Culture=neutral, PublicKeyToken=6238be57836698e6'

for ($i = 1; $i -le $maxRetries; $i++) {
    try {
        $piServers = New-Object OSIsoft.AF.PI.PIServers
        $piServer = $piServers['piserver']
        $piServer.Connect()
        $connected = $true
        Write-Host "PI Server connected successfully after $($i * $retryInterval) seconds."
        break
    } catch {
        Write-Host "  Attempt $i/$maxRetries - PI Server not ready yet, retrying in ${retryInterval}s..."
        Start-Sleep -Seconds $retryInterval
    }
}

if (-not $connected) {
    Write-Host "WARNING: Could not connect to PI Server after $($maxRetries * $retryInterval) seconds."
    Write-Host "         Please wait and retry cleanup_pi.ps1 manually."
}

Write-Host "Done! Current time: $(Get-Date)"
