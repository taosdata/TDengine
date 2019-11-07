param($installPath, $toolsPath, $package, $project)
Write-Host "Starting ExcelDna.Integration install script"

Write-Host "`tSet reference to ExcelDna.Integration to be CopyLocal=false"
$project.Object.References | Where-Object { $_.Name -eq 'ExcelDna.Integration' } | ForEach-Object { $_.CopyLocal = $false }

Write-Host "Completed ExcelDna.Integration install script"