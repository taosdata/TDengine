# modules-windows/java.ps1 — JDK + Maven toolchain

function Mod-java-Check {
    Write-Header 'Java Toolchain'

    if (Test-CommandExists 'java') {
        $verOutput = (java -version 2>&1 | Select-Object -First 1) -join ''
        $ver = [regex]::Match($verOutput, '(\d+\.\d+\.\d+)').Groups[1].Value
        $major = [int]($ver.Split('.')[0])
        if ($major -ge $script:RequiredJavaVersion) {
            Write-Ok "java $ver (major $major >= $($script:RequiredJavaVersion))"
        } else {
            Write-Warn "java $ver (need major >= $($script:RequiredJavaVersion))"
            $script:IssuesFound++
        }
    } else {
        Write-Fail 'java not found'
        $script:IssuesFound++
    }

    if (Test-CommandExists 'mvn') {
        $ver = (mvn --version 2>$null | Select-Object -First 1) -replace '.*?(\d+\.\d+\.\d+).*', '$1'
        Write-Ok "mvn $ver"
    } else {
        Write-Warn 'mvn not found'
        $script:IssuesFound++
    }
}

function Mod-java-Install {
    if (-not (Test-CommandExists 'java')) {
        if (Confirm-Action "Install OpenJDK $($script:RequiredJavaVersion)?") {
            if (Test-CommandExists 'winget') {
                Install-WithWinget "Microsoft.OpenJDK.$($script:RequiredJavaVersion)"
            } else {
                Install-WithChoco @("openjdk$($script:RequiredJavaVersion)")
            }
            $script:ChangesMade++
        }
    }

    if (-not (Test-CommandExists 'mvn')) {
        if (Confirm-Action 'Install Maven?') {
            if (Test-CommandExists 'choco') {
                Install-WithChoco @('maven')
            } elseif (Test-CommandExists 'winget') {
                Install-WithWinget 'Apache.Maven'
            }
            $script:ChangesMade++
        }
    }
}

function Mod-java-Config {
    $settingsFile = Join-Path $env:USERPROFILE '.m2\settings.xml'
    $nexusMavenUrl = 'https://nexus.tdengine.net/repository/maven-public/'

    if ((Test-Path $settingsFile) -and (Select-String -Path $settingsFile -Pattern 'nexus\.tdengine\.net' -Quiet)) {
        return
    }

    if (Confirm-Action "Configure Maven mirror → internal Nexus in $settingsFile?") {
        $m2Dir = Split-Path $settingsFile -Parent
        if (-not (Test-Path $m2Dir)) { New-Item -ItemType Directory -Path $m2Dir -Force | Out-Null }

        if (Test-Path $settingsFile) {
            Copy-Item $settingsFile "$settingsFile.bak"
            Write-Info "Backed up $settingsFile"
        }

        @"
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.2.0
                              https://maven.apache.org/xsd/settings-1.2.0.xsd">
  <mirrors>
    <mirror>
      <id>nexus-tdengine</id>
      <mirrorOf>central</mirrorOf>
      <name>TDengine Internal Nexus</name>
      <url>$nexusMavenUrl</url>
    </mirror>
  </mirrors>
</settings>
"@ | Set-Content $settingsFile -Encoding UTF8
        Write-Ok "Maven settings written to $settingsFile"
        $script:ChangesMade++
    }
}
