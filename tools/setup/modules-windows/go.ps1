# modules-windows/go.ps1 — Go toolchain + GOPROXY configuration

function Mod-go-Check {
    Write-Header 'Go Toolchain'

    if (Test-CommandExists 'go') {
        $ver = (go version) -replace '.*go(\d+\.\d+(\.\d+)?).*', '$1'
        if (Test-VersionGte $ver $script:RequiredGoVersion) {
            Write-Ok "go $ver (>= $($script:RequiredGoVersion))"
        } else {
            Write-Warn "go $ver (need >= $($script:RequiredGoVersion))"
            $script:IssuesFound++
        }
    } else {
        Write-Fail 'go not found'
        $script:IssuesFound++
    }

    # GOPROXY
    $goproxy = $env:GOPROXY
    if ($goproxy -and $goproxy -match 'nexus\.tdengine\.net') {
        Write-Ok 'GOPROXY configured to internal proxy'
    } else {
        Write-Warn 'GOPROXY not set to internal proxy'
        Write-Info "Expected: $($script:GoProxy),direct"
        $script:IssuesFound++
    }
}

function Mod-go-Install {
    if (Test-CommandExists 'go') {
        $ver = (go version) -replace '.*go(\d+\.\d+(\.\d+)?).*', '$1'
        if (Test-VersionGte $ver $script:RequiredGoVersion) { return }
    }

    if (Confirm-Action 'Install/upgrade Go?') {
        if (Test-CommandExists 'winget') {
            Install-WithWinget 'GoLang.Go'
        } else {
            Install-WithChoco @('golang')
        }
        $script:ChangesMade++
    }
}

function Mod-go-Config {
    $expected = "$($script:GoProxy),direct"
    $current = $env:GOPROXY

    if ($current -match 'nexus\.tdengine\.net') { return }

    if (Confirm-Action "Set GOPROXY to internal proxy (system environment variable)?") {
        [System.Environment]::SetEnvironmentVariable('GOPROXY', $expected, 'User')
        [System.Environment]::SetEnvironmentVariable('GONOSUMDB', 'github.com/taosdata/*', 'User')
        [System.Environment]::SetEnvironmentVariable('GONOSUMCHECK', 'github.com/taosdata/*', 'User')
        $env:GOPROXY = $expected
        Write-Ok "GOPROXY set to $expected"
        $script:ChangesMade++
    }
}
