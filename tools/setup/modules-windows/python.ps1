# modules-windows/python.ps1 — Python3 + pip + maturin toolchain

function Mod-python-Check {
    Write-Header 'Python Toolchain'

    # python3 or python
    $pythonCmd = if (Test-CommandExists 'python3') { 'python3' }
                 elseif (Test-CommandExists 'python') { 'python' }
                 else { $null }

    if ($pythonCmd) {
        $ver = (& $pythonCmd --version 2>&1) -replace '.*?(\d+\.\d+\.\d+).*', '$1'
        if (Test-VersionGte $ver $script:RequiredPythonVersion) {
            Write-Ok "$pythonCmd $ver (>= $($script:RequiredPythonVersion))"
        } else {
            Write-Warn "$pythonCmd $ver (need >= $($script:RequiredPythonVersion))"
            $script:IssuesFound++
        }
    } else {
        Write-Fail 'python not found'
        $script:IssuesFound++
    }

    # pip
    if (Test-CommandExists 'pip3' -or Test-CommandExists 'pip') {
        $pipCmd = if (Test-CommandExists 'pip3') { 'pip3' } else { 'pip' }
        $ver = (& $pipCmd --version 2>$null) -replace '.*?(\d+\.\d+).*', '$1'
        Write-Ok "pip $ver"
    } else {
        Write-Warn 'pip not found'
        $script:IssuesFound++
    }

    # maturin
    if (Test-CommandExists 'maturin') {
        $ver = (maturin --version 2>$null) -replace '.*?(\d+\.\d+\.\d+).*', '$1'
        Write-Ok "maturin $ver"
    } else {
        Write-Info 'maturin not installed (needed for connector-python)'
    }

    # pip index
    $pipCmd = if (Test-CommandExists 'pip3') { 'pip3' } elseif (Test-CommandExists 'pip') { 'pip' } else { $null }
    if ($pipCmd) {
        $index = & $pipCmd config get global.index-url 2>$null
        if ($index -match 'nora\.tdengine\.net') {
            Write-Ok 'pip index → internal Nora mirror'
        }
    }
}

function Mod-python-Install {
    $pythonCmd = if (Test-CommandExists 'python3') { 'python3' }
                 elseif (Test-CommandExists 'python') { 'python' }
                 else { $null }

    if (-not $pythonCmd) {
        if (Confirm-Action 'Install Python 3?') {
            if (Test-CommandExists 'winget') {
                Install-WithWinget 'Python.Python.3.12'
            } else {
                Install-WithChoco @('python3')
            }
            $script:ChangesMade++
        }
    }

    if (-not (Test-CommandExists 'maturin')) {
        if (Confirm-Action 'Install maturin (for connector-python)?') {
            pip install maturin 2>$null
            $script:ChangesMade++
        }
    }
}

function Mod-python-Config {
    $noraPypiUrl = 'https://nora.tdengine.net/simple/'
    $pipCmd = if (Test-CommandExists 'pip3') { 'pip3' } elseif (Test-CommandExists 'pip') { 'pip' } else { $null }

    if (-not $pipCmd) { return }

    $currentIndex = & $pipCmd config get global.index-url 2>$null
    if ($currentIndex -match 'nora\.tdengine\.net') { return }

    if (Confirm-Action 'Set pip index-url → internal Nora PyPI mirror?') {
        & $pipCmd config set global.index-url $noraPypiUrl 2>$null
        & $pipCmd config set global.trusted-host 'nora.tdengine.net' 2>$null
        Write-Ok "pip index set to $noraPypiUrl"
        $script:ChangesMade++
    }
}
