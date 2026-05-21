# modules-windows/node.ps1 — Node.js + pnpm toolchain

function Mod-node-Check {
    Write-Header 'Node.js Toolchain'

    if (Test-CommandExists 'node') {
        $ver = (node --version) -replace 'v', ''
        if (Test-VersionGte $ver $script:RequiredNodeVersion) {
            Write-Ok "node $ver (>= $($script:RequiredNodeVersion))"
        } else {
            Write-Warn "node $ver (need >= $($script:RequiredNodeVersion))"
            $script:IssuesFound++
        }
    } else {
        Write-Fail 'node not found'
        $script:IssuesFound++
    }

    if (Test-CommandExists 'pnpm') {
        Write-Ok "pnpm $(pnpm --version 2>$null)"
    } else {
        Write-Warn 'pnpm not found'
        $script:IssuesFound++
    }

    # npm registry
    if (Test-CommandExists 'npm') {
        $registry = npm config get registry 2>$null
        if ($registry -match 'nora\.tdengine\.net') {
            Write-Ok 'npm registry → internal mirror'
        } else {
            Write-Info "npm registry: $registry (public)"
        }
    }
}

function Mod-node-Install {
    if (-not (Test-CommandExists 'node') -or
        -not (Test-VersionGte ((node --version) -replace 'v', '') $script:RequiredNodeVersion)) {
        if (Confirm-Action 'Install/upgrade Node.js?') {
            if (Test-CommandExists 'winget') {
                Install-WithWinget 'OpenJS.NodeJS.LTS'
            } else {
                Install-WithChoco @('nodejs-lts')
            }
            $script:ChangesMade++
        }
    }

    if (-not (Test-CommandExists 'pnpm')) {
        if (Confirm-Action 'Install pnpm?') {
            if (Test-CommandExists 'corepack') {
                corepack enable 2>$null
                corepack prepare pnpm@latest --activate 2>$null
            } elseif (Test-CommandExists 'npm') {
                npm install -g pnpm
            }
            $script:ChangesMade++
        }
    }
}

function Mod-node-Config {
    if (-not (Test-CommandExists 'npm')) { return }

    $noraNpmUrl = 'https://nora.tdengine.net/npm/'
    $currentRegistry = npm config get registry 2>$null

    if ($currentRegistry -match 'nora\.tdengine\.net') { return }

    if (Confirm-Action 'Set npm registry → internal Nora mirror?') {
        npm config set registry $noraNpmUrl
        Write-Ok "npm registry set to $noraNpmUrl"
        $script:ChangesMade++
    }
}
