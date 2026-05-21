# modules-windows/rust.ps1 — Rust toolchain + Nora registry + sccache

function Mod-rust-Check {
    Write-Header 'Rust Toolchain'

    # rustc
    if (Test-CommandExists 'rustc') {
        $ver = (rustc --version) -replace '.*?(\d+\.\d+\.\d+).*', '$1'
        if (Test-VersionGte $ver $script:RequiredRustVersion) {
            Write-Ok "rustc $ver (>= $($script:RequiredRustVersion))"
        } else {
            Write-Warn "rustc $ver (need >= $($script:RequiredRustVersion))"
            $script:IssuesFound++
        }
    } else {
        Write-Fail 'rustc not found'
        $script:IssuesFound++
    }

    # cargo
    if (Test-CommandExists 'cargo') {
        $ver = (cargo --version 2>$null) -replace '.*?(\d+\.\d+\.\d+).*', '$1'
        Write-Ok "cargo $ver"
    } else {
        Write-Fail 'cargo not found'
        $script:IssuesFound++
    }

    # cargo config → Nora
    $cargoConfig = Join-Path $env:USERPROFILE '.cargo\config.toml'
    if ((Test-Path $cargoConfig) -and (Select-String -Path $cargoConfig -Pattern 'nora\.tdengine\.net' -Quiet)) {
        Write-Ok 'Cargo registry → Nora (internal)'
    } else {
        Write-Warn 'Cargo not configured for internal Nora registry'
        $script:IssuesFound++
    }

    # protoc
    if (Test-CommandExists 'protoc') {
        $ver = (protoc --version 2>$null) -replace '.*?(\d+\.\d+(\.\d+)?).*', '$1'
        Write-Ok "protoc $ver"
    } else {
        Write-Warn 'protoc not found (required for taosx gRPC)'
        $script:IssuesFound++
    }

    # sccache
    if (Test-CommandExists 'sccache') {
        $ver = (sccache --version 2>$null) -replace '.*?(\d+\.\d+\.\d+).*', '$1'
        Write-Ok "sccache $ver"
    } else {
        Write-Info 'sccache not installed (optional)'
    }
}

function Mod-rust-Install {
    # rustup
    if (-not (Test-CommandExists 'rustc') -or
        -not (Test-VersionGte ((rustc --version) -replace '.*?(\d+\.\d+\.\d+).*', '$1') $script:RequiredRustVersion)) {
        if (Confirm-Action 'Install/upgrade Rust via rustup?') {
            if (Test-CommandExists 'rustup') {
                rustup update stable
            } else {
                Write-Info 'Downloading rustup-init.exe...'
                $rustupUrl = 'https://win.rustup.rs/x86_64'
                $rustupExe = Join-Path $env:TEMP 'rustup-init.exe'
                Invoke-WebRequest -Uri $rustupUrl -OutFile $rustupExe -UseBasicParsing
                & $rustupExe -y --default-toolchain stable
                Remove-Item $rustupExe -ErrorAction SilentlyContinue
                # Refresh PATH
                $env:PATH = "$env:USERPROFILE\.cargo\bin;$env:PATH"
            }
            $script:ChangesMade++
        }
    }

    # protoc
    if (-not (Test-CommandExists 'protoc')) {
        if (Confirm-Action 'Install protoc?') {
            if (Test-CommandExists 'choco') {
                Install-WithChoco @('protoc')
            } elseif (Test-CommandExists 'winget') {
                Install-WithWinget 'Google.Protobuf'
            }
            $script:ChangesMade++
        }
    }
}

function Mod-rust-Config {
    $cargoConfig = Join-Path $env:USERPROFILE '.cargo\config.toml'

    if ((Test-Path $cargoConfig) -and (Select-String -Path $cargoConfig -Pattern 'nora\.tdengine\.net' -Quiet)) {
        return
    }

    # Try to copy from tsdb-builder source
    $cargoConfigSrc = Join-Path $script:SetupDir '..\tsdb-builder\.cargo\config.toml'

    if (Confirm-Action "Write Cargo config to $cargoConfig?") {
        $cargoDir = Split-Path $cargoConfig -Parent
        if (-not (Test-Path $cargoDir)) { New-Item -ItemType Directory -Path $cargoDir -Force | Out-Null }

        if (Test-Path $cargoConfig) {
            Copy-Item $cargoConfig "$cargoConfig.bak"
            Write-Info "Backed up $cargoConfig → $cargoConfig.bak"
        }

        if (Test-Path $cargoConfigSrc) {
            Copy-Item $cargoConfigSrc $cargoConfig -Force
        } else {
            @"
[source.crates-io]
replace-with = 'internal'

[source.internal]
registry = "sparse+https://nora.tdengine.net/cargo/index/"

[registries.internal]
index = "sparse+https://nora.tdengine.net/cargo/index/"

[http]
multiplexing = false
timeout = 120

[net]
git-fetch-with-cli = true
"@ | Set-Content $cargoConfig -Encoding UTF8
        }
        Write-Ok "Cargo config written to $cargoConfig"
        $script:ChangesMade++
    }
}
