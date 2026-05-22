# =============================================================================
# DEPRECATED: Use tools/setup/setup-windows.ps1 instead.
#
# This script is kept for backward compatibility but is no longer maintained.
# The new setup framework provides:
#   - Modular per-language modules (cpp, go, rust, java, node, python, dotnet)
#   - Internal dependency source configuration (GOPROXY, Cargo/Nora, Conan, etc.)
#   - Component-based selection (-Component engine, taosx)
#   - Check-only mode (-CheckOnly)
#
# Migration:
#   .\tools\setup\setup-windows.ps1 -All
# =============================================================================
Write-Warning "DEPRECATED: This script has been superseded by tools/setup/setup-windows.ps1"
Write-Host "  Use: .\tools\setup\setup-windows.ps1 -All"
Write-Host ""
Write-Host "  Continuing with legacy script for backward compatibility..."
Write-Host ""
# =============================================================================
# TSDB (TDengine) full-component build dependency installation script (Windows)
#
# Supported platform: Windows 10/11 (uses winget + direct downloads)
#
# Usage:
#   # Run PowerShell as Administrator
#   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
#   .\install_deps.ps1            # Install all dependencies
#   .\install_deps.ps1 -Check     # Only check installed/missing tools
#
# Required tools per component:
#   Engine (taosd)             : cmake, Visual Studio Build Tools (C/C++ compiler)
#   taos-adapter / taos-keeper : go
#   taos-gen (taosbenchmark)   : conan (C++ package manager), cmake
#   taos-xservice (taosx)      : cargo (Rust), protoc
#   taos-insight               : go, mage, yarn, node (>=18 <=22)
#   taos-connector-dotnet      : dotnet
#   taos-connector-jdbc        : java (17+), maven
#   taos-connector-node        : node, npm
#   taos-connector-python      : python3, maturin, cargo (Rust >=1.85)
#   taos-connector-rust        : cargo (Rust)
#   taos-connector-odbc        : cmake, win_flex_bison
#
# Generated based on actual build and debug experience, 2026-04-01
# =============================================================================

param(
    [switch]$Check
)

$ErrorActionPreference = "Stop"

# ── Output utilities ──────────────────────────────────────────────────────
function Write-Info  { param([string]$Message) Write-Host "[OK] $Message" -ForegroundColor Green }
function Write-Warn  { param([string]$Message) Write-Host "[!]  $Message" -ForegroundColor Yellow }
function Write-Fail  { param([string]$Message) Write-Host "[X]  $Message" -ForegroundColor Red }

function Test-Command {
    param([string]$Name)
    $null = Get-Command $Name -ErrorAction SilentlyContinue
    return $?
}

function Refresh-Path {
    # Reload PATH from registry so newly installed tools are visible
    $machinePath = [Environment]::GetEnvironmentVariable("Path", "Machine")
    $userPath    = [Environment]::GetEnvironmentVariable("Path", "User")
    $env:Path    = "$machinePath;$userPath"

    # Also add common tool directories
    $npmGlobal = $null
    try { $npmGlobal = (npm config get prefix 2>&1 | Out-String).Trim() } catch {}
    $extraPaths = @(
        "$env:USERPROFILE\go\bin",
        "$env:USERPROFILE\.cargo\bin",
        "$env:LOCALAPPDATA\Programs\Python\Python312\Scripts",
        "$env:LOCALAPPDATA\Programs\Python\Python312",
        "$env:LOCALAPPDATA\Programs\Python\Python311\Scripts",
        "$env:LOCALAPPDATA\Programs\Python\Python311",
        "$env:ProgramFiles\Go\bin",
        "$env:ProgramFiles\CMake\bin",
        "$env:ProgramFiles\dotnet",
        "$env:LOCALAPPDATA\Programs\Maven\apache-maven-3.9.9\bin"
    )
    if ($npmGlobal) { $extraPaths += $npmGlobal }
    # Also detect Python Scripts dir dynamically
    try {
        $pyScripts = (python -c "import sysconfig; print(sysconfig.get_path('scripts'))" 2>&1 | Out-String).Trim()
        if ($pyScripts) { $extraPaths += $pyScripts }
    } catch {}
    foreach ($p in $extraPaths) {
        if ((Test-Path $p) -and ($env:Path -notlike "*$p*")) {
            $env:Path = "$p;$env:Path"
        }
    }
}

function Get-JavaMajorVersion {
    try {
        $output = & java --version 2>&1 | Select-Object -First 1
        if ($output -match '(\d+)(\.\d+)*') {
            return [int]$Matches[1]
        }
    } catch {}
    return 0
}

function Ensure-WinGet {
    if (Test-Command "winget") {
        Write-Info "winget already available"
        return
    }
    Write-Fail "winget is not available. Please install App Installer from Microsoft Store."
    Write-Fail "https://aka.ms/getwinget"
    throw "winget is required but not found"
}

# ── Check mode: list all tool statuses ────────────────────────────────────
function Check-All {
    Refresh-Path
    Write-Host ""
    Write-Host "=============================="
    Write-Host " Dependency Check"
    Write-Host "=============================="

    $tools = @(
        @{ Name = "cmake";    Cmd = "cmake --version" },
        @{ Name = "cl";       Cmd = "cl 2>&1 | Select-String 'Version'" },
        @{ Name = "go";       Cmd = "go version" },
        @{ Name = "cargo";    Cmd = "cargo --version" },
        @{ Name = "rustc";    Cmd = "rustc --version" },
        @{ Name = "java";     Cmd = "java --version 2>&1 | Select-Object -First 1" },
        @{ Name = "mvn";      Cmd = "mvn --version 2>&1 | Select-Object -First 1" },
        @{ Name = "node";     Cmd = "node --version" },
        @{ Name = "npm";      Cmd = "npm --version" },
        @{ Name = "yarn";     Cmd = "yarn --version" },
        @{ Name = "pnpm";     Cmd = "pnpm --version" },
        @{ Name = "dotnet";   Cmd = "dotnet --version" },
        @{ Name = "conan";    Cmd = "conan --version" },
        @{ Name = "maturin";  Cmd = "maturin --version" },
        @{ Name = "mage";     Cmd = "mage --version 2>&1 | Select-Object -First 1" },
        @{ Name = "python3";  Cmd = "python --version" },
        @{ Name = "protoc";   Cmd = "protoc --version" },
        @{ Name = "win_bison";Cmd = "win_bison --version 2>&1 | Select-Object -First 1" },
        @{ Name = "win_flex"; Cmd = "win_flex --version 2>&1 | Select-Object -First 1" }
    )

    $missing = 0
    foreach ($t in $tools) {
        $toolName = $t.Name
        # python3 on Windows is usually 'python'
        $checkName = if ($toolName -eq "python3") { "python" } else { $toolName }
        if (Test-Command $checkName) {
            try {
                $ver = Invoke-Expression $t.Cmd 2>$null | Out-String
                $ver = $ver.Trim()
                if ($toolName -eq "java") {
                    $jMajor = Get-JavaMajorVersion
                    if ($jMajor -lt 17) {
                        Write-Fail "$toolName  ->  $ver (requires 17+)"
                        $missing++
                        continue
                    }
                }
                Write-Info "$toolName  ->  $ver"
            } catch {
                Write-Info "$toolName  ->  found"
            }
        } else {
            Write-Fail "$toolName  ->  not found"
            $missing++
        }
    }

    Write-Host ""
    if ($missing -gt 0) {
        Write-Warn "$missing tool(s) missing. Run without -Check to install."
    } else {
        Write-Info "All tools are installed!"
    }
    return $missing
}

# ══════════════════════════════════════════════════════════════════════════
#  Windows installation functions
# ══════════════════════════════════════════════════════════════════════════

function Install-VisualStudioBuildTools {
    Write-Host ""
    Write-Host "-- Visual Studio Build Tools (C/C++ compiler) --"

    # Check if cl.exe is available (via VS Developer environment)
    $vsWhere = "${env:ProgramFiles(x86)}\Microsoft Visual Studio\Installer\vswhere.exe"
    $vsInstalled = $false
    if (Test-Path $vsWhere) {
        $vsPath = (& $vsWhere -latest -property installationPath 2>&1 | Out-String).Trim()
        if ($vsPath -and (Test-Path "$vsPath\VC\Tools\MSVC")) {
            $vsInstalled = $true
            Write-Info "Visual Studio Build Tools already installed at: $vsPath"
        }
    }
    if (-not $vsInstalled) {
        Write-Warn "Installing Visual Studio Build Tools..."
        Write-Warn "This will install the C++ desktop workload (may take a while)."
        try {
            winget install --id Microsoft.VisualStudio.2022.BuildTools `
                --override "--wait --passive --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended" `
                --accept-source-agreements --accept-package-agreements
            Write-Info "Visual Studio Build Tools installed"
        } catch {
            Write-Warn "winget install failed, trying direct download..."
            $installerUrl = "https://aka.ms/vs/17/release/vs_buildtools.exe"
            $installerPath = "$env:TEMP\vs_buildtools.exe"
            Invoke-WebRequest -Uri $installerUrl -OutFile $installerPath -UseBasicParsing
            Start-Process -FilePath $installerPath -ArgumentList `
                "--wait", "--passive", "--add", "Microsoft.VisualStudio.Workload.VCTools", "--includeRecommended" `
                -Wait -NoNewWindow
            Remove-Item $installerPath -Force -ErrorAction SilentlyContinue
            Write-Info "Visual Studio Build Tools installed"
        }
    }
}

function Install-CMake {
    Write-Host ""
    Write-Host "-- CMake --"
    if (Test-Command "cmake") {
        Write-Info "CMake already installed: $(cmake --version | Select-Object -First 1)"
        return
    }
    Write-Warn "Installing CMake..."
    winget install --id Kitware.CMake --accept-source-agreements --accept-package-agreements
    Refresh-Path
    Write-Info "CMake installed"
}

function Install-Go {
    Write-Host ""
    Write-Host "-- Go --"
    if (Test-Command "go") {
        Write-Info "Go already installed: $(go version)"
    } else {
        Write-Warn "Installing Go..."
        winget install --id GoLang.Go --accept-source-agreements --accept-package-agreements
        Refresh-Path
        Write-Info "Go installed: $(go version)"
    }

    # Persist GOPATH/bin to User PATH
    $gobin = "$(go env GOPATH)\bin"
    $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($currentUserPath -notlike "*$gobin*") {
        [Environment]::SetEnvironmentVariable("Path", "$gobin;$currentUserPath", "User")
        Write-Info "Added $gobin to permanent User PATH"
    }
    if ($env:Path -notlike "*$gobin*") {
        $env:Path = "$gobin;$env:Path"
    }

    # mage -- taos-insight build tool
    if (-not (Test-Command "mage")) {
        Write-Warn "Installing mage..."
        go install github.com/magefile/mage@latest
    }
    Write-Info "mage installed"
}

function Install-Rust {
    Write-Host ""
    Write-Host "-- Rust / Cargo --"
    if (Test-Command "cargo") {
        Write-Info "Rust/Cargo already installed: $(cargo --version)"
    } else {
        Write-Warn "Installing Rust (rustup)..."
        # Download and run rustup-init
        $rustupUrl = "https://win.rustup.rs/x86_64"
        $rustupPath = "$env:TEMP\rustup-init.exe"
        Invoke-WebRequest -Uri $rustupUrl -OutFile $rustupPath -UseBasicParsing
        Start-Process -FilePath $rustupPath -ArgumentList "-y" -Wait -NoNewWindow
        Remove-Item $rustupPath -Force -ErrorAction SilentlyContinue
        # Persist cargo/bin to User PATH
        $cargoPath = "$env:USERPROFILE\.cargo\bin"
        $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
        if ($currentUserPath -notlike "*$cargoPath*") {
            [Environment]::SetEnvironmentVariable("Path", "$cargoPath;$currentUserPath", "User")
            Write-Info "Added $cargoPath to permanent User PATH"
        }
        if ($env:Path -notlike "*$cargoPath*") {
            $env:Path = "$cargoPath;$env:Path"
        }
        Write-Info "Rust installed: $(rustc --version)"
    }

    # edition2024 requires Rust >= 1.85
    $rustVer = (rustc --version) -replace 'rustc\s+', '' -replace '\s.*', ''
    $minor = [int]($rustVer.Split('.')[1])
    if ($minor -lt 85) {
        Write-Warn "Rust $rustVer is too old (>= 1.85 required for edition2024), upgrading..."
        rustup update stable
        Write-Info "Rust upgraded: $(rustc --version)"
    }
}

function Install-JavaMaven {
    Write-Host ""
    Write-Host "-- Java 17 + Maven --"

    # Java 17
    $javaMajor = Get-JavaMajorVersion
    if ((Test-Command "java") -and ($javaMajor -ge 17)) {
        Write-Info "Java already installed: $(java --version 2>&1 | Select-Object -First 1)"
    } else {
        Write-Warn "Installing OpenJDK 17..."
        winget install --id Microsoft.OpenJDK.17 --accept-source-agreements --accept-package-agreements
        Refresh-Path
        # Set JAVA_HOME
        $javaHome = "${env:ProgramFiles}\Microsoft\jdk-17*"
        $javaHomePath = (Get-Item $javaHome -ErrorAction SilentlyContinue | Select-Object -First 1).FullName
        if ($javaHomePath) {
            $env:JAVA_HOME = $javaHomePath
            [Environment]::SetEnvironmentVariable("JAVA_HOME", $javaHomePath, "User")
            Write-Info "JAVA_HOME set to $javaHomePath"
        }
        Write-Info "Java installed: $(java --version 2>&1 | Select-Object -First 1)"
    }

    # Maven
    if (Test-Command "mvn") {
        Write-Info "Maven already installed: $(mvn --version 2>&1 | Select-Object -First 1)"
    } else {
        Write-Warn "Installing Maven..."
        # winget does not always have Maven, use direct download
        $mavenVersion = "3.9.9"
        $mavenUrl = "https://archive.apache.org/dist/maven/maven-3/$mavenVersion/binaries/apache-maven-$mavenVersion-bin.zip"
        $mavenZip = "$env:TEMP\apache-maven-$mavenVersion-bin.zip"
        $mavenInstallDir = "$env:LOCALAPPDATA\Programs\Maven"

        Invoke-WebRequest -Uri $mavenUrl -OutFile $mavenZip -UseBasicParsing
        if (-not (Test-Path $mavenInstallDir)) {
            New-Item -ItemType Directory -Path $mavenInstallDir -Force | Out-Null
        }
        Expand-Archive -Path $mavenZip -DestinationPath $mavenInstallDir -Force
        Remove-Item $mavenZip -Force -ErrorAction SilentlyContinue

        $mavenBin = "$mavenInstallDir\apache-maven-$mavenVersion\bin"
        $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
        if ($currentUserPath -notlike "*$mavenBin*") {
            [Environment]::SetEnvironmentVariable("Path", "$mavenBin;$currentUserPath", "User")
        }
        $env:Path = "$mavenBin;$env:Path"
        Write-Info "Maven installed: $(mvn --version 2>&1 | Select-Object -First 1)"
    }
}

function Install-Node {
    Write-Host ""
    Write-Host "-- Node.js 22 + yarn + pnpm --"

    if (Test-Command "node") {
        $nodeVer = (node --version) -replace '^v', ''
        $nodeMajor = [int]($nodeVer.Split('.')[0])
        if ($nodeMajor -lt 18) {
            Write-Warn "Node.js v$nodeVer < v18, upgrading..."
            winget install --id OpenJS.NodeJS.LTS --accept-source-agreements --accept-package-agreements --force
            Refresh-Path
        } elseif ($nodeMajor -gt 24) {
            Write-Warn "Node.js v$nodeVer > v24. Installing LTS version..."
            winget install --id OpenJS.NodeJS.LTS --accept-source-agreements --accept-package-agreements --force
            Refresh-Path
        } else {
            Write-Info "Node.js already installed: v$nodeVer"
        }
    } else {
        Write-Warn "Installing Node.js LTS..."
        winget install --id OpenJS.NodeJS.LTS --accept-source-agreements --accept-package-agreements
        Refresh-Path
        Write-Info "Node.js installed: $(node --version)"
    }

    # Persist npm global bin dir to User PATH
    $npmGlobal = $null
    try { $npmGlobal = (npm config get prefix 2>&1 | Out-String).Trim() } catch {}
    if ($npmGlobal) {
        $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
        if ($currentUserPath -notlike "*$npmGlobal*") {
            [Environment]::SetEnvironmentVariable("Path", "$npmGlobal;$currentUserPath", "User")
            Write-Info "Added $npmGlobal to permanent User PATH"
        }
        if ($env:Path -notlike "*$npmGlobal*") {
            $env:Path = "$npmGlobal;$env:Path"
        }
    }

    # yarn
    if (-not (Test-Command "yarn")) {
        Write-Warn "Installing yarn..."
        npm install -g yarn
    }
    Refresh-Path
    Write-Info "yarn: $(yarn --version)"

    # pnpm
    if (-not (Test-Command "pnpm")) {
        Write-Warn "Installing pnpm..."
        npm install -g pnpm
    }
    Refresh-Path
    Write-Info "pnpm: $(pnpm --version)"
}

function Install-DotNet {
    Write-Host ""
    Write-Host "-- .NET SDK --"
    # Check for SDK specifically (not just runtime)
    $hasSdk = $false
    if (Test-Command "dotnet") {
        try {
            $sdks = dotnet --list-sdks 2>&1 | Out-String
            if ($sdks -and $sdks.Trim()) {
                $hasSdk = $true
                $sdkVer = ($sdks.Trim().Split("`n") | Select-Object -Last 1) -replace '\s.*', ''
                Write-Info ".NET SDK already installed: $sdkVer"
            }
        } catch {}
    }
    if (-not $hasSdk) {
        Write-Warn "Installing .NET SDK 8..."
        winget install --id Microsoft.DotNet.SDK.8 --accept-source-agreements --accept-package-agreements
        Refresh-Path
        Write-Info ".NET SDK installed: $((dotnet --version 2>&1 | Out-String).Trim())"
    }
}

function Install-Python {
    Write-Host ""
    Write-Host "-- Python 3 + maturin --"

    # python on Windows
    $pythonCmd = $null
    if (Test-Command "python") {
        $pythonCmd = "python"
    } elseif (Test-Command "python3") {
        $pythonCmd = "python3"
    }

    if ($pythonCmd) {
        $pyVer = & $pythonCmd -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>&1 | Out-String
        $pyVer = $pyVer.Trim()
        $pyParts = $pyVer.Split('.')
        $pyMajor = [int]$pyParts[0]
        $pyMinor = [int]$pyParts[1]
        if ($pyMajor -lt 3 -or ($pyMajor -eq 3 -and $pyMinor -lt 9)) {
            Write-Warn "Python $pyVer is too old (>= 3.9 required), installing newer version..."
            winget install --id Python.Python.3.12 --accept-source-agreements --accept-package-agreements
            Refresh-Path
        } else {
            Write-Info "Python already installed: $($pythonCmd) $pyVer"
        }
    } else {
        Write-Warn "Installing Python 3.12..."
        winget install --id Python.Python.3.12 --accept-source-agreements --accept-package-agreements
        Refresh-Path
    }

    # Upgrade pip
    try { python -m pip install --upgrade pip 2>&1 | Out-Null } catch {}

    # Persist Python Scripts dir to User PATH
    try {
        $pyScripts = (python -c "import sysconfig; print(sysconfig.get_path('scripts'))" 2>&1 | Out-String).Trim()
        if ($pyScripts -and (Test-Path $pyScripts)) {
            $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
            if ($currentUserPath -notlike "*$pyScripts*") {
                [Environment]::SetEnvironmentVariable("Path", "$pyScripts;$currentUserPath", "User")
                Write-Info "Added $pyScripts to permanent User PATH"
            }
            if ($env:Path -notlike "*$pyScripts*") {
                $env:Path = "$pyScripts;$env:Path"
            }
        }
    } catch {}

    # maturin -- Rust-Python bridge build tool
    if (Test-Command "maturin") {
        Write-Info "maturin already installed: $(maturin --version)"
    } else {
        Write-Warn "Installing maturin..."
        python -m pip install maturin
        Refresh-Path
        Write-Info "maturin installed"
    }
}

function Install-Conan {
    Write-Host ""
    Write-Host "-- Conan (C++ package manager) --"
    if (Test-Command "conan") {
        Write-Info "Conan already installed: $(conan --version)"
    } else {
        Write-Warn "Installing Conan..."
        python -m pip install conan
        Refresh-Path
        Write-Info "Conan installed"
    }

    # Initialize conan default profile (required by taos-gen build)
    $hasDefault = $false
    try {
        $profiles = conan profile list 2>&1 | Out-String
        if ($profiles -match 'default') { $hasDefault = $true }
    } catch {}
    if (-not $hasDefault) {
        Write-Warn "Creating conan default profile..."
        try { conan profile detect 2>&1 | Out-Null } catch {}
    }
    Write-Info "Conan profile ready"
}

function Install-Protoc {
    Write-Host ""
    Write-Host "-- protoc (Protocol Buffers compiler) --"
    if (Test-Command "protoc") {
        Write-Info "protoc already installed: $(protoc --version)"
        return
    }
    Write-Warn "Installing protoc..."
    # Download from GitHub releases
    $protocVersion = "28.3"
    $protocUrl = "https://github.com/protocolbuffers/protobuf/releases/download/v$protocVersion/protoc-$protocVersion-win64.zip"
    $protocZip = "$env:TEMP\protoc-$protocVersion-win64.zip"
    $protocDir = "$env:LOCALAPPDATA\Programs\protoc"

    Invoke-WebRequest -Uri $protocUrl -OutFile $protocZip -UseBasicParsing
    if (-not (Test-Path $protocDir)) {
        New-Item -ItemType Directory -Path $protocDir -Force | Out-Null
    }
    Expand-Archive -Path $protocZip -DestinationPath $protocDir -Force
    Remove-Item $protocZip -Force -ErrorAction SilentlyContinue

    $protocBin = "$protocDir\bin"
    $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($currentUserPath -notlike "*$protocBin*") {
        [Environment]::SetEnvironmentVariable("Path", "$protocBin;$currentUserPath", "User")
    }
    $env:Path = "$protocBin;$env:Path"
    Write-Info "protoc installed: $(protoc --version)"
}

function Install-WinFlexBison {
    Write-Host ""
    Write-Host "-- win_flex / win_bison (flex & bison for Windows) --"
    if ((Test-Command "win_bison") -and (Test-Command "win_flex")) {
        Write-Info "win_flex_bison already installed"
        return
    }
    Write-Warn "Installing win_flex_bison..."
    winget install --id lexxmark.winflexbison3 --accept-source-agreements --accept-package-agreements
    Refresh-Path
    if (-not (Test-Command "win_bison")) {
        # Fallback: download directly
        $wfbVersion = "2.5.25"
        $wfbUrl = "https://github.com/lexxmark/winflexbison/releases/download/v$wfbVersion/win_flex_bison-$wfbVersion.zip"
        $wfbZip = "$env:TEMP\win_flex_bison.zip"
        $wfbDir = "$env:LOCALAPPDATA\Programs\WinFlexBison"

        Invoke-WebRequest -Uri $wfbUrl -OutFile $wfbZip -UseBasicParsing
        if (-not (Test-Path $wfbDir)) {
            New-Item -ItemType Directory -Path $wfbDir -Force | Out-Null
        }
        Expand-Archive -Path $wfbZip -DestinationPath $wfbDir -Force
        Remove-Item $wfbZip -Force -ErrorAction SilentlyContinue

        $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")
        if ($currentUserPath -notlike "*$wfbDir*") {
            [Environment]::SetEnvironmentVariable("Path", "$wfbDir;$currentUserPath", "User")
        }
        $env:Path = "$wfbDir;$env:Path"
    }
    Write-Info "win_flex_bison installed"
}

function Install-Git {
    Write-Host ""
    Write-Host "-- Git --"
    if (Test-Command "git") {
        Write-Info "Git already installed: $(git --version)"
        return
    }
    Write-Warn "Installing Git..."
    winget install --id Git.Git --accept-source-agreements --accept-package-agreements
    Refresh-Path
    Write-Info "Git installed"
}

# ══════════════════════════════════════════════════════════════════════════
#  Verify installation
# ══════════════════════════════════════════════════════════════════════════
function Verify-All {
    Refresh-Path
    Write-Host ""
    Write-Host "=============================="
    Write-Host " Installation verification"
    Write-Host "=============================="

    $toolsSpec = @(
        @{ Name = "cmake";      Check = "cmake";      Cmd = "cmake --version | Select-Object -First 1" },
        @{ Name = "go";         Check = "go";          Cmd = "go version" },
        @{ Name = "rustc";      Check = "rustc";       Cmd = "rustc --version" },
        @{ Name = "cargo";      Check = "cargo";       Cmd = "cargo --version" },
        @{ Name = "java";       Check = "java";        Cmd = "java --version 2>&1 | Select-Object -First 1" },
        @{ Name = "mvn";        Check = "mvn";         Cmd = "mvn --version 2>&1 | Select-Object -First 1" },
        @{ Name = "node";       Check = "node";        Cmd = "node --version" },
        @{ Name = "yarn";       Check = "yarn";        Cmd = "yarn --version" },
        @{ Name = "pnpm";       Check = "pnpm";        Cmd = "pnpm --version" },
        @{ Name = "python";     Check = "python";      Cmd = "python --version" },
        @{ Name = "maturin";    Check = "maturin";     Cmd = "maturin --version" },
        @{ Name = "dotnet";     Check = "dotnet";      Cmd = "dotnet --version" },
        @{ Name = "protoc";     Check = "protoc";      Cmd = "protoc --version" },
        @{ Name = "win_bison";  Check = "win_bison";   Cmd = "win_bison --version 2>&1 | Select-Object -First 1" },
        @{ Name = "win_flex";   Check = "win_flex";    Cmd = "win_flex --version 2>&1 | Select-Object -First 1" },
        @{ Name = "conan";      Check = "conan";       Cmd = "conan --version" },
        @{ Name = "mage";       Check = "mage";        Cmd = "mage --version 2>&1 | Select-Object -First 1" },
        @{ Name = "git";        Check = "git";         Cmd = "git --version" }
    )

    $failed = 0
    foreach ($spec in $toolsSpec) {
        if (Test-Command $spec.Check) {
            try {
                $ver = (Invoke-Expression $spec.Cmd 2>$null | Out-String).Trim()
                if ($spec.Name -eq "java") {
                    $jMajor = Get-JavaMajorVersion
                    if ($jMajor -lt 17) {
                        Write-Fail "  $($spec.Name) -- $ver (requires 17+)"
                        $failed++
                        continue
                    }
                }
                Write-Info "  $($spec.Name) -- $ver"
            } catch {
                Write-Info "  $($spec.Name) -- found"
            }
        } else {
            Write-Fail "  $($spec.Name) -- not found!"
            $failed++
        }
    }

    Write-Host ""
    if ($failed -eq 0) {
        Write-Info "All dependencies installed successfully!"
    } else {
        Write-Fail "$failed tool(s) failed to install, please check the logs above."
    }

    Write-Host ""
    Write-Host "=============================="
    Write-Host " Build command (Developer PowerShell for VS):"
    Write-Host '   cd debug; cmake .. -G "Visual Studio 17 2022" -A x64; cmake --build . --config Release'
    Write-Host "=============================="
}

# ══════════════════════════════════════════════════════════════════════════
#  PATH environment variable configuration
# ══════════════════════════════════════════════════════════════════════════
function Install-PathConfig {
    Write-Host ""
    Write-Host "-- Configure persistent User environment variables --"

    $currentUserPath = [Environment]::GetEnvironmentVariable("Path", "User")

    # Collect all paths that should be persistent
    $pathsToAdd = @(
        "$env:USERPROFILE\go\bin",
        "$env:USERPROFILE\.cargo\bin"
    )

    # npm global dir
    try {
        $npmGlobal = (npm config get prefix 2>&1 | Out-String).Trim()
        if ($npmGlobal) { $pathsToAdd += $npmGlobal }
    } catch {}

    # Python Scripts dir
    try {
        $pyScripts = (python -c "import sysconfig; print(sysconfig.get_path('scripts'))" 2>&1 | Out-String).Trim()
        if ($pyScripts) { $pathsToAdd += $pyScripts }
    } catch {}

    $changed = $false
    foreach ($p in $pathsToAdd) {
        if ($p -and $currentUserPath -notlike "*$p*") {
            $currentUserPath = "$p;$currentUserPath"
            $changed = $true
            Write-Info "  + $p"
        }
    }

    if ($changed) {
        [Environment]::SetEnvironmentVariable("Path", $currentUserPath, "User")
        Write-Info "User PATH updated (persistent, survives reboot)"
    } else {
        Write-Info "User PATH already configured, skipping"
    }

    # Set JAVA_HOME permanently if java is available
    if ((Test-Command "java") -and -not [Environment]::GetEnvironmentVariable("JAVA_HOME", "User")) {
        $javaHome = "${env:ProgramFiles}\Microsoft\jdk-17*"
        $javaHomePath = (Get-Item $javaHome -ErrorAction SilentlyContinue | Select-Object -First 1).FullName
        if ($javaHomePath) {
            [Environment]::SetEnvironmentVariable("JAVA_HOME", $javaHomePath, "User")
            $env:JAVA_HOME = $javaHomePath
            Write-Info "JAVA_HOME=$javaHomePath (persistent)"
        }
    }

    # Set GO111MODULE permanently
    $goModule = [Environment]::GetEnvironmentVariable("GO111MODULE", "User")
    if ($goModule -ne "on") {
        [Environment]::SetEnvironmentVariable("GO111MODULE", "on", "User")
        $env:GO111MODULE = "on"
        Write-Info "GO111MODULE=on (persistent)"
    }

    # Reload PATH into current session
    Refresh-Path
    Write-Info "All environment variables are set persistently (User scope, survives reboot)"
}

# ══════════════════════════════════════════════════════════════════════════
#  Main entry point
# ══════════════════════════════════════════════════════════════════════════
function Main {
    Write-Host ""
    Write-Host "OS: Windows $([System.Environment]::OSVersion.Version) ($env:PROCESSOR_ARCHITECTURE)"

    if ($Check) {
        $result = Check-All
        exit $result
    }

    # Check admin privileges for certain installs
    $isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
        [Security.Principal.WindowsBuiltInRole]::Administrator
    )
    if (-not $isAdmin) {
        Write-Warn "Not running as Administrator. Some installations (VS Build Tools, protoc, Maven) may need admin rights."
        Write-Warn "Consider re-running as Administrator if installation fails."
    }

    Write-Host ""
    Write-Host "=============================="
    Write-Host " Installing TSDB full-component build dependencies (Windows)"
    Write-Host "=============================="
    Write-Host ""

    Ensure-WinGet

    Install-Git
    Install-VisualStudioBuildTools
    Install-CMake
    Install-Go
    Install-Rust
    Install-JavaMaven
    Install-Node
    Install-DotNet
    Install-Python
    Install-Conan
    Install-Protoc
    Install-WinFlexBison
    Install-PathConfig

    Verify-All
}

Main
