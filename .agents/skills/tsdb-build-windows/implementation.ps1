# TDinternal Windows Build Skill Implementation

$script:BuildDir = "D:\TDinternal\debug"
$script:VsVarsPath = "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"

function Invoke-InVsCmd {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Command
    )

    if (-not (Test-Path $script:VsVarsPath)) {
        throw "vcvars64.bat not found: $script:VsVarsPath"
    }

    $full = "`"$script:VsVarsPath`" && $Command"
    cmd /c $full
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE: $Command"
    }
}

function Ensure-BuildDir {
    if (-not (Test-Path $script:BuildDir)) {
        New-Item -ItemType Directory -Path $script:BuildDir -Force | Out-Null
    }
}

function Invoke-TDinternalBuild {
    Ensure-BuildDir
    Push-Location $script:BuildDir
    try {
        Invoke-InVsCmd 'cmake .. -G "NMake Makefiles JOM"'
        Invoke-InVsCmd 'jom'
    }
    finally {
        Pop-Location
    }
}

Export-ModuleMember -Function Invoke-TDinternalBuild
