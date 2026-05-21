# modules-windows/dotnet.ps1 — .NET SDK toolchain

function Mod-dotnet-Check {
    Write-Header '.NET Toolchain'

    if (Test-CommandExists 'dotnet') {
        $ver = dotnet --version 2>$null
        Write-Ok "dotnet SDK $ver"

        $sdkCount = (dotnet --list-sdks 2>$null | Measure-Object).Count
        Write-Info "$sdkCount SDK(s) installed"

        # NuGet source
        $sources = dotnet nuget list source 2>$null
        if ($sources -match 'nora\.tdengine\.net') {
            Write-Ok 'NuGet source → internal mirror'
        } else {
            Write-Info 'NuGet using default sources'
        }
    } else {
        Write-Fail 'dotnet not found'
        $script:IssuesFound++
    }
}

function Mod-dotnet-Install {
    if (Test-CommandExists 'dotnet') { return }

    if (Confirm-Action 'Install .NET SDK?') {
        if (Test-CommandExists 'winget') {
            Install-WithWinget 'Microsoft.DotNet.SDK.8'
        } else {
            Install-WithChoco @('dotnet-sdk')
        }
        $script:ChangesMade++
    }
}

function Mod-dotnet-Config {
    if (-not (Test-CommandExists 'dotnet')) { return }

    $noraNugetUrl = 'https://nora.tdengine.net/nuget/v3/index.json'
    $sources = dotnet nuget list source 2>$null

    if ($sources -match 'nora\.tdengine\.net') { return }

    if (Confirm-Action 'Add internal NuGet source (Nora)?') {
        dotnet nuget add source $noraNugetUrl --name 'tdengine-internal' 2>$null
        Write-Ok "NuGet source added: $noraNugetUrl"
        $script:ChangesMade++
    }
}
