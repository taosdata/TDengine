# modules-windows/cpp.ps1 — C/C++ toolchain (cmake, MSVC/clang, ccache, conan)

function Mod-cpp-Check {
    Write-Header 'C/C++ Toolchain'

    # cmake
    if (Test-CommandExists 'cmake') {
        $ver = (cmake --version | Select-Object -First 1) -replace '.*?(\d+\.\d+\.\d+).*', '$1'
        if (Test-VersionGte $ver $script:RequiredCMakeVersion) {
            Write-Ok "cmake $ver (>= $($script:RequiredCMakeVersion))"
        } else {
            Write-Warn "cmake $ver (need >= $($script:RequiredCMakeVersion))"
            $script:IssuesFound++
        }
    } else {
        Write-Fail 'cmake not found'
        $script:IssuesFound++
    }

    # MSVC (cl.exe)
    if (Test-CommandExists 'cl') {
        Write-Ok "MSVC cl.exe found"
    } else {
        Write-Info "MSVC cl.exe not in PATH (use Developer Command Prompt or VS Build Tools)"
    }

    # conan
    if (Test-CommandExists 'conan') {
        $ver = (conan --version 2>$null) -replace '.*?(\d+\.\d+\.\d+).*', '$1'
        Write-Ok "conan $ver"
    } else {
        Write-Warn 'conan not found'
        $script:IssuesFound++
    }
}

function Mod-cpp-Install {
    if (-not (Test-CommandExists 'cmake')) {
        if (Confirm-Action 'Install CMake?') {
            if (Test-CommandExists 'winget') {
                Install-WithWinget 'Kitware.CMake'
            } else {
                Install-WithChoco @('cmake', '--installargs="ADD_CMAKE_TO_PATH=System"')
            }
            $script:ChangesMade++
        }
    }

    if (-not (Test-CommandExists 'conan')) {
        if (Confirm-Action 'Install Conan?') {
            pip install conan 2>$null
            $script:ChangesMade++
        }
    }
}

function Mod-cpp-Config {
    if (-not (Test-CommandExists 'conan')) { return }

    $remotes = conan remote list 2>$null
    if ($remotes -match 'nexus') { return }

    if (Confirm-Action 'Add Conan Nexus remote?') {
        conan remote add nexus $script:ConanRemoteUrl --index 0 2>$null
        Write-Ok "Conan remote 'nexus' added"
        $script:ChangesMade++
    }
}
