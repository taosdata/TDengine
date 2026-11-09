@echo off
setlocal
rem Optional allocator files; staging never modifies taosd.exe.
set "source=%~1"
set "dest=%~2"
set "allocator=%~3"
if not "%allocator%"=="" (
    for %%f in ("%allocator%.dll" "mimalloc-redirect.dll" "minject.exe" "mimalloc-LICENSE.txt") do (
        if not exist "%source%\%%~f" (
            echo Missing mimalloc package file: %source%\%%~f
            exit /b 1
        )
    )
)
if not exist "%dest%" mkdir "%dest%"
rem A reused installation must not retain an allocator from an OFF/other-config build.
for %%f in (mimalloc.dll mimalloc-debug.dll mimalloc-redirect.dll minject.exe mimalloc-LICENSE.txt) do (
    if exist "%dest%\%%f" (
        del /q "%dest%\%%f"
        if exist "%dest%\%%f" exit /b 1
    )
)
if "%allocator%"=="" exit /b 0
for %%f in ("%allocator%.dll" "mimalloc-redirect.dll" "minject.exe" "mimalloc-LICENSE.txt") do (
    copy /y "%source%\%%~f" "%dest%\%%~f" > nul
    if errorlevel 1 exit /b 1
)
exit /b 0
