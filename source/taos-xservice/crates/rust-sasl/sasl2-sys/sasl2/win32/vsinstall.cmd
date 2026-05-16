@echo off
setlocal EnableDelayedExpansion

set SaslIntermediateLibDir=%1
set InstallRoot=%2
set InstallLibDir=%3
set InstallBinDir=%4

echo Installing Cyrus SASL to %InstallRoot%
echo   BinDir:     %InstallBinDir%
echo   LibDir:     %InstallLibDir%
echo   IncludeDir: %InstallRoot%\include\sasl
echo   PluginsDir: %InstallRoot%\plugins\sasl
echo ---

if not exist %InstallLibDir% mkdir %InstallLibDir%
if not exist %InstallBinDir% mkdir %InstallBinDir%
if not exist %InstallRoot%\plugins\sasl mkdir %InstallRoot%\plugins\sasl
if not exist %InstallRoot%\include\sasl mkdir %InstallRoot%\include\sasl

for /f "usebackq delims=|" %%f in (`dir /b "%SaslIntermediateLibDir%\*.dll"`) do (
  set libname=%%~nf
  set prefix=!libname:~0,6!
  if !prefix!==plugin set outdir=%InstallRoot%\plugins\sasl
  if not !prefix!==plugin set outdir=%InstallBinDir%
  
  xcopy /d /y %SaslIntermediateLibDir%\%%~nf.dll  !outdir!
  xcopy /d /y /c %SaslIntermediateLibDir%\%%~nf.pdb  !outdir!
  if not !prefix!==plugin xcopy /d /y %SaslIntermediateLibDir%\%%~nf.lib %InstallLibDir%
)

xcopy /d /y ..\include\*.h %InstallRoot%\include\sasl\
