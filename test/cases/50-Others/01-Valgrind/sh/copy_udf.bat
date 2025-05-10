@echo off

echo Executing copy_udf.bat
set SCRIPT_DIR=%cd%
echo SCRIPT_DIR: %SCRIPT_DIR%

echo %cd% | grep community > nul && cd ..\..\.. || cd ..\..
set TAOS_DIR=%cd%
echo find udf library in %TAOS_DIR%
set UDF1_DIR=%TAOS_DIR%\debug\build\lib\udf1.dll
set UDF2_DIR=%TAOS_DIR%\debug\build\lib\udf2.dll

echo %UDF1_DIR%
echo %UDF2_DIR%

set UDF_TMP=C:\Windows\Temp\udf
rm -rf %UDF_TMP%
mkdir %UDF_TMP%

echo Copy udf shared library files to %UDF_TMP%

cp %UDF1_DIR% %UDF_TMP%
cp %UDF2_DIR% %UDF_TMP%
