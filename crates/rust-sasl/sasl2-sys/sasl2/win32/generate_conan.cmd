set vsversion=%1
set arch=%2
set usedebug=%3
set platform=%4
set configuration=%5

set build_type=Release
if %usedebug%==true (
  set build_type=Debug
)

set conan_arch=%arch%
if not %arch%==x86 (
  set conan_arch=x86_64
)

set vsversion=%vsversion:~0,2%

mkdir conan\%platform%\%configuration%
cd conan\%platform%\%configuration%
conan install ..\..\.. -s compiler="Visual Studio" -s compiler.version=%vsversion% -s arch=%conan_arch% -s build_type=%build_type%