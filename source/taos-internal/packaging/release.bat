cd D:\0-ver_release_room\TDinternal-2.0
mkdir debug
cd debug
mkdir release"%1"-64bit
cd  release"%1"-64bit
call "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" amd64
cmake ../../ -G "NMake Makefiles"  -DVERNUMBER="%1"
nmake install
c:
cd C:\Program Files (x86)\Inno Setup 6
ISCC.exe
iscc /DMyAppInstallName="TDengine-client-%1-Windows-x64" D:\0-ver_release_room\TDengine-winodes-client-release\windows-client-64bit\tdengine_j.iss

d:
cd D:\0-ver_release_room\TDinternal-2.0
cd debug
mkdir release"%1"-32bit
cd  release"%1"-32bit
call "C:\Program Files (x86)\Microsoft Visual Studio 12.0\VC\vcvarsall.bat" x86
cmake ../../ -G "NMake Makefiles"  -DVERNUMBER="%1"
nmake install
c:
cd C:\Program Files (x86)\Inno Setup 6
ISCC.exe
iscc /DMyAppInstallName="TDengine-client-%1-Windows-x86" D:\0-ver_release_room\TDengine-winodes-client-release\windows-client-32bit\tdengine_j.iss