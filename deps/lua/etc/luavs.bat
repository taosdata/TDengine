cd src
cl /O2 /W3 /c /DLUA_BUILD_AS_DLL l*.c
del lua.obj luac.obj
link /DLL /out:lua51.dll l*.obj
cl /O2 /W3 /c /DLUA_BUILD_AS_DLL lua.c
link /out:lua.exe lua.obj lua51.lib
cd ..
