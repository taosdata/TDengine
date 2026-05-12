# Redirect to the standard windows makefile if GnuWin32's uname.exe is
# installed.

# Setup the PATH 

export SHELL := C:/msys/1.0/bin/sh.exe
export PATH := C:/msys/1.0/bin:$(PATH)

include Windows.mak
