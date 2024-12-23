# Microsoft Developer Studio Project File - Name="CUnit" - Package Owner=<4>
# Microsoft Developer Studio Generated Build File, Format Version 6.00
# ** DO NOT EDIT **

# TARGTYPE "Win32 (x86) Static Library" 0x0104

CFG=CUnit - Win32 Debug
!MESSAGE This is not a valid makefile. To build this project using NMAKE,
!MESSAGE use the Export Makefile command and run
!MESSAGE 
!MESSAGE NMAKE /f "CUnit.mak".
!MESSAGE 
!MESSAGE You can specify a configuration when running NMAKE
!MESSAGE by defining the macro CFG on the command line. For example:
!MESSAGE 
!MESSAGE NMAKE /f "CUnit.mak" CFG="CUnit - Win32 Debug"
!MESSAGE 
!MESSAGE Possible choices for configuration are:
!MESSAGE 
!MESSAGE "CUnit - Win32 Release" (based on "Win32 (x86) Static Library")
!MESSAGE "CUnit - Win32 Debug" (based on "Win32 (x86) Static Library")
!MESSAGE 

# Begin Project
# PROP AllowPerConfigDependencies 0
# PROP Scc_ProjName ""
# PROP Scc_LocalPath ""
CPP=cl.exe
RSC=rc.exe

!IF  "$(CFG)" == "CUnit - Win32 Release"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 0
# PROP BASE Output_Dir "Release"
# PROP BASE Intermediate_Dir "Release"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 0
# PROP Output_Dir ""
# PROP Intermediate_Dir "Temp"
# PROP Target_Dir ""
# ADD BASE CPP /nologo /W3 /GX /O2 /D "WIN32" /D "NDEBUG" /D "_MBCS" /D "_LIB" /YX /FD /c
# ADD CPP /nologo /W3 /GX /O2 /I "./Headers" /D "NDEBUG" /D "WIN32" /D "_MBCS" /D "_LIB" /D "_DELAYTEST" /YX /FD /c
# ADD BASE RSC /l 0x409 /d "NDEBUG"
# ADD RSC /l 0x409 /d "NDEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LIB32=link.exe -lib
# ADD BASE LIB32 /nologo
# ADD LIB32 /nologo

!ELSEIF  "$(CFG)" == "CUnit - Win32 Debug"

# PROP BASE Use_MFC 0
# PROP BASE Use_Debug_Libraries 1
# PROP BASE Output_Dir "Debug"
# PROP BASE Intermediate_Dir "Debug"
# PROP BASE Target_Dir ""
# PROP Use_MFC 0
# PROP Use_Debug_Libraries 1
# PROP Output_Dir ""
# PROP Intermediate_Dir "Temp"
# PROP Target_Dir ""
# ADD BASE CPP /nologo /W3 /Gm /GX /ZI /Od /D "WIN32" /D "_DEBUG" /D "_MBCS" /D "_LIB" /YX /FD /GZ /c
# ADD CPP /nologo /W3 /Gm /GX /ZI /Od /I "./Headers" /D "_LIB" /D VERSION=1.0.6B /D "_DEBUG" /D "WIN32" /D "_MBCS" /D "_DELAYTEST" /D "MEMTRACE" /YX /FD /GZ /c
# ADD BASE RSC /l 0x409 /d "_DEBUG"
# ADD RSC /l 0x409 /d "_DEBUG"
BSC32=bscmake.exe
# ADD BASE BSC32 /nologo
# ADD BSC32 /nologo
LIB32=link.exe -lib
# ADD BASE LIB32 /nologo
# ADD LIB32 /nologo

!ENDIF

# Begin Target

# Name "CUnit - Win32 Release"
# Name "CUnit - Win32 Debug"
# Begin Group "Source Files"

# PROP Default_Filter "cpp;c;cxx;rc;def;r;odl;idl;hpj;bat"
# Begin Source File

SOURCE=.\Sources\Automated\Automated.c
# End Source File
# Begin Source File

SOURCE=.\Sources\Basic\Basic.c
# End Source File
# Begin Source File

SOURCE=.\Sources\Console\Console.c
# End Source File
# Begin Source File

SOURCE=.\Sources\Framework\CUError.c
# End Source File
# Begin Source File

SOURCE=.\Sources\Framework\MyMem.c
# End Source File
# Begin Source File

SOURCE=.\Sources\Framework\TestDB.c
# End Source File
# Begin Source File

SOURCE=.\Sources\Framework\TestRun.c
# End Source File
# Begin Source File

SOURCE=.\Sources\Framework\Util.c
# End Source File
# End Group
# Begin Group "Header Files"

# PROP Default_Filter "h;hpp;hxx;hm;inl"
# Begin Source File

SOURCE=.\Headers\Automated.h
# End Source File
# Begin Source File

SOURCE=.\Headers\Basic.h
# End Source File
# Begin Source File

SOURCE=.\Headers\Console.h
# End Source File
# Begin Source File

SOURCE=.\Headers\CUError.h
# End Source File
# Begin Source File

SOURCE=.\Headers\CUnit.h
# End Source File
# Begin Source File

SOURCE=.\Headers\MyMem.h
# End Source File
# Begin Source File

SOURCE=.\Headers\TestDB.h
# End Source File
# Begin Source File

SOURCE=.\Headers\TestRun.h
# End Source File
# Begin Source File

SOURCE=.\Headers\Util.h
# End Source File
# End Group
# End Target
# End Project
