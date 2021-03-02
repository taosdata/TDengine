CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

#
# If it is a Windows operating system
#   1.Use command line tool of VS2013 or higher version
#     mkdir build; cd build;
#     cmake -G "NMake Makefiles" ..
#     nmake install
#   2.Use the VS development interface tool
#     mkdir build; cd build;
#     cmake -A x64 ..
#     open the file named TDengine.sln
#

# Set macro definitions according to os platform
SET(TD_LINUX FALSE)
  SET(TD_LINUX_64 FALSE)
  SET(TD_LINUX_32 FALSE)
  SET(TD_ARM_64 FALSE)
  SET(TD_ARM_32 FALSE)
  SET(TD_MIPS_64 FALSE)
  SET(TD_MIPS_32 FALSE)
  SET(TD_APLHINE FALSE)
  SET(TD_NINGSI FALSE)
  SET(TD_NINGSI_60 FALSE)
  SET(TD_NINGSI_80 FALSE)
SET(TD_WINDOWS FALSE)
  SET(TD_WINDOWS_64 FALSE)
  SET(TD_WINDOWS_32 FALSE)
SET(TD_DARWIN FALSE)
  SET(TD_DARWIN_64 FALSE)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  #
  # Get OS information and store in variable TD_OS_INFO.
  #
  execute_process(COMMAND chmod 777 ${TD_COMMUNITY_DIR}/packaging/tools/get_os.sh)
  execute_process(COMMAND ${TD_COMMUNITY_DIR}/packaging/tools/get_os.sh "" OUTPUT_VARIABLE TD_OS_INFO)
  MESSAGE(STATUS "The current os is " ${TD_OS_INFO})

  SET(TD_LINUX TRUE)
  IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
    SET(TD_LINUX_64 TRUE)
    MESSAGE(STATUS "The current platform is Linux 64-bit")
  ELSEIF (${CMAKE_SIZEOF_VOID_P} MATCHES 4)
    SET(TD_LINUX_32 TRUE)
    MESSAGE(STATUS "The current platform is Linux 32-bit")
  ELSE ()
    MESSAGE(FATAL_ERROR "The current platform is Linux neither 32-bit nor 64-bit, not supported yet")
    EXIT ()
  ENDIF ()

  IF (${TD_OS_INFO} MATCHES "Alpine")
    SET(TD_APLHINE TRUE)
    MESSAGE(STATUS "The current OS is Alpine, append extra flags")
  ENDIF()
ELSEIF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  SET(TD_DARWIN TRUE)
  IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
    SET(TD_DARWIN_64 TRUE)
    MESSAGE(STATUS "The current platform is Darwin 64-bit")
  ELSE ()
    MESSAGE(FATAL_ERROR "The current platform is Darwin 32-bit, not supported yet")
    EXIT ()
  ENDIF ()
ELSEIF (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  SET(TD_WINDOWS TRUE)
  IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
    SET(TD_WINDOWS_64 TRUE)
    MESSAGE(STATUS "The current platform is Windows 64-bit")
  ELSE ()
    SET(TD_WINDOWS_32 TRUE)
    MESSAGE(STATUS "The current platform is Windows 32-bit")
  ENDIF ()
ELSE()
  MESSAGE(FATAL_ERROR "The current platform is not Linux/Darwin/Windows, stop compile")
  EXIT ()
ENDIF ()

# if generate ARM version:
#   cmake -DCPUTYPE=aarch32 .. or cmake -DCPUTYPE=aarch64
IF (${CPUTYPE} MATCHES "aarch32")
  SET(TD_LINUX TRUE)
  SET(TD_LINUX_32 FALSE)
  SET(TD_ARM_32 TRUE)
  MESSAGE(STATUS "input cpuType: aarch32")
ELSEIF (${CPUTYPE} MATCHES "aarch64")
  SET(TD_LINUX TRUE)
  SET(TD_LINUX_64 FALSE)
  SET(TD_ARM_64 TRUE)
  MESSAGE(STATUS "input cpuType: aarch64")
ELSEIF (${CPUTYPE} MATCHES "mips64")
  SET(TD_LINUX TRUE)
  SET(TD_LINUX_64 FALSE)
  SET(TD_MIPS_64 TRUE)
  MESSAGE(STATUS "input cpuType: mips64")
ELSEIF (${CPUTYPE} MATCHES "x64")
  MESSAGE(STATUS "input cpuType: x64")
ELSEIF (${CPUTYPE} MATCHES "x86")
  MESSAGE(STATUS "input cpuType: x86")
ELSE ()
  MESSAGE(STATUS "input cpuType unknown " ${CPUTYPE})

  MESSAGE(STATUS "The current platform is " ${CMAKE_SYSTEM_PROCESSOR})
  IF (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)")
    MESSAGE(STATUS "The current platform is x86")
  ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "armv7l")
    SET(CPUTYPE "aarch32")
  ENDIF ()

ENDIF ()

#   cmake -DOSTYPE=Ningsi
IF (${OSTYPE} MATCHES "Ningsi60")
  SET(TD_NINGSI TRUE)
  SET(TD_NINGSI_60 TRUE)
  MESSAGE(STATUS "input osType: Ningsi60")
ELSEIF (${OSTYPE} MATCHES "Ningsi80")
  SET(TD_NINGSI TRUE)
  SET(TD_NINGSI_80 TRUE)
  MESSAGE(STATUS "input osType: Ningsi80")
ELSEIF (${OSTYPE} MATCHES "Linux")
  MESSAGE(STATUS "input osType: Linux")
ELSEIF (${OSTYPE} MATCHES "Alpine")
  MESSAGE(STATUS "input osType: Alpine")
  SET(TD_APLHINE TRUE)
ELSE ()
  MESSAGE(STATUS "input osType unknown: " ${OSTYPE})
ENDIF ()
