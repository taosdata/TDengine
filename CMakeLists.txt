CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

SET(CMAKE_C_STANDARD 11)
SET(CMAKE_VERBOSE_MAKEFILE ON)

#
## generate debug version:
## mkdir debug; cd debug; cmake -DCMAKE_BUILD_TYPE=Debug ..
## generate release version:
## mkdir release; cd release; cmake -DCMAKE_BUILD_TYPE=Release ..
#

#set macro definitions
ADD_DEFINITIONS(-D_REENTRANT -D_M_X64 -D__USE_POSIX -D_LIBC_REENTRANT)

#set macro definitions according to os platform
IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  ADD_DEFINITIONS(-DLINUX -D_LINUX)
ELSE ()
  MESSAGE(FATAL_ERROR "The current system is not linux, stop compile")
  EXIT ()
ENDIF ()

#set debug & release related options
SET(COMMON_FLAGS "-std=gnu99 -Wall -fPIC -malign-double -Wno-char-subscripts -malign-stringops -msse4.2 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE")
SET(DEBUG_FLAGS "-O0 -DDEBUG")

#compiler debug options
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${COMMON_FLAGS} ${DEBUG_FLAGS}")
SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${COMMON_FLAGS}")

MESSAGE(STATUS "Project source directory: " ${PROJECT_SOURCE_DIR})

IF (${CMAKE_BUILD_TYPE} MATCHES "Release")
  MESSAGE(STATUS "Build Release Version")
ELSE ()
  SET(CMAKE_BUILD_TYPE "Debug")
  MESSAGE(STATUS "Build Debug Version")
ENDIF ()

#set output directory
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/lib)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/bin)
SET(TESTS_OUTPUT_DIR ${PROJECT_BINARY_DIR}/test)

MESSAGE(STATUS "Project binary output path: " ${PROJECT_BINARY_DIR})
MESSAGE(STATUS "Executable files output path: " ${EXECUTABLE_OUTPUT_PATH})
MESSAGE(STATUS "Project library output path: " ${LIBRARY_OUTPUT_PATH})

# check for x64 platform
IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
  MESSAGE(STATUS "Check targe platform x64, pass")
  ADD_SUBDIRECTORY(deps)
  ADD_SUBDIRECTORY(src)
ELSE ()
  MESSAGE(FATAL_ERROR "Check targe platform x86, not supported yet")
ENDIF ()

SET(MAKE_INSTALL_SH "${PROJECT_SOURCE_DIR}/packaging/tools/make_install.sh")
INSTALL(CODE "MESSAGE(\"make install script: ${MAKE_INSTALL_SH}\")")
INSTALL(CODE "execute_process(COMMAND chmod 777 ${MAKE_INSTALL_SH})")
INSTALL(CODE "execute_process(COMMAND ${MAKE_INSTALL_SH} ${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR})")