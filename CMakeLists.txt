CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

SET(CMAKE_C_STANDARD 11)
SET(CMAKE_VERBOSE_MAKEFILE ON)
SET(TD_ROOT_DIR ${PROJECT_SOURCE_DIR})

#
# If it is a Linux operating system
#   1.Generate debug version:
#     mkdir debug; cd debug; 
#     cmake -DCMAKE_BUILD_TYPE=Debug ..
#   2.Generate release version:
#     mkdir release; cd release; 
#     cmake -DCMAKE_BUILD_TYPE=Release .. 
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
SET(TD_OS_WINDOWS FALSE)
SET(TD_OS_LINUX FALSE)
SET(TD_OS_ARM FALSE)
SET(TD_OS_MAC FALSE)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  
  SET(TD_OS_DIR ${PROJECT_SOURCE_DIR}/src/os/linux)
  SET(TD_LINUX TRUE)
  ADD_DEFINITIONS(-DLINUX)
  
  SET(COMMON_FLAGS "-std=gnu99 -Wall -fPIC -malign-double -g -Wno-char-subscripts -malign-stringops -msse4.2 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE")
  SET(DEBUG_FLAGS "-O0 -DDEBUG")
  SET(RELEASE_FLAGS "-O0")
  
  ADD_DEFINITIONS(-D_REENTRANT -D__USE_POSIX -D_LIBC_REENTRANT)
  
  IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
    MESSAGE(STATUS "The current platform is Linux 64-bit")
    ADD_DEFINITIONS(-D_M_X64)
  ELSE ()
    MESSAGE(FATAL_ERROR "The current platform is Linux 32-bit, not supported yet")
    EXIT ()
  ENDIF ()
  
ELSEIF (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  SET(CMAKE_GENERATOR "NMake Makefiles" CACHE INTERNAL "" FORCE)
  SET(TD_OS_DIR ${PROJECT_SOURCE_DIR}/src/os/windows)
  SET(TD_WINDOWS TRUE)
  ADD_DEFINITIONS(-DWINDOWS)
  
  SET(COMMON_FLAGS "/nologo /WX- /Oi /Oy- /Gm- /EHsc /MT /GS /Gy /fp:precise /Zc:wchar_t /Zc:forScope /Gd /errorReport:prompt /analyze-")
  SET(DEBUG_FLAGS "/Zi /W3 /GL")
  SET(RELEASE_FLAGS "/W0 /GL")	
  
  ADD_DEFINITIONS(-D_MBCS -D_CRT_SECURE_NO_DEPRECATE -D_CRT_NONSTDC_NO_DEPRECATE)  
  
  IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
    MESSAGE(STATUS "The current platform is Windows 64-bit")
    ADD_DEFINITIONS(-D_M_X64)    
  ELSE ()
    MESSAGE(FATAL_ERROR "The current platform is Windows 32-bit, not supported yet")
    EXIT ()
  ENDIF ()

ELSE ()

  MESSAGE(FATAL_ERROR "The current platform is not Linux or Windows, stop compile")
  EXIT ()
  
ENDIF ()

# Set compiler options
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${COMMON_FLAGS} ${DEBUG_FLAGS}")
SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${COMMON_FLAGS} ${RELEASE_FLAGS}")

# Set c++ compiler options
# SET(COMMON_CXX_FLAGS "${COMMON_FLAGS} -std=c++11")
# SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${COMMON_CXX_FLAGS} ${DEBUG_FLAGS}")
# SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${COMMON_CXX_FLAGS} ${RELEASE_FLAGS}")

IF (${CMAKE_BUILD_TYPE} MATCHES "Debug")
  MESSAGE(STATUS "Build Debug Version")
ELSEIF (${CMAKE_BUILD_TYPE} MATCHES "Release")
  MESSAGE(STATUS "Build Release Version")
ELSE ()
  IF (TD_WINDOWS)
    SET(CMAKE_BUILD_TYPE "Release")
	MESSAGE(STATUS "Build Release Version")
  ELSE ()
    SET(CMAKE_BUILD_TYPE "Debug")
	MESSAGE(STATUS "Build Debug Version")
  ENDIF()    
ENDIF ()

FIND_PROGRAM(TD_MVN_INSTALLED mvn)
IF (TD_MVN_INSTALLED)
  MESSAGE(STATUS "MVN is installed and JDBC will be compiled")
ELSE ()
  MESSAGE(STATUS "MVN is not installed and JDBC is not compiled")
ENDIF ()

#set output directory
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/lib)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/bin)
SET(TD_TESTS_OUTPUT_DIR ${PROJECT_BINARY_DIR}/test)

MESSAGE(STATUS "Operating system dependency directory: " ${TD_OS_DIR})
MESSAGE(STATUS "Project source directory: " ${PROJECT_SOURCE_DIR})
MESSAGE(STATUS "Project binary files output path: " ${PROJECT_BINARY_DIR})
MESSAGE(STATUS "Project executable files output path: " ${EXECUTABLE_OUTPUT_PATH})
MESSAGE(STATUS "Project library files output path: " ${LIBRARY_OUTPUT_PATH})

ADD_SUBDIRECTORY(deps)
ADD_SUBDIRECTORY(src)

IF (TD_LINUX)
  SET(TD_MAKE_INSTALL_SH "${TD_ROOT_DIR}/packaging/tools/make_install.sh")
  INSTALL(CODE "MESSAGE(\"make install script: ${TD_MAKE_INSTALL_SH}\")")
  INSTALL(CODE "execute_process(COMMAND chmod 777 ${TD_MAKE_INSTALL_SH})")
  INSTALL(CODE "execute_process(COMMAND ${TD_MAKE_INSTALL_SH} ${TD_ROOT_DIR} ${PROJECT_BINARY_DIR})")
ELSEIF (TD_WINDOWS)
  SET(CMAKE_INSTALL_PREFIX C:/TDengine)
  INSTALL(DIRECTORY ${TD_ROOT_DIR}/src/connector/go DESTINATION connector)
  INSTALL(DIRECTORY ${TD_ROOT_DIR}/src/connector/grafana DESTINATION connector)
  INSTALL(DIRECTORY ${TD_ROOT_DIR}/src/connector/python DESTINATION connector)
  INSTALL(DIRECTORY ${TD_ROOT_DIR}/tests/examples DESTINATION .)
  INSTALL(DIRECTORY ${TD_ROOT_DIR}/packaging/cfg DESTINATION .)
  INSTALL(FILES ${TD_ROOT_DIR}/src/inc/taos.h DESTINATION include)
  INSTALL(TARGETS taos RUNTIME DESTINATION driver) 
  INSTALL(TARGETS shell RUNTIME DESTINATION .)
  IF (TD_MVN_INSTALLED)
    INSTALL(FILES ${TD_ROOT_DIR}/src/connector/jdbc/target/taos-jdbcdriver-1.0.0-dist.jar DESTINATION connector/jdbc)
  ENDIF ()
ENDIF ()  

INCLUDE(CPack)