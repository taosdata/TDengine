CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

SET(CMAKE_C_STANDARD 11)
SET(CMAKE_VERBOSE_MAKEFILE ON)

#
# If need to set debug options
#   1.Generate debug version:
#     mkdir debug; cd debug;
#     cmake -DCMAKE_BUILD_TYPE=Debug ..
#   2.Generate release version:
#     mkdir release; cd release;
#     cmake -DCMAKE_BUILD_TYPE=Release ..
#

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

IF (NOT DEFINED TD_CLUSTER)
  MESSAGE(STATUS "Build the Lite Version")
  SET(TD_CLUSTER FALSE)
  SET(TD_LITE TRUE)

  SET(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR})
  MESSAGE(STATUS "Community directory: " ${TD_COMMUNITY_DIR})


  # Set macro definitions according to os platform
  SET(TD_LINUX_64 FALSE)
  SET(TD_LINUX_32 FALSE)
  SET(TD_ARM FALSE)
  SET(TD_ARM_64 FALSE)
  SET(TD_ARM_32 FALSE)
  SET(TD_MIPS_64 FALSE)
  SET(TD_DARWIN_64 FALSE)
  SET(TD_WINDOWS_64 FALSE)

  # if generate ARM version:
  #   cmake -DARMVER=arm32 .. or cmake -DARMVER=arm64
  IF (${ARMVER} MATCHES "arm32")
    SET(TD_ARM TRUE)
    SET(TD_ARM_32 TRUE)
    ADD_DEFINITIONS(-D_TD_ARM_)
    ADD_DEFINITIONS(-D_TD_ARM_32_)
  ELSEIF (${ARMVER} MATCHES "arm64")
    SET(TD_ARM TRUE)
    SET(TD_ARM_64 TRUE)
    ADD_DEFINITIONS(-D_TD_ARM_)
    ADD_DEFINITIONS(-D_TD_ARM_64_)
  ENDIF ()

  IF (TD_ARM)
    ADD_DEFINITIONS(-D_TD_ARM_)
    IF (TD_ARM_32)
      ADD_DEFINITIONS(-D_TD_ARM_32_)
    ELSEIF (TD_ARM_64)
      ADD_DEFINITIONS(-D_TD_ARM_64_)
    ELSE ()
      EXIT ()
    ENDIF ()
  ENDIF ()

  IF (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
    IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
      SET(TD_LINUX_64 TRUE)
      SET(TD_OS_DIR ${TD_COMMUNITY_DIR}/src/os/linux)
      ADD_DEFINITIONS(-D_M_X64)
      MESSAGE(STATUS "The current platform is Linux 64-bit")
  ELSEIF (${CMAKE_SIZEOF_VOID_P} MATCHES 4)
    IF (TD_ARM)
      SET(TD_LINUX_32 TRUE)
      SET(TD_OS_DIR ${TD_COMMUNITY_DIR}/src/os/linux)
      #ADD_DEFINITIONS(-D_M_IX86)
      MESSAGE(STATUS "The current platform is Linux 32-bit")
    ELSE ()
      MESSAGE(FATAL_ERROR "The current platform is Linux 32-bit, but no ARM not supported yet")
      EXIT ()
    ENDIF ()
  ELSE ()
      MESSAGE(FATAL_ERROR "The current platform is Linux neither 32-bit nor 64-bit, not supported yet")
      EXIT ()
    ENDIF ()
  ELSEIF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
      SET(TD_DARWIN_64 TRUE)
      SET(TD_OS_DIR ${TD_COMMUNITY_DIR}/src/os/darwin)
      MESSAGE(STATUS "The current platform is Darwin 64-bit")
    ELSE ()
      MESSAGE(FATAL_ERROR "The current platform is Darwin 32-bit, not supported yet")
      EXIT ()
    ENDIF ()
  ELSEIF (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
    IF (${CMAKE_SIZEOF_VOID_P} MATCHES 8)
      SET(TD_WINDOWS_64 TRUE)
      SET(TD_OS_DIR ${TD_COMMUNITY_DIR}/src/os/windows)
      ADD_DEFINITIONS(-D_M_X64)
      MESSAGE(STATUS "The current platform is Windows 64-bit")
    ELSE ()
      MESSAGE(FATAL_ERROR "The current platform is Windows 32-bit, not supported yet")
      EXIT ()
    ENDIF ()
  ELSE()
    MESSAGE(FATAL_ERROR "The current platform is not Linux/Darwin/Windows, stop compile")
    EXIT ()
  ENDIF ()

  FIND_PROGRAM(TD_MVN_INSTALLED mvn)
  IF (TD_MVN_INSTALLED)
    MESSAGE(STATUS "MVN is installed and JDBC will be compiled")
  ELSE ()
    MESSAGE(STATUS "MVN is not installed and JDBC is not compiled")
  ENDIF ()

  #
  # debug flag  
  #
  # ADD_DEFINITIONS(-D_CHECK_HEADER_FILE_)
  IF (${MEM_CHECK} MATCHES "true")
    ADD_DEFINITIONS(-DTAOS_MEM_CHECK)
  ENDIF ()

  IF (TD_CLUSTER)
    ADD_DEFINITIONS(-DCLUSTER)
    ADD_DEFINITIONS(-DTSDB_REPLICA_MAX_NUM=3)
  ELSE ()
    ADD_DEFINITIONS(-DLITE)
    ADD_DEFINITIONS(-DTSDB_REPLICA_MAX_NUM=1)
  ENDIF ()
  IF (TD_LINUX_64)
    SET(DEBUG_FLAGS "-O0 -DDEBUG")
    SET(RELEASE_FLAGS "-O0")
    IF (NOT TD_ARM)
      IF (${CMAKE_CXX_COMPILER_ID} MATCHES "Clang")
        SET(COMMON_FLAGS "-std=gnu99 -Wall -fPIC -malign-double -g -Wno-char-subscripts -msse4.2 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE")
      ELSE ()
        SET(COMMON_FLAGS "-std=gnu99 -Wall -fPIC -malign-double -g -Wno-char-subscripts -malign-stringops -msse4.2 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE")
      ENDIF ()
    ELSE ()
      SET(COMMON_FLAGS "-std=gnu99 -Wall -fPIC -g -Wno-char-subscripts -fsigned-char -munaligned-access -fpack-struct=8 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE")
    ENDIF ()
    ADD_DEFINITIONS(-DLINUX)
    ADD_DEFINITIONS(-D_REENTRANT -D__USE_POSIX -D_LIBC_REENTRANT)
  ELSEIF (TD_LINUX_32)
    IF (NOT TD_ARM)
      EXIT ()
    ENDIF ()
    SET(DEBUG_FLAGS "-O0 -DDEBUG")
    SET(RELEASE_FLAGS "-O0")
    SET(COMMON_FLAGS "-std=gnu99 -Wall -fPIC -g -Wno-char-subscripts -fsigned-char -munaligned-access -fpack-struct=8 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE")
    ADD_DEFINITIONS(-DLINUX)
    ADD_DEFINITIONS(-D_REENTRANT -D__USE_POSIX -D_LIBC_REENTRANT)
    ADD_DEFINITIONS(-DUSE_LIBICONV)
  ELSEIF (TD_WINDOWS_64)
    SET(CMAKE_GENERATOR "NMake Makefiles" CACHE INTERNAL "" FORCE)
    SET(COMMON_FLAGS "/nologo /WX- /Oi /Oy- /Gm- /EHsc /MT /GS /Gy /fp:precise /Zc:wchar_t /Zc:forScope /Gd /errorReport:prompt /analyze-")
    SET(DEBUG_FLAGS "/Zi /W3 /GL")
    SET(RELEASE_FLAGS "/W0 /GL")
    ADD_DEFINITIONS(-DWINDOWS)
    ADD_DEFINITIONS(-D__CLEANUP_C)
    ADD_DEFINITIONS(-DPTW32_STATIC_LIB)
    ADD_DEFINITIONS(-DPTW32_BUILD)
    ADD_DEFINITIONS(-D_MBCS -D_CRT_SECURE_NO_DEPRECATE -D_CRT_NONSTDC_NO_DEPRECATE)
  ELSEIF (TD_DARWIN_64)
    SET(COMMON_FLAGS "-std=gnu99 -Wall -fPIC -malign-double -g -Wno-char-subscripts -msse4.2 -D_FILE_OFFSET_BITS=64 -D_LARGE_FILE -Wno-unused-variable -Wno-bitfield-constant-conversion")
    SET(DEBUG_FLAGS "-O0 -DDEBUG")
    SET(RELEASE_FLAGS "-O0")
    ADD_DEFINITIONS(-DDARWIN)
    ADD_DEFINITIONS(-D_REENTRANT -D__USE_POSIX -D_LIBC_REENTRANT)
  ELSE ()
    MESSAGE(FATAL_ERROR "The current platform is not support yet, stop compile")
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
    IF (TD_WINDOWS_64)
      SET(CMAKE_BUILD_TYPE "Release")
      MESSAGE(STATUS "Build Release Version in Windows as default")
    ELSE ()
      SET(CMAKE_BUILD_TYPE "Debug")
      MESSAGE(STATUS "Build Debug Version as default")
    ENDIF()
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

  IF (TD_LINUX_64)
    SET(TD_MAKE_INSTALL_SH "${TD_COMMUNITY_DIR}/packaging/tools/make_install.sh")
    INSTALL(CODE "MESSAGE(\"make install script: ${TD_MAKE_INSTALL_SH}\")")
    INSTALL(CODE "execute_process(COMMAND chmod 777 ${TD_MAKE_INSTALL_SH})")
    INSTALL(CODE "execute_process(COMMAND ${TD_MAKE_INSTALL_SH} ${TD_COMMUNITY_DIR} ${PROJECT_BINARY_DIR})")
  ELSEIF (TD_LINUX_32)
    IF (NOT TD_ARM)
      EXIT ()
    ENDIF ()
    SET(TD_MAKE_INSTALL_SH "${TD_COMMUNITY_DIR}/packaging/tools/make_install.sh")
    INSTALL(CODE "MESSAGE(\"make install script: ${TD_MAKE_INSTALL_SH}\")")
    INSTALL(CODE "execute_process(COMMAND chmod 777 ${TD_MAKE_INSTALL_SH})")
    INSTALL(CODE "execute_process(COMMAND ${TD_MAKE_INSTALL_SH} ${TD_COMMUNITY_DIR} ${PROJECT_BINARY_DIR})")  
  ELSEIF (TD_WINDOWS_64)
    SET(CMAKE_INSTALL_PREFIX C:/TDengine)
    INSTALL(DIRECTORY ${TD_COMMUNITY_DIR}/src/connector/go DESTINATION connector)
    INSTALL(DIRECTORY ${TD_COMMUNITY_DIR}/src/connector/grafana DESTINATION connector)
    INSTALL(DIRECTORY ${TD_COMMUNITY_DIR}/src/connector/python DESTINATION connector)
    INSTALL(DIRECTORY ${TD_COMMUNITY_DIR}/tests/examples DESTINATION .)
    INSTALL(DIRECTORY ${TD_COMMUNITY_DIR}/packaging/cfg DESTINATION .)
    INSTALL(FILES ${TD_COMMUNITY_DIR}/src/inc/taos.h DESTINATION include)
    INSTALL(FILES ${LIBRARY_OUTPUT_PATH}/taos.lib DESTINATION driver)
    INSTALL(FILES ${LIBRARY_OUTPUT_PATH}/taos.exp DESTINATION driver)
    INSTALL(FILES ${LIBRARY_OUTPUT_PATH}/taos.dll DESTINATION driver)
    INSTALL(FILES ${EXECUTABLE_OUTPUT_PATH}/taos.exe DESTINATION .)
    #INSTALL(TARGETS taos RUNTIME DESTINATION driver)
    #INSTALL(TARGETS shell RUNTIME DESTINATION .)
    IF (TD_MVN_INSTALLED)
      INSTALL(FILES ${LIBRARY_OUTPUT_PATH}/taos-jdbcdriver-1.0.2-dist.jar DESTINATION connector/jdbc)
    ENDIF ()
  ENDIF ()
ENDIF ()

ADD_SUBDIRECTORY(deps)
ADD_SUBDIRECTORY(src)

INCLUDE(CPack)
