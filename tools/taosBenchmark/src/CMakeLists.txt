# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
INCLUDE_DIRECTORIES(../inc)

MESSAGE(STATUS "The current platform " ${CMAKE_SYSTEM_PROCESSOR} " is detected")

IF (CMAKE_SYSTEM_PROCESSOR MATCHES "(amd64)|(AMD64)|(x86_64)")
    MESSAGE(STATUS "The current platform is amd64")
    MESSAGE(STATUS "Set CPUTYPE to x64")
    SET(CURRENT_CPUTYPE "x64")
    SET(CURRENT_PLATFORM_ARCH_STR "amd64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)")
    MESSAGE(STATUS "The current platform is x86")
    MESSAGE(STATUS "Set CPUTYPE to x86")
    SET(CURRENT_CPUTYPE "x32")
    SET(CURRENT_PLATFORM_ARCH_STR "i386")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "armv7l")
    MESSAGE(STATUS "Set CPUTYPE to aarch32")
    SET(CURRENT_CPUTYPE "aarch32")
    MESSAGE(STATUS "Set CPUTYPE to aarch32")
    SET(CURRENT_PLATFORM_ARCH_STR "arm")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
    SET(CURRENT_CPUTYPE "aarch64")
    MESSAGE(STATUS "Set CPUTYPE to aarch64")
    SET(CURRENT_PLATFORM_ARCH_STR "arm64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "mips64")
    SET(CURRENT_CPUTYPE "mips64")
    MESSAGE(STATUS "Set CPUTYPE to mips64")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
    SET(CURRENT_CPUTYPE "apple_m1")
    MESSAGE(STATUS "Set CPUTYPE to apple silicon m1")
ELSEIF (CMAKE_SYSTEM_PROCESSOR MATCHES "loongarch64")
    SET(CURRENT_CPUTYPE "loongarch64")
    MESSAGE(STATUS "Set CPUTYPE to loongarch64")
ENDIF ()

#
# collect --version information
#
MESSAGE(STATUS "collect --version show info:")
# version
IF (DEFINED BUILD_VER_NUMBER)
    ADD_DEFINITIONS(-DTD_VER_NUMBER="${BUILD_VER_NUMBER}")
    MESSAGE(STATUS "version:${BUILD_VER_NUMBER}")
ELSE ()    
    # abort build
    MESSAGE(FATAL_ERROR "build taos-tools not found BUILD_VER_NUMBER define.")
ENDIF ()

# commit id
FIND_PACKAGE(Git)
IF(GIT_FOUND)
    # get
    EXECUTE_PROCESS(
        COMMAND git log -1 --format=%H
        WORKING_DIRECTORY ${TD_COMMUNITY_DIR}
        OUTPUT_VARIABLE GIT_COMMIT_ID
        )

    STRING(SUBSTRING "${GIT_COMMIT_ID}" 0 40 TAOSBENCHMARK_COMMIT_ID)

    # show
    MESSAGE(STATUS "taosBenchmark commit id: ${TAOSBENCHMARK_COMMIT_ID}")
    # define
    ADD_DEFINITIONS(-DTAOSBENCHMARK_COMMIT_ID="${TAOSBENCHMARK_COMMIT_ID}")
ELSE()
    MESSAGE(FATAL_ERROR "build taos-tools FIND_PACKAGE(Git) failed.")
ENDIF (GIT_FOUND)

# build info
SET(BUILD_INFO "${BUILD_VER_OSTYPE}-${BUILD_VER_CPUTYPE} ${BUILD_VER_DATE}")
ADD_DEFINITIONS(-DBUILD_INFO="${BUILD_INFO}")
MESSAGE(STATUS "build:${BUILD_INFO}")
MESSAGE("")

MESSAGE(STATUS "Current system is: ${CMAKE_SYSTEM_NAME}")

IF (BUILD_CUS_NAME OR BUILD_CUS_PROMPT OR BUILD_CUS_EMAIL)
    ADD_DEFINITIONS(-I${CMAKE_CURRENT_SOURCE_DIR}/../../../../community/include/util)
ENDIF (BUILD_CUS_NAME OR BUILD_CUS_PROMPT OR BUILD_CUS_EMAIL)

IF (CMAKE_SYSTEM_NAME MATCHES "Linux")
  ADD_DEFINITIONS(-DLINUX)
ELSEIF (CMAKE_SYSTEM_NAME MATCHES "Darwin")
  ADD_DEFINITIONS(-DDARWIN)
ELSE ()
  ADD_DEFINITIONS(-DWINDOWS)
ENDIF ()
ADD_EXECUTABLE(taosBenchmark
  benchMain.c benchLog.c benchTmq.c benchQuery.c benchCsv.c benchJsonOpt.c
  benchInsert.c benchInsertMix.c benchDataMix.c wrapDb.c benchData.c
  benchCommandOpt.c benchUtil.c benchUtilDs.c benchSys.c toolstime.c
  toolsSys.c toolsString.c ../../src/pub.c
)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  DEP_ext_pthread(taosBenchmark)
  DEP_ext_msvcregex(taosBenchmark)
ENDIF ()
TARGET_LINK_LIBRARIES(taosBenchmark PRIVATE toolscJson)

# libtaos.so owns the public API and dispatches to the driver it dlopen()s;
# libtaosnative.so carries the engine internals this tool uses directly
# (functions *and* globals such as gMemPoolHandle).  Both are listed, and the
# order does not matter on Linux: the driver no longer exports the public names
# there -- it hands them to the dispatcher through tdNativeApiTable -- so even
# if CMake hoists it ahead of libtaos.so, taos_options() still lands in the
# dispatcher.  macOS keeps the driver's own export of those names, so it
# resolves the internals through libtaos.dylib's re-export of the driver dylib
# instead of naming it here.
#
# No --copy-dt-needed-entries: naming libtaosnative.so explicitly is what pulls
# the internals in, and mold rejects that option outright.
TARGET_LINK_LIBRARIES(taosBenchmark PRIVATE taos)
if(TD_LINUX)
  TARGET_LINK_LIBRARIES(taosBenchmark PRIVATE ${TAOS_NATIVE_LIB})
endif()

if(NOT WIN32)
  target_link_libraries(taosBenchmark PRIVATE stdc++)
  target_link_libraries(taosBenchmark PRIVATE m)
endif()

# benchCsv.c calls zlib (gzopen/gzwrite/gzerror/gzclose) directly.  That used to
# arrive by accident: libtaos linked os/util PUBLIC, and CMake propagates a
# static library's private dependencies ($<LINK_ONLY:...>) to its consumers, so
# libz.a was already on every tool's link line.  Hiding the bundled dependency
# symbols (cmake/third_party_symbols.cmake, --exclude-libs on Linux) ended that:
# libz's symbols are no longer exported by libtaos/libtaosnative, so name the
# archive explicitly here (same fix as tools/taosdump).
if(TD_LINUX OR TD_DARWIN)
  target_link_libraries(taosBenchmark PRIVATE ${ext_zlib_libs})
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  check_include_file("argp.h" _have_argp "-I${BREW_PREFIX}/include")
  if(NOT _have_argp)
    message(FATAL_ERROR "please run:brew install argp-standalone")
  endif()
  # Suppress ld64 duplicate static library noise caused by transitive links.
  include(CheckLinkerFlag)
  check_linker_flag(C "-Wl,-no_warn_duplicate_libraries" _have_no_warn_dup_libs)
  if(_have_no_warn_dup_libs)
    if(TARGET taosdump)
      target_link_options(taosdump PRIVATE -Wl,-no_warn_duplicate_libraries)
    endif()
    target_link_options(taosBenchmark PRIVATE -Wl,-no_warn_duplicate_libraries)
  endif()
  # brew install argp-standalone
  target_include_directories(taosBenchmark PRIVATE ${BREW_PREFIX}/include)
  td_fix_macos_client_libtaos_dep(taosBenchmark)
endif()
