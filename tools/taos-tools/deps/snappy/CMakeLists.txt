# Copyright 2019 Google Inc. All Rights Reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#
#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the name of Google Inc. nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

cmake_minimum_required(VERSION 3.1)
project(Snappy VERSION 1.1.8 LANGUAGES C CXX)

# C++ standard can be overridden when this is used as a sub-project.
if(NOT CMAKE_CXX_STANDARD)
  # This project requires C++11.
  set(CMAKE_CXX_STANDARD 11)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
  set(CMAKE_CXX_EXTENSIONS OFF)
endif(NOT CMAKE_CXX_STANDARD)

# BUILD_SHARED_LIBS is a standard CMake variable, but we declare it here to make
# it prominent in the GUI.
option(BUILD_SHARED_LIBS "Build shared libraries(DLLs)." OFF)

option(SNAPPY_BUILD_TESTS "Build Snappy's own tests." ON)

option(SNAPPY_FUZZING_BUILD "Build Snappy for fuzzing." OFF)

option(SNAPPY_REQUIRE_AVX "Target processors with AVX support." OFF)

option(SNAPPY_REQUIRE_AVX2 "Target processors with AVX2 support." OFF)

option(SNAPPY_INSTALL "Install Snappy's header and library" ON)

include(TestBigEndian)
test_big_endian(SNAPPY_IS_BIG_ENDIAN)

include(CheckIncludeFile)
check_include_file("byteswap.h" HAVE_BYTESWAP_H)
check_include_file("sys/endian.h" HAVE_SYS_ENDIAN_H)
check_include_file("sys/mman.h" HAVE_SYS_MMAN_H)
check_include_file("sys/resource.h" HAVE_SYS_RESOURCE_H)
check_include_file("sys/time.h" HAVE_SYS_TIME_H)
check_include_file("sys/uio.h" HAVE_SYS_UIO_H)
check_include_file("unistd.h" HAVE_UNISTD_H)
check_include_file("windows.h" HAVE_WINDOWS_H)

include(CheckLibraryExists)
check_library_exists(z zlibVersion "" HAVE_LIBZ)
check_library_exists(lzo2 lzo1x_1_15_compress "" HAVE_LIBLZO2)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("/arch:AVX" HAVE_VISUAL_STUDIO_ARCH_AVX)
CHECK_CXX_COMPILER_FLAG("/arch:AVX2" HAVE_VISUAL_STUDIO_ARCH_AVX2)
CHECK_CXX_COMPILER_FLAG("-mavx" HAVE_CLANG_MAVX)
CHECK_CXX_COMPILER_FLAG("-mbmi2" HAVE_CLANG_MBMI2)
if(SNAPPY_REQUIRE_AVX2)
  if(HAVE_VISUAL_STUDIO_ARCH_AVX2)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX2")
  endif(HAVE_VISUAL_STUDIO_ARCH_AVX2)
  if(HAVE_CLANG_MAVX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
  endif(HAVE_CLANG_MAVX)
  if(HAVE_CLANG_MBMI2)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mbmi2")
  endif(HAVE_CLANG_MBMI2)
elseif (SNAPPY_REQUIRE_AVX)
  if(HAVE_VISUAL_STUDIO_ARCH_AVX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX")
  endif(HAVE_VISUAL_STUDIO_ARCH_AVX)
  if(HAVE_CLANG_MAVX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mavx")
  endif(HAVE_CLANG_MAVX)
endif(SNAPPY_REQUIRE_AVX2)

include(CheckCXXSourceCompiles)
check_cxx_source_compiles("
int main() {
  return __builtin_expect(0, 1);
}" HAVE_BUILTIN_EXPECT)

check_cxx_source_compiles("
int main() {
  return __builtin_ctzll(0);
}" HAVE_BUILTIN_CTZ)

check_cxx_source_compiles("
#include <tmmintrin.h>

int main() {
  const __m128i *src = 0;
  __m128i dest;
  const __m128i shuffle_mask = _mm_load_si128(src);
  const __m128i pattern = _mm_shuffle_epi8(_mm_loadl_epi64(src), shuffle_mask);
  _mm_storeu_si128(&dest, pattern);
  return 0;
}" SNAPPY_HAVE_SSSE3)

check_cxx_source_compiles("
#include <immintrin.h>
int main() {
  return _bzhi_u32(0, 1);
}" SNAPPY_HAVE_BMI2)

include(CheckSymbolExists)
check_symbol_exists("mmap" "sys/mman.h" HAVE_FUNC_MMAP)
check_symbol_exists("sysconf" "unistd.h" HAVE_FUNC_SYSCONF)

find_package(GTest QUIET)
if(GTEST_FOUND)
  set(HAVE_GTEST 1)
endif(GTEST_FOUND)

find_package(Gflags QUIET)
if(GFLAGS_FOUND)
  set(HAVE_GFLAGS 1)
endif(GFLAGS_FOUND)

configure_file(
  "cmake/config.h.in"
  "${PROJECT_BINARY_DIR}/config.h"
)

# We don't want to define HAVE_ macros in public headers. Instead, we use
# CMake's variable substitution with 0/1 variables, which will be seen by the
# preprocessor as constants.
set(HAVE_SYS_UIO_H_01 ${HAVE_SYS_UIO_H})
if(NOT HAVE_SYS_UIO_H_01)
  set(HAVE_SYS_UIO_H_01 0)
endif(NOT HAVE_SYS_UIO_H_01)

if (SNAPPY_FUZZING_BUILD)
  if (NOT "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
    message(WARNING "Fuzzing builds are only supported with Clang")
  endif (NOT "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")

  if(NOT CMAKE_CXX_FLAGS MATCHES "-fsanitize=address")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  endif(NOT CMAKE_CXX_FLAGS MATCHES "-fsanitize=address")

  if(NOT CMAKE_CXX_FLAGS MATCHES "-fsanitize=fuzzer-no-link")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=fuzzer-no-link")
  endif(NOT CMAKE_CXX_FLAGS MATCHES "-fsanitize=fuzzer-no-link")
endif (SNAPPY_FUZZING_BUILD)

configure_file(
  "snappy-stubs-public.h.in"
  "${PROJECT_BINARY_DIR}/snappy-stubs-public.h")

add_library(snappy "")
target_sources(snappy
  PRIVATE
    "snappy-internal.h"
    "snappy-stubs-internal.h"
    "snappy-c.cc"
    "snappy-sinksource.cc"
    "snappy-stubs-internal.cc"
    "snappy.cc"
    "${PROJECT_BINARY_DIR}/config.h"

  # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
  $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/snappy-c.h>
    $<INSTALL_INTERFACE:include/snappy-c.h>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/snappy-sinksource.h>
    $<INSTALL_INTERFACE:include/snappy-sinksource.h>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/snappy.h>
    $<INSTALL_INTERFACE:include/snappy.h>
    $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/snappy-stubs-public.h>
    $<INSTALL_INTERFACE:include/snappy-stubs-public.h>
)
target_include_directories(snappy
  PUBLIC
    $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}>
    $<INSTALL_INTERFACE:include>
)
set_target_properties(snappy
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

target_compile_definitions(snappy PRIVATE -DHAVE_CONFIG_H)
if(BUILD_SHARED_LIBS)
  set_target_properties(snappy PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif(BUILD_SHARED_LIBS)

if(SNAPPY_BUILD_TESTS)
  enable_testing()

  add_executable(snappy_unittest "")
  target_sources(snappy_unittest
    PRIVATE
      "snappy_unittest.cc"
      "snappy-test.cc"
  )
  target_compile_definitions(snappy_unittest PRIVATE -DHAVE_CONFIG_H)
  target_link_libraries(snappy_unittest snappy ${GFLAGS_LIBRARIES})

  if(HAVE_LIBZ)
    target_link_libraries(snappy_unittest z)
  endif(HAVE_LIBZ)
  if(HAVE_LIBLZO2)
    target_link_libraries(snappy_unittest lzo2)
  endif(HAVE_LIBLZO2)

  target_include_directories(snappy_unittest
    BEFORE PRIVATE
      "${PROJECT_SOURCE_DIR}"
      "${GTEST_INCLUDE_DIRS}"
      "${GFLAGS_INCLUDE_DIRS}"
  )

  add_test(
    NAME snappy_unittest
    WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
    COMMAND "${PROJECT_BINARY_DIR}/snappy_unittest")
endif(SNAPPY_BUILD_TESTS)

if(SNAPPY_FUZZING_BUILD)
  add_executable(snappy_compress_fuzzer "")
  target_sources(snappy_compress_fuzzer
    PRIVATE "snappy_compress_fuzzer.cc"
  )
  target_link_libraries(snappy_compress_fuzzer snappy)
  set_target_properties(snappy_compress_fuzzer
    PROPERTIES LINK_FLAGS "-fsanitize=fuzzer"
  )

  add_executable(snappy_uncompress_fuzzer "")
  target_sources(snappy_uncompress_fuzzer
    PRIVATE "snappy_uncompress_fuzzer.cc"
  )
  target_link_libraries(snappy_uncompress_fuzzer snappy)
  set_target_properties(snappy_uncompress_fuzzer
    PROPERTIES LINK_FLAGS "-fsanitize=fuzzer"
  )
endif(SNAPPY_FUZZING_BUILD)

# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

if(SNAPPY_INSTALL)
  install(TARGETS snappy
    EXPORT SnappyTargets
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  )
  install(
    FILES
      "snappy-c.h"
      "snappy-sinksource.h"
      "snappy.h"
      "${PROJECT_BINARY_DIR}/snappy-stubs-public.h"
    DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  )

  include(CMakePackageConfigHelpers)
  configure_package_config_file(
    "cmake/${PROJECT_NAME}Config.cmake.in"
    "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
  write_basic_package_version_file(
    "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
    COMPATIBILITY SameMajorVersion
  )
  install(
    EXPORT SnappyTargets
    NAMESPACE Snappy::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
  install(
    FILES
      "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake"
      "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake"
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}"
  )
endif(SNAPPY_INSTALL)
