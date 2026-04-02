cmake_minimum_required(VERSION 3.19)
project(taosbenchmark LANGUAGES CXX)

set(CMAKE_SKIP_RPATH ON)

# Conan 2.x integration
include(${CMAKE_BINARY_DIR}/conan/conan_toolchain.cmake)
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR}/conan)
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG ON)

# Options
option(TSGEN_ENABLE_TEST "Enable testing" ON)
option(TSGEN_ENABLE_COVERAGE "Enable code coverage" OFF)
option(TSGEN_ENABLE_SANITIZER "Enable sanitizers" OFF)
option(TSGEN_BUNDLE_JEMALLOC "Use jemalloc as allocator" OFF)
option(TSGEN_BUNDLE_MIMALLOC "Use mimalloc as allocator" ON)

# Global settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Library suffixes based on platform
if(CMAKE_SYSTEM_NAME MATCHES "Windows")
  set(CMAKE_FIND_LIBRARY_SUFFIXES .lib .dll)
else()
  set(CMAKE_FIND_LIBRARY_SUFFIXES .a .so)
endif()

# System type settings
message(STATUS "CMAKE_SYSTEM_NAME: ${CMAKE_SYSTEM_NAME}")
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(TSGEN_LINUX TRUE)
  set(TSGEN_BUILD_TARGET_OSTYPE "Linux")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(TSGEN_DARWIN TRUE)
  set(TSGEN_BUILD_TARGET_OSTYPE "Darwin")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  set(TSGEN_WINDOWS TRUE)
  set(TSGEN_BUILD_TARGET_OSTYPE "Windows")
else()
  message(FATAL_ERROR "Not ported on `${CMAKE_SYSTEM_NAME}`")
  set(TSGEN_BUILD_TARGET_OSTYPE "Unknow OS")
endif()

# CPU architecture settings
message(STATUS "CMAKE_SYSTEM_PROCESSOR: ${CMAKE_SYSTEM_PROCESSOR}")
string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" TSGEN_BUILD_TARGET_CPUTYPE)
if(TSGEN_BUILD_TARGET_CPUTYPE STREQUAL "aarch64")
  set(TSGEN_BUILD_TARGET_CPUTYPE "arm64")
endif()
message(STATUS "CPU: ${TSGEN_BUILD_TARGET_CPUTYPE}")

# Build type settings
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Build type" FORCE)
endif()
string(TOUPPER ${CMAKE_BUILD_TYPE} BUILD_TYPE_UPPER)

string(TIMESTAMP TSGEN_BUILD_DATE "%Y-%m-%d %H:%M:%S %z")
if(TSGEN_BUILD_DATE MATCHES " %z$")
  string(REPLACE " %z" " +0800" TSGEN_BUILD_DATE "${TSGEN_BUILD_DATE}")
endif()
message(STATUS "Sys Info: ${TSGEN_BUILD_TARGET_OSTYPE}-${TSGEN_BUILD_TARGET_CPUTYPE} ${TSGEN_BUILD_DATE}")

# Code coverage
if(TSGEN_ENABLE_COVERAGE)
  message(STATUS "Enable code coverage")
  if(TSGEN_LINUX)
    # GCC/gcov
    set(COVERAGE_COMPILE_FLAGS "-fprofile-arcs -ftest-coverage -fprofile-update=atomic")
    set(COVERAGE_LINK_FLAGS "--coverage -lgcov")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COVERAGE_COMPILE_FLAGS} ${COVERAGE_LINK_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COVERAGE_COMPILE_FLAGS} ${COVERAGE_LINK_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${COVERAGE_LINK_FLAGS}")
  elseif(TSGEN_DARWIN)
    # Clang/llvm-cov gcov
    set(COVERAGE_COMPILE_FLAGS "--coverage")
    set(COVERAGE_LINK_FLAGS   "--coverage")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COVERAGE_COMPILE_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COVERAGE_COMPILE_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${COVERAGE_LINK_FLAGS}")
  endif()
endif()

# Sanitizer
if(TSGEN_ENABLE_SANITIZER AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-fsanitize=address)

  if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.13")
    add_link_options(-fsanitize=address)
  else()
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
  endif()

  message(STATUS "AddressSanitizer enabled")
endif()

# Memory allocator settings
if(TSGEN_BUNDLE_JEMALLOC AND TSGEN_BUNDLE_MIMALLOC)
    message(FATAL_ERROR "Cannot use both jemalloc and mimalloc at the same time")
endif()

# Set link indicators
set(ALLOCATOR_LINK_FLAGS "")
set(ALLOCATOR_LIB "")

if(TSGEN_BUNDLE_JEMALLOC)
  message(STATUS "Using jemalloc memory allocator")
  find_package(jemalloc REQUIRED)
  set(ALLOCATOR_LIB jemalloc::jemalloc)
elseif(TSGEN_BUNDLE_MIMALLOC)
  message(STATUS "Using mimalloc memory allocator")
  find_package(mimalloc REQUIRED)
  set(ALLOCATOR_LIB mimalloc-static)
else()
  message(STATUS "Using system default memory allocator")
endif()

# Global compile options based on compiler
if(MSVC)
  # MSVC compiler options
  add_compile_options(/W3 /utf-8)
  # Disable unsafe function warnings
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
  # Linker options to disable default libraries and incremental linking
  add_link_options(/NODEFAULTLIB:LIBCMT /INCREMENTAL:NO)
else()
  # GCC/Clang compiler options
  add_compile_options(
    -Wall
    -Wextra
    -Werror
  )
  # Optimization options by build type
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O0 -g)
  else()
    add_compile_options(-O3 -g)
  endif()
endif()

# Add cmake modules path
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR})
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

# Load utilities
include(Utils)
setup_colored_output()
check_requirements()

# Find dependencies
find_package(fmt REQUIRED)
find_package(ZLIB REQUIRED)
find_package(LZ4 REQUIRED)
find_package(zstd REQUIRED)
find_package(Snappy REQUIRED)
find_package(Iconv REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(luajit REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(Threads REQUIRED)
find_package(spdlog REQUIRED)
find_package(vincentlaucsb-csv-parser REQUIRED)

if(TSGEN_DARWIN)
  include_directories(/usr/local/include)
elseif(TSGEN_LINUX)
  include_directories(/usr/include)
elseif(TSGEN_WINDOWS)
  include_directories("C:/TDengine/include")
endif()

if(HAVE_GIT)
  execute_process(COMMAND git log -1 --format=%H WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} OUTPUT_VARIABLE GIT_COMMITID)
  # message(STATUS "git log result:${GIT_COMMITID}")
  if(GIT_COMMITID)
    string (REGEX REPLACE "[\n\t\r]" "" GIT_COMMITID ${GIT_COMMITID})
    SET(TSGEN_BUILD_GIT ${GIT_COMMITID})
  else()
    message(STATUS "not a git repository")
    SET(TSGEN_BUILD_GIT "no git commit id")
  endif()
else()
  message(STATUS "no git found")
  SET(TSGEN_BUILD_GIT  "no git commit id")
endif()
message(STATUS "Git Info: ${TSGEN_BUILD_GIT}")

# Test support
if(TSGEN_ENABLE_TEST)
  enable_testing()
endif()

# Add main program and test directories
add_subdirectory(src)
if(TSGEN_ENABLE_TEST)
  add_subdirectory(test)
endif()