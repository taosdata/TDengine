cmake_minimum_required(VERSION 3.0...3.31)

project(HxAdapter)

# add_library(hx_adapter SHARED src/main.cpp)
# set_target_properties(hx_adapter PROPERTIES OUTPUT_NAME "hx_adapter")

set(HxAdapter_VERSION 0.1)

option(ENABLE_ASAN "Enable AddressSanitizer" OFF)

message(STATUS "CMAKE_SYSTEM_NAME: ${CMAKE_SYSTEM_NAME}")
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(HXADPT_LINUX TRUE)
  set(HXADPT_BUILD_TARGET_OSTYPE "Linux")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(HXADPT_DARWIN TRUE)
  set(HXADPT_BUILD_TARGET_OSTYPE "Darwin")
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  set(HXADPT_WINDOWS TRUE)
  set(HXADPT_BUILD_TARGET_OSTYPE "Windows")
else()
  message(FATAL_ERROR "Not ported on `${CMAKE_SYSTEM_NAME}`")
  set(HXADPT_BUILD_TARGET_OSTYPE "Unknow OS")
endif()


# judging platform architecture
if (CMAKE_SIZEOF_VOID_P EQUAL 8)
  message(STATUS "Building for 64-bit platform")
  set(HXADPT_BUILD_TARGET_CPUTYPE "x64")
else()
  message(STATUS "Building for 32-bit platform")
  set(HXADPT_BUILD_TARGET_CPUTYPE "x86")
  set(HXADPT_X86 TRUE)
endif()

string(TIMESTAMP HXADPT_BUILD_DATE "%Y-%m-%d %H:%M:%S %z")
message(STATUS "Sys Info: ${HXADPT_BUILD_TARGET_OSTYPE}-${HXADPT_BUILD_TARGET_CPUTYPE}-${HXADPT_BUILD_DATE}")

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(COMMON_COMPILE_OPTIONS "")

if(NOT HXADPT_WINDOWS)
  set(COMMON_COMPILE_OPTIONS -Wall -Wextra -Werror -Wno-unused-function -fvisibility=hidden)
else()
  set(COMMON_COMPILE_OPTIONS /W3 /WX)
endif()

if(CMAKE_BUILD_TYPE MATCHES "Release")
  message(STATUS "Release mode")
  if(NOT HXADPT_WINDOWS)
    string(REPLACE "-Werror" "" COMMON_COMPILE_OPTIONS "${COMMON_COMPILE_OPTIONS}")
    add_compile_options(
      ${COMMON_COMPILE_OPTIONS}
      -O2
      -Wno-unused-variable
    )
  else()
    add_compile_options(
      ${COMMON_COMPILE_OPTIONS}
      /O2
      /wd4101
    )
  endif()
else()
  message(STATUS "Debug mode")
  if(NOT HXADPT_WINDOWS)
    add_compile_options(
      ${COMMON_COMPILE_OPTIONS}
      -O0
    )
  else()
    add_compile_options(
      ${COMMON_COMPILE_OPTIONS}
      /Od
    )
  endif()
endif()


if(ENABLE_ASAN AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(-fsanitize=address)

  if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.13")
    add_link_options(-fsanitize=address)
  else()
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
  endif()

  message(STATUS "AddressSanitizer enabled")
endif()

include(CTest)
include(cmake/macros.cmake)

set_colorful()
check_requirements()

include_directories(inc)
if(HXADPT_DARWIN)
  include_directories(/usr/local/include)
  link_directories(/usr/local/lib)
elseif(HXADPT_WINDOWS)
  include_directories(C:/TDengine/include)
  link_directories(C:/TDengine/driver)

  # better set /source-charset:utf-8 for source-file-specific compile_options
  # add_compile_options(/source-charset:utf-8)
  add_compile_options(/DICONV_CONST=)
endif()
include_directories(${CMAKE_CURRENT_BINARY_DIR})


if(HAVE_GIT)
  execute_process(COMMAND git log -1 --format=%H WORKING_DIRECTORY ${PROJECT_SOURCE_DIR} OUTPUT_VARIABLE GIT_COMMITID)
  # message(STATUS "git log result:${GIT_COMMITID}")
  if(GIT_COMMITID)
    string(REGEX REPLACE "[\n\t\r]" "" GIT_COMMITID ${GIT_COMMITID})
    SET(HXADPT_BUILD_GIT ${GIT_COMMITID})
  else()
    message(STATUS "not a git repository")
    SET(HXADPT_BUILD_GIT "no git commit id")
  endif()
else()
  message(STATUS "no git found")
  SET(HXADPT_BUILD_GIT "no git commit id")
endif()
message(STATUS "Git Info: ${HXADPT_BUILD_GIT}")


if(HAVE_CPPCHECK)
  message(STATUS "have cppcheck")
  add_custom_target(
      cppcheck
      COMMAND ${CPPCHECK} --enable=all --inline-suppr --suppress=missingIncludeSystem --std=c++17 --quiet --template=gcc ${PROJECT_SOURCE_DIR}
      COMMENT "${BoldGreen}Running Cppcheck...${ColorReset}"
      VERBATIM
  )
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  if(HAVE_CLANG)
    message(STATUS "have clang")
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
    add_custom_target(
        clang-analyze
        COMMAND ${SCAN_BUILD_COMMAND} ${CMAKE_COMMAND} --build .
        COMMENT "${BoldGreen}Running Clang Static Analyzer...${ColorReset}"
        VERBATIM
    )
  endif()
endif()


add_subdirectory(src)
add_subdirectory(tests)

