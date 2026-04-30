cmake_minimum_required(VERSION 3.12)
project(boosttools C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# ── Source files ──────────────────────────────────────────────
set(BOOST_SOURCES
    src/main.c
    src/conn_pool.c
    src/schema_sync.c
    src/data_sync.c
    src/progress.c
)

add_executable(boosttools ${BOOST_SOURCES})

# ── TDengine client library ──────────────────────────────────
# Attempt to locate the installed TDengine client library.
# Priority: 1) TDENGINE_DIR env/cmake var  2) /usr/local  3) source tree
if(DEFINED TDENGINE_DIR)
    set(_TD_ROOT "${TDENGINE_DIR}")
elseif(DEFINED ENV{TDENGINE_DIR})
    set(_TD_ROOT "$ENV{TDENGINE_DIR}")
else()
    # Fallback: assume we are inside the TDengine source tree.
    set(_TD_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../..")
endif()

# Header search paths
target_include_directories(boosttools PRIVATE
    ${_TD_ROOT}/include/client
    ${_TD_ROOT}/include/common
    ${_TD_ROOT}/include/util
    ${_TD_ROOT}/include/os
    ${_TD_ROOT}/include
)

# Find libtaos (shared library)
find_library(TAOS_LIB
    NAMES taos
    HINTS
        ${_TD_ROOT}/build/build/lib
        ${_TD_ROOT}/debug/build/lib
        /usr/local/taos/driver
        /usr/lib
        /usr/local/lib
)

if(TAOS_LIB)
    message(STATUS "Found libtaos: ${TAOS_LIB}")
    target_link_libraries(boosttools PRIVATE ${TAOS_LIB})
else()
    message(WARNING "libtaos not found -- linking with -ltaos (must be in LD path)")
    target_link_libraries(boosttools PRIVATE taos)
endif()

# ── System libraries ─────────────────────────────────────────
find_package(Threads REQUIRED)
target_link_libraries(boosttools PRIVATE Threads::Threads)

# Math library (for some platforms)
target_link_libraries(boosttools PRIVATE m)

# ── Compiler flags ───────────────────────────────────────────
target_compile_options(boosttools PRIVATE
    -Wall -Wextra -Wno-unused-parameter
    $<$<CONFIG:Release>:-O2>
    $<$<CONFIG:Debug>:-g -O0 -fsanitize=address>
)

target_link_options(boosttools PRIVATE
    $<$<CONFIG:Debug>:-fsanitize=address>
)

# ── Install ──────────────────────────────────────────────────
install(TARGETS boosttools RUNTIME DESTINATION bin)

message(STATUS "")
message(STATUS "=== boosttools build configuration ===")
message(STATUS "  TDengine root:  ${_TD_ROOT}")
message(STATUS "  libtaos:        ${TAOS_LIB}")
message(STATUS "  C compiler:     ${CMAKE_C_COMPILER}")
message(STATUS "  Build type:     ${CMAKE_BUILD_TYPE}")
message(STATUS "======================================")
