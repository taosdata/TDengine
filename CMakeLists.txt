cmake_minimum_required(VERSION 3.16)

project(
    TDengine
    VERSION 3.0
    DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

# ============================================================================
# DEPENDENCIES
# ============================================================================
set(CMAKE_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/cmake")

function(cat IN_FILE OUT_FILE)
    file(READ ${IN_FILE} CONTENTS)
    file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction(cat IN_FILE OUT_FILE)

set(DEPS_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")
configure_file("${CMAKE_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${DEPS_TMP_FILE})

## googletest
option(BUILD_TEST "If build unit tests using googletest" OFF)

if(${BUILD_TEST})
    cat("${CMAKE_SUPPORT_DIR}/gtest_CMakeLists.txt.in" ${DEPS_TMP_FILE})
endif(${BUILD_TEST})

## download dependencies
configure_file(${DEPS_TMP_FILE} "${CMAKE_SOURCE_DIR}/deps/deps-download/CMakeLists.txt")
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/deps/deps-download")
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/deps/deps-download")

# deps
add_subdirectory(deps)

# src
add_subdirectory(src)

# tests (TODO)