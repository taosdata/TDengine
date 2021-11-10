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
include(${CMAKE_SUPPORT_DIR}/cmake.options)

function(cat IN_FILE OUT_FILE)
    file(READ ${IN_FILE} CONTENTS)
    file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction(cat IN_FILE OUT_FILE)

set(DEPS_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")
configure_file("${CMAKE_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${DEPS_TMP_FILE})

## googletest
if(${BUILD_TEST})
    cat("${CMAKE_SUPPORT_DIR}/gtest_CMakeLists.txt.in" ${DEPS_TMP_FILE})
endif(${BUILD_TEST})

## lz4
cat("${CMAKE_SUPPORT_DIR}/lz4_CMakeLists.txt.in" ${DEPS_TMP_FILE})

## zlib
cat("${CMAKE_SUPPORT_DIR}/zlib_CMakeLists.txt.in" ${DEPS_TMP_FILE})

## cJson
cat("${CMAKE_SUPPORT_DIR}/cjson_CMakeLists.txt.in" ${DEPS_TMP_FILE})

## leveldb
if(${BUILD_WITH_LEVELDB})
    cat("${CMAKE_SUPPORT_DIR}/leveldb_CMakeLists.txt.in" ${DEPS_TMP_FILE})
endif(${BUILD_WITH_LEVELDB})

## rocksdb
if(${BUILD_WITH_ROCKSDB})
    cat("${CMAKE_SUPPORT_DIR}/rocksdb_CMakeLists.txt.in" ${DEPS_TMP_FILE})
    add_definitions(-DUSE_ROCKSDB)
endif(${BUILD_WITH_ROCKSDB})

## lucene
if(${BUILD_WITH_LUCENE})
    cat("${CMAKE_SUPPORT_DIR}/lucene_CMakeLists.txt.in" ${DEPS_TMP_FILE})
endif(${BUILD_WITH_LUCENE})

## NuRaft
if(${BUILD_WITH_NURAFT})
    cat("${CMAKE_SUPPORT_DIR}/nuraft_CMakeLists.txt.in" ${DEPS_TMP_FILE})
endif(${BUILD_WITH_NURAFT})

## download dependencies
configure_file(${DEPS_TMP_FILE} "${CMAKE_SOURCE_DIR}/deps/deps-download/CMakeLists.txt")
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/deps/deps-download")
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/deps/deps-download")

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC -gdwarf-2 -msse4.2 -mfma")

# deps
add_subdirectory(deps)

# api
add_library(api INTERFACE)
target_include_directories(api INTERFACE "include/client")

# src
add_subdirectory(source)

# docs 
# https://vicrucann.github.io/tutorials/quick-cmake-doxygen/
if(${BUILD_DOCS})
    find_package(Doxygen)
    if (DOXYGEN_FOUND)
        # Build the doc
        set(DOXYGEN_IN ${CMAKE_SOURCE_DIR}/docs/Doxyfile.in)
        set(DOXYGEN_OUT ${CMAKE_BINARY_DIR}/Doxyfile)

        configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
        message("Doxygen build start")

        add_custom_target(
            tdengine_doxygen ALL
            COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            COMMENT "Generating API doxumentation with Doxygen"
            VERBATIM
        )
    else(DOXYGEN_FOUND)
        message("Doxygen need to be installed to generate the doxygen documentation")
    endif(DOXYGEN_FOUND)
endif(${BUILD_DOCS})

# tests (TODO)
