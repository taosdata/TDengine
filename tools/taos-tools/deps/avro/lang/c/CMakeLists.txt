#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
cmake_minimum_required(VERSION 3.1)
project(AvroC C)
enable_testing()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR})

# Eliminates warning about linker paths when linking both zlib and
# liblzma.
cmake_policy(SET CMP0003 NEW)

#-----------------------------------------------------------------------
# Retrieve the current version number

if (UNIX)
    execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/version.sh project
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE AVRO_VERSION_RESULT
        OUTPUT_VARIABLE AVRO_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(AVRO_VERSION_RESULT)
        message(FATAL_ERROR "Cannot determine Avro version number")
    endif(AVRO_VERSION_RESULT)

    execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/version.sh libtool
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE LIBAVRO_VERSION_RESULT
        OUTPUT_VARIABLE LIBAVRO_VERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(LIBAVRO_VERSION_RESULT)
        message(FATAL_ERROR "Cannot determine libavro version number")
    endif(LIBAVRO_VERSION_RESULT)

    execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/version.sh libcurrent
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        RESULT_VARIABLE LIBAVRO_SOVERSION_RESULT
        OUTPUT_VARIABLE LIBAVRO_SOVERSION
        OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(LIBAVRO_SOVERSION_RESULT)
        message(FATAL_ERROR "Cannot determine libavro version number")
    endif(LIBAVRO_SOVERSION_RESULT)

else(UNIX)
    # Hard code for win32 -- need to figure out how to port version.sh for
    # Windows.
    set(LIBAVRO_VERSION "22:0:0")
endif(UNIX)


#-----------------------------------------------------------------------
# Extract major.minor.patch from version number

if (UNIX)
    string(REGEX REPLACE "([0-9]+)\\..*"
        "\\1"
        AVRO_MAJOR_VERSION
        ${AVRO_VERSION}
    )
    string(REGEX REPLACE ".*\\.([0-9]+)\\..*"
        "\\1"
        AVRO_MINOR_VERSION
        ${AVRO_VERSION}
    )
    string(REGEX REPLACE ".*\\..*\\.([0-9]+).*"
        "\\1"
        AVRO_PATCH_VERSION
        ${AVRO_VERSION}
    )
    string(REGEX REPLACE ".*\\..*\\.[0-9]+(.*)"
        "\\1"
        AVRO_VERSION_EXTENSION
        ${AVRO_VERSION}
    )
endif(UNIX)

#-----------------------------------------------------------------------
# Source package support

include(CPackConfig.txt)
include(CheckLibraryExists)


if(APPLE)
    if (NOT CMAKE_INSTALL_NAME_DIR)
        set(CMAKE_INSTALL_NAME_DIR "${CMAKE_INSTALL_PREFIX}/lib")
    endif (NOT CMAKE_INSTALL_NAME_DIR)
endif(APPLE)

if(CMAKE_COMPILER_IS_GNUCC)
    add_definitions(-W -Wall)
endif(CMAKE_COMPILER_IS_GNUCC)

if (WIN32)
   # Compile win32 in C++ to allow declarations after statements
   add_definitions(/TP)
endif(WIN32)

# Uncomment to allow missing fields in the resolved-writer
# add_definitions(-DAVRO_ALLOW_MISSING_FIELDS_IN_RESOLVED_WRITER)

# Uncomment to allow non-atomic increment/decrement of reference count
# add_definitions(-DAVRO_ALLOW_NON_ATOMIC_REFCOUNT)

# Thread support (only for *nix with pthreads)
set(THREADS_LIBRARIES)
if(UNIX AND THREADSAFE AND CMAKE_COMPILER_IS_GNUCC)
    set(CMAKE_THREAD_PREFER_PTHREAD)
    find_package(Threads)

    if(NOT CMAKE_USE_PTHREADS_INIT)
        message(FATAL_ERROR "pthreads not found")
    endif(NOT CMAKE_USE_PTHREADS_INIT)

    add_definitions(-DTHREADSAFE -D_REENTRANT)
    set(THREADS_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
endif(UNIX AND THREADSAFE AND CMAKE_COMPILER_IS_GNUCC)

include_directories(${AvroC_SOURCE_DIR}/src)


# Enable codecs

find_package(ZLIB)
if (ZLIB_FOUND)
    set(ZLIB_PKG zlib)
    add_definitions(-DDEFLATE_CODEC)
    include_directories(${ZLIB_INCLUDE_DIRS})
    message("Enabled deflate codec")
else (ZLIB_FOUND)
    set(ZLIB_PKG "")
    set(ZLIB_LIBRARIES "")
    message("Disabled deflate codec. zlib not found.")
endif (ZLIB_FOUND)

find_package(Snappy)
if (SNAPPY_FOUND AND ZLIB_FOUND)  # Snappy borrows crc32 from zlib
    set(SNAPPY_PKG libsnappy)
    add_definitions(-DSNAPPY_CODEC)
    include_directories(${SNAPPY_INCLUDE_DIRS})
    message("Enabled snappy codec")
else (SNAPPY_FOUND AND ZLIB_FOUND)
    set(SNAPPY_PKG "")
    set(SNAPPY_LIBRARIES "")
    message("Disabled snappy codec. libsnappy not found or zlib not found.")
endif (SNAPPY_FOUND AND ZLIB_FOUND)

find_package(PkgConfig)
pkg_check_modules(LZMA liblzma)
if (LZMA_FOUND)
    set(LZMA_PKG liblzma)
    add_definitions(-DLZMA_CODEC)
    include_directories(${LZMA_INCLUDE_DIRS})
    link_directories(${LZMA_LIBRARY_DIRS})
    message("Enabled lzma codec")
else (LZMA_FOUND)
    set(LZMA_PKG "")
    set(LZMA_LIBRARIES "")
    message("Disabled lzma codec. liblzma not found.")
endif (LZMA_FOUND)

set(CODEC_LIBRARIES ${ZLIB_LIBRARIES} ${LZMA_LIBRARIES} ${SNAPPY_LIBRARIES})
set(CODEC_PKG "@ZLIB_PKG@ @LZMA_PKG@ @SNAPPY_PKG@")

# Jansson JSON library
pkg_check_modules(JANSSON jansson>=2.3)
if (JANSSON_FOUND)
    set(JANSSON_PKG libjansson)
    include_directories(${JANSSON_INCLUDE_DIRS})
    link_directories(${JANSSON_LIBRARY_DIRS})
else (JANSSON_FOUND)
    message(FATAL_ERROR "libjansson >=2.3 not found")
endif (JANSSON_FOUND)


add_subdirectory(src)
add_subdirectory(examples)
add_subdirectory(tests)
add_subdirectory(docs)

add_custom_target(pretty
    "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake_pretty.cmake")
