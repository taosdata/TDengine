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
cmake_minimum_required (VERSION 3.1)

set (CMAKE_LEGACY_CYGWIN_WIN32 0)

if (NOT DEFINED CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 11)
endif()

set(CMAKE_CXX_STANDARD_REQUIRED ON)

if (CMAKE_VERSION VERSION_GREATER_EQUAL 3.0)
    # Enable MACOSX_RPATH by default
    cmake_policy (SET CMP0042 NEW)
endif()

if (NOT DEFINED CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_NO_WARNINGS)
    set (CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS_NO_WARNINGS ON)
endif()

if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt" AVRO_VERSION)
else (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/../../share/VERSION.txt"
        AVRO_VERSION)
endif (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION.txt)

string(REPLACE "\n" "" AVRO_VERSION  ${AVRO_VERSION})
string(REPLACE "." ";" AVRO_VERSION  ${AVRO_VERSION})
list(GET AVRO_VERSION 0 AVRO_VERSION_MAJOR)
list(GET AVRO_VERSION 1 AVRO_VERSION_MINOR)
list(GET AVRO_VERSION 2 AVRO_VERSION_PATCH)

project (Avro-cpp)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_SOURCE_DIR})

if (WIN32 AND NOT CYGWIN AND NOT MSYS)
    add_definitions (/EHa)
    add_definitions (
        -DNOMINMAX
        -DBOOST_REGEX_DYN_LINK
        -DBOOST_FILESYSTEM_DYN_LINK
        -DBOOST_SYSTEM_DYN_LINK
        -DBOOST_IOSTREAMS_DYN_LINK
        -DBOOST_PROGRAM_OPTIONS_DYN_LINK
        -DBOOST_ALL_NO_LIB)
endif()

if (CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
if (AVRO_ADD_PROTECTOR_FLAGS)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fstack-protector-all -D_GLIBCXX_DEBUG")
    # Unset _GLIBCXX_DEBUG for avrogencpp.cc because using Boost Program Options
    # leads to linking errors when compiling with _GLIBCXX_DEBUG as described on
    # https://stackoverflow.com/questions/19729036/
    set_source_files_properties(impl/avrogencpp.cc PROPERTIES COMPILE_FLAGS "-U_GLIBCXX_DEBUG")
endif ()
endif ()


find_package (Boost 1.38 REQUIRED
    COMPONENTS filesystem iostreams program_options regex system)

find_package(Snappy)
if (SNAPPY_FOUND)
    set(SNAPPY_PKG libsnappy)
    add_definitions(-DSNAPPY_CODEC_AVAILABLE)
    message("Enabled snappy codec")
else (SNAPPY_FOUND)
    set(SNAPPY_PKG "")
    set(SNAPPY_LIBRARIES "")
    set(SNAPPY_INCLUDE_DIR "")
    message("Disabled snappy codec. libsnappy not found.")
endif (SNAPPY_FOUND)

add_definitions (${Boost_LIB_DIAGNOSTIC_DEFINITIONS})

include_directories (api ${CMAKE_CURRENT_BINARY_DIR} ${Boost_INCLUDE_DIRS})

set (AVRO_SOURCE_FILES
        impl/Compiler.cc impl/Node.cc impl/LogicalType.cc
        impl/NodeImpl.cc impl/ResolverSchema.cc impl/Schema.cc
        impl/Types.cc impl/ValidSchema.cc impl/Zigzag.cc
        impl/BinaryEncoder.cc impl/BinaryDecoder.cc
        impl/Stream.cc impl/FileStream.cc
        impl/Generic.cc impl/GenericDatum.cc
        impl/DataFile.cc
        impl/parsing/Symbol.cc
        impl/parsing/ValidatingCodec.cc
        impl/parsing/JsonCodec.cc
        impl/parsing/ResolvingDecoder.cc
        impl/json/JsonIO.cc
        impl/json/JsonDom.cc
        impl/Resolver.cc impl/Validator.cc
        )

add_library (avrocpp SHARED ${AVRO_SOURCE_FILES})

set_property (TARGET avrocpp
    APPEND PROPERTY COMPILE_DEFINITIONS AVRO_DYN_LINK)

add_library (avrocpp_s STATIC ${AVRO_SOURCE_FILES})
target_include_directories(avrocpp_s PRIVATE ${SNAPPY_INCLUDE_DIR})

set_property (TARGET avrocpp avrocpp_s
    APPEND PROPERTY COMPILE_DEFINITIONS AVRO_SOURCE)

set_target_properties (avrocpp PROPERTIES
    VERSION ${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR}.${AVRO_VERSION_PATCH})

set_target_properties (avrocpp_s PROPERTIES
    VERSION ${AVRO_VERSION_MAJOR}.${AVRO_VERSION_MINOR}.${AVRO_VERSION_PATCH})

target_link_libraries (avrocpp ${Boost_LIBRARIES} ${SNAPPY_LIBRARIES})
target_include_directories(avrocpp PRIVATE ${SNAPPY_INCLUDE_DIR})

add_executable (precompile test/precompile.cc)

target_link_libraries (precompile avrocpp_s ${Boost_LIBRARIES} ${SNAPPY_LIBRARIES})

macro (gen file ns)
    add_custom_command (OUTPUT ${file}.hh
        COMMAND avrogencpp
            -p -
            -i ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/${file}
            -o ${file}.hh -n ${ns} -U
        DEPENDS avrogencpp ${CMAKE_CURRENT_SOURCE_DIR}/jsonschemas/${file})
    add_custom_target (${file}_hh DEPENDS ${file}.hh)
endmacro (gen)

gen (empty_record empty)
gen (bigrecord testgen)
gen (bigrecord_r testgen_r)
gen (bigrecord2 testgen2)
gen (tweet testgen3)
gen (union_array_union uau)
gen (union_map_union umu)
gen (union_conflict uc)
gen (recursive rec)
gen (reuse ru)
gen (circulardep cd)
gen (tree1 tr1)
gen (tree2 tr2)
gen (crossref cr)
gen (primitivetypes pt)
gen (cpp_reserved_words cppres)

add_executable (avrogencpp impl/avrogencpp.cc)
target_link_libraries (avrogencpp avrocpp_s ${Boost_LIBRARIES} ${SNAPPY_LIBRARIES})

enable_testing()

macro (unittest name)
    add_executable (${name} test/${name}.cc)
    target_link_libraries (${name} avrocpp ${Boost_LIBRARIES} ${SNAPPY_LIBRARIES})
    add_test (NAME ${name} WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        COMMAND ${CMAKE_CURRENT_BINARY_DIR}/${name})
endmacro (unittest)

unittest (buffertest)
unittest (unittest)
unittest (SchemaTests)
unittest (LargeSchemaTests)
unittest (CodecTests)
unittest (StreamTests)
unittest (SpecificTests)
unittest (DataFileTests)
unittest (JsonTests)
unittest (AvrogencppTests)
unittest (CompilerTests)
unittest (AvrogencppTestReservedWords)

add_dependencies (AvrogencppTestReservedWords cpp_reserved_words_hh)

add_dependencies (AvrogencppTests bigrecord_hh bigrecord_r_hh bigrecord2_hh
    tweet_hh
    union_array_union_hh union_map_union_hh union_conflict_hh
    recursive_hh reuse_hh circulardep_hh tree1_hh tree2_hh crossref_hh
    primitivetypes_hh empty_record_hh)

include (InstallRequiredSystemLibraries)

set (CPACK_PACKAGE_FILE_NAME "avrocpp-${AVRO_VERSION_MAJOR}")

include (CPack)

install (TARGETS avrocpp avrocpp_s
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION lib)

install (TARGETS avrogencpp RUNTIME DESTINATION bin)

install (DIRECTORY api/ DESTINATION include/avro
    FILES_MATCHING PATTERN *.hh)

if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release CACHE STRING
      "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel."
      FORCE)
endif (NOT CMAKE_BUILD_TYPE)
