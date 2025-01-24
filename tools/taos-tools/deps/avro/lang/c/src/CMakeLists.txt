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

set(AVRO_SRC
    allocation.c
    array.c
    avro.h
    avro/allocation.h
    avro/basics.h
    avro/consumer.h
    avro/data.h
    avro/errors.h
    avro/generic.h
    avro/io.h
    avro/legacy.h
    avro/refcount.h
    avro/resolver.h
    avro/schema.h
    avro/value.h
    avro_generic_internal.h
    avro_private.h
    codec.c
    codec.h
    consumer.c
    consume-binary.c
    datafile.c
    datum.c
    datum.h
    datum_equal.c
    datum_read.c
    datum_size.c
    datum_skip.c
    datum_validate.c
    datum_value.c
    datum_write.c
    dump.c
    dump.h
    encoding.h
    encoding_binary.c
    errors.c
    generic.c
    io.c
    map.c
    memoize.c
    resolved-reader.c
    resolved-writer.c
    resolver.c
    schema.c
    schema.h
    schema_equal.c
    st.c
    st.h
    string.c
    value.c
    value-hash.c
    value-json.c
    value-read.c
    value-sizeof.c
    value-write.c
    wrapped-buffer.c
)


source_group(Avro FILES ${AVRO_SRC})

# The version.sh script gives us a VERSION that uses colon as a
# separator; we need periods.

string(REPLACE ":" "." LIBAVRO_DOT_VERSION ${LIBAVRO_VERSION})

add_library(avro-static STATIC ${AVRO_SRC})
target_link_libraries(avro-static ${JANSSON_LIBRARIES} ${CODEC_LIBRARIES} ${THREADS_LIBRARIES})
set_target_properties(avro-static PROPERTIES OUTPUT_NAME avro)

if (NOT WIN32)
# TODO: Create Windows DLLs. See https://www.cmake.org/Wiki/BuildingWinDLL
add_library(avro-shared SHARED ${AVRO_SRC})
target_link_libraries(avro-shared ${JANSSON_LIBRARIES} ${CODEC_LIBRARIES} ${THREADS_LIBRARIES})
set_target_properties(avro-shared PROPERTIES
        OUTPUT_NAME avro
        VERSION ${LIBAVRO_DOT_VERSION}
        SOVERSION ${LIBAVRO_SOVERSION})
endif(NOT WIN32)

install(FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/avro.h
        DESTINATION include)
install(DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}/avro
        DESTINATION include
        FILES_MATCHING PATTERN "*.h")

include(GNUInstallDirs)

if (WIN32)
install(TARGETS avro-static
	RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
       )
else(WIN32)
install(TARGETS avro-static avro-shared
	RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
	LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
       )
endif(WIN32)

# Install pkg-config file

set(prefix ${CMAKE_INSTALL_PREFIX})
set(VERSION ${AVRO_VERSION})
configure_file(avro-c.pc.in avro-c.pc)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/avro-c.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)

add_executable(avrocat avrocat.c)
target_link_libraries(avrocat avro-static)
install(TARGETS avrocat RUNTIME DESTINATION bin)

add_executable(avroappend avroappend.c)
target_link_libraries(avroappend avro-static)
install(TARGETS avroappend RUNTIME DESTINATION bin)

if (NOT WIN32)
#TODO: Port getopt() to Windows to compile avropipe.c and avromod.c
add_executable(avropipe avropipe.c)
target_link_libraries(avropipe avro-static)
install(TARGETS avropipe RUNTIME DESTINATION bin)

add_executable(avromod avromod.c)
target_link_libraries(avromod avro-static)
install(TARGETS avromod RUNTIME DESTINATION bin)
endif(NOT WIN32)
