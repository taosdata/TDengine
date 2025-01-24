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

set (AVRO_DOC_SRC
    index.txt
)

# TODO(dln): Use FindAsciidoc script instead.
message(STATUS "Searching for asciidoc...")
find_program(ASCIIDOC_EXECUTABLE asciidoc)
find_program(SOURCE_HIGHLIGHT_EXECUTABLE source-highlight)

if (ASCIIDOC_EXECUTABLE AND SOURCE_HIGHLIGHT_EXECUTABLE)
    foreach(_file ${AVRO_DOC_SRC})
        get_filename_component(_file_we ${_file} NAME_WE)
        set(_file_path "${CMAKE_CURRENT_SOURCE_DIR}/${_file}")
        set(_html_out "${CMAKE_CURRENT_BINARY_DIR}/${_file_we}.html")
        add_custom_command(
            OUTPUT "${_html_out}"
            COMMAND ${ASCIIDOC_EXECUTABLE}
                -a avro_version=${AVRO_VERSION}
                -a libavro_version=${LIBAVRO_VERSION}
                -a toc
                --unsafe -n -o "${_html_out}" "${_file_path}"
            DEPENDS "${_file_path}"
            COMMENT "asciidoc ${_file}"
        )
        install(FILES "${_html_out}" DESTINATION share/doc/avro-c)
        add_custom_target("${_file_we}_html" ALL echo -n
            DEPENDS "${_file}" "${_html_out}"
        )
        add_custom_target(docs DEPENDS "${_html_out}")
    endforeach(_file)
else(ASCIIDOC_EXECUTABLE AND SOURCE_HIGHLIGHT_EXECUTABLE)
    message(WARNING "asciidoc and/or source-highlight not found. HTML documentation will *NOT* be built.")
endif(ASCIIDOC_EXECUTABLE AND SOURCE_HIGHLIGHT_EXECUTABLE)

