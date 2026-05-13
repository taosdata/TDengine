###############################################################################
# MIT License
#
# Copyright (c) 2022-2023 freemine <freemine@yeah.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

list(APPEND common_SOURCES
    auxiliary.c
    enums.c
    gnu_source.c
    iconv_wrapper.c
    logger.c
    parser.c)

add_library(common_obj OBJECT ${common_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(common_obj ex_iconv)
    endif()
endif()
target_include_directories(common_obj PRIVATE
    ${ODBC_INCLUDE_DIRECTORY}
    ${ODBCINST_INCLUDE_DIRECTORY})
set_property(TARGET common_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

list(APPEND odbc_helpers_SOURCES
    odbc_helpers.c)

add_library(odbc_helpers_obj OBJECT ${odbc_helpers_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(odbc_helpers_obj ex_iconv)
    endif()
endif()
set_property(TARGET odbc_helpers_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

list(APPEND taos_helpers_SOURCES
    taos_helpers.c)

add_library(taos_helpers_obj OBJECT ${taos_helpers_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(taos_helpers_obj ex_iconv)
    endif()
endif()
set_property(TARGET taos_helpers_obj PROPERTY POSITION_INDEPENDENT_CODE 1)
target_include_directories(taos_helpers_obj PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/..)

parser_gen(ejson_parser)

set(ejson_parser_SOURCES
    ejson_parser_tab.c)

add_library(ejson_parser_obj OBJECT ${ejson_parser_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(ejson_parser_obj ex_iconv)
    endif()
    # NOTE: because 'utf8' is found in inc/ejson_parser.h
    set_source_files_properties(ejson_parser_tab.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
endif()
set_property(TARGET ejson_parser_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

