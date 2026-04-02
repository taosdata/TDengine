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

parser_gen(conn_parser)

set(conn_parser_SOURCES
    conn_parser_tab.c)

add_library(conn_parser_obj OBJECT ${conn_parser_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(conn_parser_obj ex_iconv)
    endif()
endif()
target_include_directories(conn_parser_obj PRIVATE
    ${INTERNAL_INC_PATH})
set_property(TARGET conn_parser_obj PROPERTY POSITION_INDEPENDENT_CODE 1)




parser_gen(ext_parser)

set(ext_parser_SOURCES
    ext_parser_tab.c)

add_library(ext_parser_obj OBJECT ${ext_parser_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(ext_parser_obj ex_iconv)
    endif()
endif()
target_include_directories(ext_parser_obj PRIVATE
    ${INTERNAL_INC_PATH})
set_property(TARGET ext_parser_obj PROPERTY POSITION_INDEPENDENT_CODE 1)




parser_gen(sqls_parser)

set(sqls_parser_SOURCES
    sqls_parser_tab.c)

add_library(sqls_parser_obj OBJECT ${sqls_parser_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(sqls_parser_obj ex_iconv)
    endif()
endif()
target_include_directories(sqls_parser_obj PRIVATE
    ${INTERNAL_INC_PATH})
set_property(TARGET sqls_parser_obj PROPERTY POSITION_INDEPENDENT_CODE 1)


parser_gen(url_parser)

set(url_parser_SOURCES
    url_parser_tab.c)

add_library(url_parser_obj OBJECT ${url_parser_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(url_parser_obj ex_iconv)
    endif()
endif()
target_include_directories(url_parser_obj PRIVATE
    ${INTERNAL_INC_PATH})
set_property(TARGET url_parser_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

parser_gen(ts_parser)

set(ts_parser_SOURCES
    ts_parser_tab.c)

add_library(ts_parser_obj OBJECT ${ts_parser_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(ts_parser_obj ex_iconv)
    endif()
endif()
target_include_directories(ts_parser_obj PRIVATE
    ${INTERNAL_INC_PATH})
set_property(TARGET ts_parser_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

