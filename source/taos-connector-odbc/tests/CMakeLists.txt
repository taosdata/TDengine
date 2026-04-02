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

configure_file(test_config.h.in ${CMAKE_CURRENT_BINARY_DIR}/test_config.h)

list(APPEND test_helper_SOURCES
    test_helper.c)

add_library(test_helper_obj OBJECT ${test_helper_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(test_helper_obj ex_iconv)
    endif()
    # NOTE: because 'utf8' is found in inc/ejson_parser.h
    set_source_files_properties(test_helper.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
endif()
add_dependencies(test_helper_obj ex_cjson)
set_property(TARGET test_helper_obj PROPERTY POSITION_INDEPENDENT_CODE 1)
target_include_directories(test_helper_obj PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR} ${CJSON_INSTALL_PATH}/include)

if (TODBC_WINDOWS)
    add_subdirectory(cs)
endif ()

add_subdirectory(c)
add_subdirectory(cpp)
if(NOT USE_SAN)
  add_subdirectory(node)
  add_subdirectory(rust)
  add_subdirectory(python)
  add_subdirectory(go)
  add_subdirectory(erl)
  add_subdirectory(hs)
  add_subdirectory(lisp)
  add_subdirectory(R)
endif()

add_subdirectory(taos)
