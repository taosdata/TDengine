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

add_executable(basic
  basic.c
  $<TARGET_OBJECTS:common_obj>
  $<TARGET_OBJECTS:ejson_parser_obj>)

if(TODBC_WINDOWS)
  set_source_files_properties(basic.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
  target_link_libraries(basic taos_odbc_a)
else()
  target_link_libraries(basic taos_odbc_a dl)
endif()

target_include_directories(basic PRIVATE
    ${INTERNAL_INC_PATH})

add_dependencies(basic taos_odbc_a)

add_test(NAME basic
    COMMAND basic)

if(ENABLE_VALGRIND)
  add_test(NAME Vbasic
      COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/basic)
endif()

add_executable(tls_test tls_test.c)

if(TODBC_WINDOWS)
  set_source_files_properties(tls_test.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
  target_link_libraries(tls_test taos_odbc_a)
else()
  target_link_libraries(tls_test taos_odbc_a dl)
endif()

target_include_directories(tls_test PRIVATE
    ${INTERNAL_INC_PATH})

add_dependencies(tls_test taos_odbc_a)

add_test(NAME tls_test
    COMMAND tls_test)

if(ENABLE_VALGRIND)
  add_test(NAME Vtls_test
      COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/tls_test)
endif()

