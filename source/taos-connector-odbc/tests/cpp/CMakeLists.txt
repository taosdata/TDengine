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

add_executable(SQLBindCol
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:os_port_obj>
    SQLBindCol_ref.cpp)

target_include_directories(SQLBindCol PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/..)
if(NOT TODBC_WINDOWS)
  target_link_libraries(SQLBindCol odbc pthread)
else()
  set_source_files_properties(SQLBindCol_ref.cpp PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
  target_link_libraries(SQLBindCol odbc32)
endif()

add_test(NAME SQLBindCol
    COMMAND SQLBindCol)

if(ENABLE_VALGRIND)
  add_test(NAME VSQLBindCol
      COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/SQLBindCol)
endif()

if(ENABLE_MYSQL_TEST)
  add_test(NAME mySQLBindCol
      COMMAND SQLBindCol mysql)

  if(ENABLE_VALGRIND)
    add_test(NAME VmySQLBindCol
        COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/SQLBindCol mysql)
  endif()
endif()
