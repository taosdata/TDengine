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

if(FAKE_TAOS)
    add_executable(taos_benchmark
        taos_benchmark.c
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:os_port_obj>
        $<TARGET_OBJECTS:taos_helpers_obj>)
else()
    add_executable(taos_benchmark
        taos_benchmark.c)
    target_link_libraries(taos_benchmark ${TAOS_LIB_NAME})
endif()

add_executable(odbc_benchmark
    odbc_benchmark.c)
if(NOT TODBC_WINDOWS)
    target_link_libraries(odbc_benchmark odbc)
else()
    target_link_libraries(odbc_benchmark odbc32)
endif()


if(FAKE_TAOS)
    add_executable(tb
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:os_port_obj>
        $<TARGET_OBJECTS:taos_helpers_obj>
        tb.c
    )
else()
    add_executable(tb
        $<TARGET_OBJECTS:common_obj>
        $<TARGET_OBJECTS:os_port_obj>
        tb.c
    )
    target_link_libraries(tb ${TAOS_LIB_NAME})
endif()

if(TODBC_WINDOWS)
    set_source_files_properties(tb.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
else()
    target_link_libraries(tb pthread)
endif()

add_executable(ob
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:os_port_obj>
    $<TARGET_OBJECTS:odbc_helpers_obj>
    ob.c
)
if(NOT TODBC_WINDOWS)
    target_link_libraries(ob odbc pthread)
else()
    target_link_libraries(ob odbc32)
    set_source_files_properties(ob.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
endif()

