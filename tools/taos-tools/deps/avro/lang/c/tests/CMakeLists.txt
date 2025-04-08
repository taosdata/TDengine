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

macro(add_avro_executable name)
    set(source "${ARGV1}")
    if (NOT source)
        set(source "${name}.c")
    endif (NOT source)
    add_executable(${name} ${source})
    target_link_libraries(${name} avro-static)
endmacro(add_avro_executable)

macro(add_avro_test name)
    add_avro_executable(${name} ${ARGN})
    if (WIN32)
        set(exec_name ${CMAKE_CURRENT_BINARY_DIR}/Debug/${name}.exe)
    else (WIN32)
        set(exec_name ${CMAKE_CURRENT_BINARY_DIR}/${name})
    endif (WIN32)

    add_test(${name}
        ${CMAKE_COMMAND} -E chdir ${AvroC_SOURCE_DIR}/tests
        ${exec_name}
    )
endmacro(add_avro_test)

macro(add_avro_test_checkmem name)
    add_avro_test(${name} ${ARGN})
    if(UNIX)
        find_program(MEMORYCHECK_COMMAND valgrind )
        if(MEMORYCHECK_COMMAND)
            add_test(memcheck_${name}
                ${CMAKE_COMMAND} -E chdir ${AvroC_SOURCE_DIR}/tests
                ${MEMORYCHECK_COMMAND}
                    --log-file=${CMAKE_CURRENT_BINARY_DIR}/memcheck_${name}.log
                    --leak-check=full
                    --show-reachable=yes
                    --error-exitcode=1
                    ${exec_name}
            )
        endif(MEMORYCHECK_COMMAND)
    endif (UNIX)
endmacro(add_avro_test_checkmem)

add_avro_executable(generate_interop_data)
add_avro_executable(performance)
add_avro_executable(test_interop_data)

add_avro_test_checkmem(test_data_structures)
add_avro_test_checkmem(test_avro_schema)
add_avro_test_checkmem(test_avro_schema_names)
add_avro_test_checkmem(test_avro_values)
add_avro_test_checkmem(test_avro_766)
add_avro_test_checkmem(test_avro_968)
add_avro_test_checkmem(test_avro_984)
add_avro_test_checkmem(test_avro_1034)
add_avro_test_checkmem(test_avro_1084)
add_avro_test_checkmem(test_avro_1087)
add_avro_test_checkmem(test_avro_1165)
add_avro_test_checkmem(test_avro_1167)
add_avro_test_checkmem(test_avro_1237)
add_avro_test_checkmem(test_avro_1238)
add_avro_test_checkmem(test_avro_1279)
add_avro_test_checkmem(test_avro_1405)
add_avro_test_checkmem(test_avro_1572)
add_avro_test(test_avro_data) # Skip memory check for datum. Deprecated and has a lot of memory issues
add_avro_test_checkmem(test_refcount)
add_avro_test_checkmem(test_avro_1379)
add_avro_test_checkmem(test_avro_1691)
add_avro_test_checkmem(test_avro_1906)
add_avro_test_checkmem(test_avro_1904)
