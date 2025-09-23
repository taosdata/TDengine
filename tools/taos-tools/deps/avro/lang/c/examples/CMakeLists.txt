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

add_executable(quickstop quickstop.c)
target_link_libraries(quickstop avro-static)

if (WIN32)
    set(exec_name ${CMAKE_CURRENT_BINARY_DIR}/Debug/quickstop.exe)
else (WIN32)
    set(exec_name ${CMAKE_CURRENT_BINARY_DIR}/quickstop)
endif (WIN32)

add_test(quickstop
    ${CMAKE_COMMAND} -E chdir ${AvroC_SOURCE_DIR}/examples
    ${exec_name}
)
