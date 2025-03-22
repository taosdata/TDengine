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

# Linux kernel source indent format options
set(INDENT_OPTS -nbad -bap -nbc -bbo -hnl -br -brs -c33 -cd33 -ncdb -ce -ci4
    -cli0 -d0 -di1 -nfc1 -i8 -ip0 -l80 -lp -npcs -nprs -npsl -sai
    -saf -saw -ncs -nsc -sob -nfca -cp33 -ss -ts8)

foreach($dir src tests examples)
	exec_program(indent
                 ARGS ${INDENT_OPTS} ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/*.[c,h]
                 OUTPUT_VARIABLE indent_output
                 RETURN_VALUE ret)
    message(STATUS ${indent_output})
	# TODO: mv ${CMAKE_CURRENT_SOURCE_DIR}/${dir}/*~ /tmp; \
endforeach()

