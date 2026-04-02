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

add_executable(taos_test
    taos_test.c
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:test_helper_obj>
    $<TARGET_OBJECTS:ejson_parser_obj>
    $<TARGET_OBJECTS:os_port_obj>
    $<TARGET_OBJECTS:taos_helpers_obj>)
add_dependencies(taos_test ex_cjson)

target_include_directories(taos_test PRIVATE ${CJSON_INSTALL_PATH}/include ${CMAKE_CURRENT_BINARY_DIR}/..)
target_link_directories(taos_test PRIVATE ${CJSON_INSTALL_PATH}/lib)
# NOTE: cJSON_xxx exported in `libtaos.so`
if(TODBC_WINDOWS)
  target_link_libraries(taos_test cjson)
  # NOTE: because 'utf8' is found in inc/ejson_parser.h
  set_source_files_properties(taos_test.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
else()
  target_link_libraries(taos_test cjson dl pthread)
endif()
if(NOT FAKE_TAOS)
  target_link_libraries(taos_test ${TAOS_LIB_NAME})
endif()

if(NOT FAKE_TAOS)
  add_test(NAME taos_test
        COMMAND "${CMAKE_COMMAND}" -E env
            TAOS_TEST_CASES=${CMAKE_CURRENT_SOURCE_DIR}/taos_test.cases
            $<TARGET_FILE:taos_test>)
endif()

if(ENABLE_VALGRIND)
  add_test(NAME Vtaos_test
        COMMAND "${CMAKE_COMMAND}" -E env
            TAOS_TEST_CASES=${CMAKE_CURRENT_SOURCE_DIR}/taos_test.cases
            ${CMAKE_SOURCE_DIR}/sh/valgrind.sh $<TARGET_FILE:taos_test>)
endif()

add_executable(taos_regression
    taos_regression.c
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:test_helper_obj>
    $<TARGET_OBJECTS:ejson_parser_obj>
    $<TARGET_OBJECTS:os_port_obj>
    $<TARGET_OBJECTS:taos_helpers_obj>)
add_dependencies(taos_regression ex_cjson)

target_include_directories(taos_regression PRIVATE ${CJSON_INSTALL_PATH}/include)
target_link_directories(taos_regression PRIVATE ${CJSON_INSTALL_PATH}/lib)
# NOTE: cJSON_xxx exported in `libtaos.so`
if(TODBC_WINDOWS)
  set_source_files_properties(taos_regression.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
  target_link_libraries(taos_regression cjson)
else()
  target_link_libraries(taos_regression cjson dl pthread)
endif()
if(NOT FAKE_TAOS)
  target_link_libraries(taos_regression ${TAOS_LIB_NAME})
endif()

if(NOT FAKE_TAOS)
  if(DEFINED SERVER_FOR_TEST)
    add_test(NAME taos_regression
        COMMAND taos_regression --ip "${HOST_FOR_TEST}" --port "${PORT_FOR_TEST}")
  else()
    add_test(NAME taos_regression
        COMMAND taos_regression)
  endif()
endif()

if(ENABLE_VALGRIND)
  add_test(NAME Vtaos_regression
      COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/taos_regression)
endif()

if(HAVE_TAOSWS)
  add_executable(taosws_regression
      taosws_regression.c
      $<TARGET_OBJECTS:common_obj>
      $<TARGET_OBJECTS:test_helper_obj>
      $<TARGET_OBJECTS:ejson_parser_obj>
      $<TARGET_OBJECTS:os_port_obj>)
  add_dependencies(taosws_regression ex_cjson)

  target_include_directories(taosws_regression PRIVATE ${CJSON_INSTALL_PATH}/include ${CMAKE_CURRENT_BINARY_DIR}/..)
  target_link_directories(taosws_regression PRIVATE ${CJSON_INSTALL_PATH}/lib)
  # NOTE: cJSON_xxx exported in `libtaos.so`
  if(TODBC_WINDOWS)
    set_source_files_properties(taosws_regression.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
    target_link_libraries(taosws_regression cjson)
  else()
    target_link_libraries(taosws_regression cjson dl pthread)
  endif()

  if(NOT FAKE_TAOSWS)
    target_link_libraries(taosws_regression ${TAOSWS_LIB_NAME})
  endif()

  add_test(NAME taosws_regression
      COMMAND taosws_regression)

  if(ENABLE_VALGRIND)
    add_test(NAME Vtaosws_regression
        COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/taosws_regression)
  endif()
endif()

