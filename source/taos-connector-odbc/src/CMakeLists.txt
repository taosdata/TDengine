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

set(INTERNAL_INC_PATH ${CMAKE_CURRENT_SOURCE_DIR}/inc)

add_subdirectory(core)
add_subdirectory(parser)
add_subdirectory(utils)
add_subdirectory(os_port)

list(APPEND odbc_SOURCES odbc.c)

add_library(odbc_obj OBJECT ${odbc_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
      add_dependencies(odbc_obj ex_iconv)
    endif()
endif()
target_include_directories(odbc_obj PRIVATE
    ${INTERNAL_INC_PATH}
    ${ODBC_INCLUDE_DIRECTORY}
    ${ODBCINST_INCLUDE_DIRECTORY})
set_property(TARGET odbc_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

# FIXME:
# set(CMAKE_REQUIRED_LINK_OPTIONS ${ODBC_LINK_OPTIONS} ${ODBCINST_LINK_OPTIONS})

list(APPEND taos_odbc_a_SOURCES
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:core_obj>
    $<TARGET_OBJECTS:odbc_obj>
    $<TARGET_OBJECTS:utils_obj>
    $<TARGET_OBJECTS:os_port_obj>
    $<TARGET_OBJECTS:conn_parser_obj>
    $<TARGET_OBJECTS:ext_parser_obj>
    $<TARGET_OBJECTS:sqls_parser_obj>
    $<TARGET_OBJECTS:url_parser_obj>
    $<TARGET_OBJECTS:ts_parser_obj>
    $<TARGET_OBJECTS:taos_helpers_obj>)

add_library(taos_odbc_a STATIC ${taos_odbc_a_SOURCES})
if(NOT FAKE_TAOS)
  target_link_libraries(taos_odbc_a ${TAOS_LIB_NAME})
endif()
if(NOT TODBC_WINDOWS)
    target_link_libraries(taos_odbc_a pthread)
    target_link_libraries(taos_odbc_a odbcinst)
    target_link_libraries(taos_odbc_a m)
else()
    target_link_libraries(taos_odbc_a legacy_stdio_definitions)
    target_link_libraries(taos_odbc_a odbccp32)
endif()
if(HAVE_TAOSWS)
  target_link_libraries(taos_odbc_a ${TAOSWS_LIB_NAME})
endif()

list(APPEND taos_odbc_SOURCES ${taos_odbc_a_SOURCES})
if(TODBC_WINDOWS)
  list(APPEND taos_odbc_SOURCES taos_odbc.def)
endif()
add_library(taos_odbc SHARED ${taos_odbc_SOURCES})
if(NOT FAKE_TAOS)
  target_link_libraries(taos_odbc ${TAOS_LIB_NAME})
endif()
if(NOT TODBC_WINDOWS)
    target_link_libraries(taos_odbc pthread)
    target_link_libraries(taos_odbc odbcinst)
    target_link_libraries(taos_odbc m)
else()
    target_link_libraries(taos_odbc legacy_stdio_definitions)
    target_link_libraries(taos_odbc odbccp32)
endif()
if(HAVE_TAOSWS)
  target_link_libraries(taos_odbc ${TAOSWS_LIB_NAME})
endif()
if(NOT TODBC_WINDOWS)
  set_source_files_properties(odbc.c PROPERTIES COMPILE_FLAGS -fvisibility=default)
endif()

set_target_properties(taos_odbc PROPERTIES VERSION 0.1)

if(TODBC_WINDOWS)
  if(TODBC_X86)
    set(INSTALL_PREFIX "C:/TDengine/taos_odbc/x86")
  else()
    set(INSTALL_PREFIX "C:/TDengine/taos_odbc/x64")
  endif()
  
  install(TARGETS taos_odbc  
          RUNTIME DESTINATION "${INSTALL_PREFIX}/bin"
          ARCHIVE DESTINATION "${INSTALL_PREFIX}/lib")
else()
  install(TARGETS taos_odbc)
endif()

add_subdirectory(tests)
