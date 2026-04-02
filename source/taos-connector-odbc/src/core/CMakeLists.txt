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

list(APPEND core_SOURCES charset.c)
list(APPEND core_SOURCES columns.c)
list(APPEND core_SOURCES conn.c)
list(APPEND core_SOURCES desc.c)
list(APPEND core_SOURCES ds.c)
list(APPEND core_SOURCES env.c)
list(APPEND core_SOURCES errs.c)
list(APPEND core_SOURCES primarykeys.c)
list(APPEND core_SOURCES stmt.c)
list(APPEND core_SOURCES tables.c)
list(APPEND core_SOURCES tls.c)
list(APPEND core_SOURCES topic.c)
list(APPEND core_SOURCES tsdb.c)
list(APPEND core_SOURCES typesinfo.c)
list(APPEND core_SOURCES var.c)

if(TODBC_WINDOWS)
  configure_file("taos_odbc_ver_en.rc.in" "${CMAKE_CURRENT_BINARY_DIR}/taos_odbc_ver_en.rc")
  list(APPEND core_SOURCES setup.c)
  if(TODBC_X86)
    list(APPEND core_SOURCES taos_odbc_setup_en_x86.rc)
    list(APPEND core_SOURCES taos_odbc_setup_zh_x86.rc)
  else()
    list(APPEND core_SOURCES taos_odbc_setup_en_x64.rc)
    list(APPEND core_SOURCES taos_odbc_setup_zh_x64.rc)
  endif()
  list(APPEND core_SOURCES taos_odbc_ver_en.rc)
  # list(APPEND core_SOURCES taos_odbc_ver_zh.rc)
endif()

add_library(core_obj OBJECT ${core_SOURCES})
if(TODBC_WINDOWS)
    if(USE_WIN_ICONV)
        add_dependencies(core_obj ex_iconv)
    endif()
endif()
target_include_directories(core_obj PRIVATE
    ${INTERNAL_INC_PATH}
    ${ODBC_INCLUDE_DIRECTORY}
    ${ODBCINST_INCLUDE_DIRECTORY})
set_property(TARGET core_obj PROPERTY POSITION_INDEPENDENT_CODE 1)
target_include_directories(core_obj PRIVATE
    ${CMAKE_CURRENT_BINARY_DIR}/../..
    ${CMAKE_CURRENT_SOURCE_DIR}    
    )
