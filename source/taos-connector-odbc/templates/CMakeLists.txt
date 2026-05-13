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

if(TODBC_LINUX)
  set(libtaos_odbc "/usr/local/lib/libtaos_odbc.so")
endif()

if(TODBC_DARWIN)
  set(libtaos_odbc "/usr/local/lib/libtaos_odbc.dylib")
endif()

if(NOT TODBC_WINDOWS)
  configure_file(odbc.ini.in ${CMAKE_CURRENT_BINARY_DIR}/odbc.ini)
  configure_file(odbcinst.ini.in ${CMAKE_CURRENT_BINARY_DIR}/odbcinst.ini)

  add_custom_target(install_templates
      COMMAND odbcinst -u -s -h -n TAOS_ODBC_DSN
      COMMAND cmake -E echo TAOS_ODBC_DSN is uninstalled from ~/.odbc.ini

      COMMAND sudo odbcinst -u -s -l -n TAOS_ODBC_DSN
      COMMAND cmake -E echo TAOS_ODBC_DSN is uninstalled from /etc/odbc.ini

      COMMAND sudo odbcinst -u -d -l -n TDengine
      COMMAND cmake -E echo TDengine is uninstalled from /etc/odbcinst.ini

      COMMAND sudo odbcinst -i -d -f ${CMAKE_CURRENT_BINARY_DIR}/odbcinst.ini -l
      COMMAND cmake -E echo TDengine is installed in /etc/odbcinst.ini

      COMMAND sudo odbcinst -i -s -f ${CMAKE_CURRENT_BINARY_DIR}/odbc.ini -l
      COMMAND cmake -E echo TAOS_ODBC_DSN is installed in /etc/odbc.ini

      COMMAND odbcinst -i -s -f ${CMAKE_CURRENT_BINARY_DIR}/odbc.ini -h
      COMMAND cmake -E echo TAOS_ODBC_DSN is installed in ~/.odbc.ini)
else()
  if(TODBC_X86)
    set(DRIVER_PATH "C:/TDengine/taos_odbc/x86/bin/taos_odbc.dll")
  else()
    set(DRIVER_PATH "C:/TDengine/taos_odbc/x64/bin/taos_odbc.dll")
  endif()
  string(REPLACE "/" "\\" DRIVER_PATH "${DRIVER_PATH}")
  message("DRIVER_PATH: ${DRIVER_PATH}")
  configure_file(win_odbcinst.ini.in ${CMAKE_CURRENT_BINARY_DIR}/win_odbcinst.ini @ONLY)
  configure_file(win_odbc_install.ini.in ${CMAKE_CURRENT_BINARY_DIR}/win_odbc_install.ini @ONLY)

  set(WINDOWS_SYSTEM_PATH $ENV{SystemRoot})
  message(STATUS "Windows System Path: ${WINDOWS_SYSTEM_PATH}")
  if(TODBC_X86)
    set(ODBCCONF_PATH "${WINDOWS_SYSTEM_PATH}/SysWOW64/odbcconf.exe")
  else()
    set(ODBCCONF_PATH "${WINDOWS_SYSTEM_PATH}/System32/odbcconf.exe")
  endif()
  string(REPLACE "/" "\\" ODBCCONF_PATH "${ODBCCONF_PATH}")
  message("ODBCCONF_PATH: ${ODBCCONF_PATH}")
  message("CMAKE_CURRENT_BINARY_DIR: ${CMAKE_CURRENT_BINARY_DIR}")

  add_custom_target(install_templates
    COMMAND ${ODBCCONF_PATH} /S /F ${CMAKE_CURRENT_BINARY_DIR}/win_odbcinst.ini
    COMMAND ${CMAKE_COMMAND} -E echo "-=Done=-"
  )
endif()