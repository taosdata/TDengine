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

macro(add_sample name)
  add_executable(${name}
      $<TARGET_OBJECTS:common_obj>
      $<TARGET_OBJECTS:os_port_obj>
      $<TARGET_OBJECTS:odbc_helpers_obj>
      ${name}.c)
  add_dependencies(${name} ex_cjson)

  target_include_directories(${name} PRIVATE ${CJSON_INSTALL_PATH}/include)
  target_link_directories(${name} PRIVATE ${CJSON_INSTALL_PATH}/lib)
  if(NOT TODBC_WINDOWS)
      target_link_libraries(${name} odbc cjson pthread)
  else()
      set_source_files_properties(${name}.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
      target_link_libraries(${name} odbc32 cjson)
  endif()
endmacro()

add_sample(dump)
add_sample(demo_topic)
add_sample(bind_col)
add_sample(bind_param)
add_sample(desc_bind_col)

