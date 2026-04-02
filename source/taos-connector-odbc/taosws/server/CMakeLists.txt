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

add_executable(taoswsd
    taoswsd.c)

add_dependencies(taoswsd ex_libwebsockets)

target_include_directories(taoswsd PRIVATE
    ${CJSON_INSTALL_PATH}/include
    ${LIBWEBSOCKETS_INSTALL_PATH}/include)
target_link_directories(taoswsd PRIVATE
    ${CJSON_INSTALL_PATH}/lib
    ${LIBWEBSOCKETS_INSTALL_PATH}/lib)
target_link_libraries(taoswsd cjson libwebsockets.a ${TAOS_LIB_NAME} ssl crypto uuid)

set_target_properties(taoswsd PROPERTIES VERSION 0.1)
install(TARGETS taoswsd)

add_custom_target(install_taoswsd
    COMMAND sudo ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/taoswsd.service /etc/systemd/system/taoswsd.service)

