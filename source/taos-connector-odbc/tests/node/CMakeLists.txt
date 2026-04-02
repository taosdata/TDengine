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

if(HAVE_NODEJS)
  configure_file(
      "${CMAKE_CURRENT_SOURCE_DIR}/node_test.js"
      "${CMAKE_CURRENT_BINARY_DIR}/node_test.js"
      @ONLY)

  add_test(NAME node_test
      COMMAND ${NODEJS_NAME} ${CMAKE_CURRENT_BINARY_DIR}/node_test.js)

  if(ENABLE_VALGRIND)
    add_test(NAME Vnode_test
        COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh node ${CMAKE_CURRENT_BINARY_DIR}/node_test.js)
  endif()
endif()

