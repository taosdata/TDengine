#!/bin/bash

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


_path_to_this_file=$( { pushd $(dirname "$0") >/dev/null; pwd; popd >/dev/null; } )
_path_to_valgrind=${_path_to_this_file}/../valgrind


valgrind --leak-check=full                                                \
         --show-leak-kinds=definite                                       \
         --show-reachable=no                                              \
         --num-callers=500                                                \
         --exit-on-first-error=yes                                        \
         --error-exitcode=1                                               \
         --suppressions=${_path_to_valgrind}/taos.supp                    \
         --suppressions=${_path_to_valgrind}/node.supp                    \
         --suppressions=${_path_to_valgrind}/mysql.supp                   \
         --suppressions=${_path_to_valgrind}/sqlite3.supp                 \
         --suppressions=${_path_to_valgrind}/valgrind.supp                \
         --gen-suppressions=all                                           \
         --track-origins=yes                                              \
         --errors-for-leak-kinds=definite                                 \
         "$@"

