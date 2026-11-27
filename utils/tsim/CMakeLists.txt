# Copyright (c) 2022 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

LIST(APPEND TSIM_SRC src/simEntry.c)
LIST(APPEND TSIM_SRC src/simExec.c)
LIST(APPEND TSIM_SRC src/simParse.c)
LIST(APPEND TSIM_SRC src/simSystem.c)

ADD_LIBRARY(tsim_static STATIC ${TSIM_SRC})
TARGET_INCLUDE_DIRECTORIES(
    tsim_static
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
# The simulator calls the public C API (taos_connect, taos_query, taos_errno,
# ...) as well as a few engine internals.  The API lives in libtaos.so, which
# dispatches to the driver it dlopen()s; on Linux the driver no longer exports
# those names, so naming it alone leaves the API undefined.  tsim_static is an
# archive, so the executable is what resolves them -- keep both here.
TARGET_LINK_LIBRARIES(
    tsim_static
    PUBLIC ${TAOS_LIB}
    PUBLIC ${TAOS_NATIVE_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
)

LIST(APPEND TSIM_EXE_SRC src/simMain.c)
ADD_EXECUTABLE(tsim ${TSIM_EXE_SRC})
TARGET_LINK_LIBRARIES(
    tsim
    PUBLIC tsim_static
)

IF(BUILD_TEST)
    ADD_SUBDIRECTORY(test)
ENDIF()
