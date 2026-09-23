# Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
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

add_subdirectory(tdb)
add_subdirectory(transport)
add_subdirectory(wal)
add_subdirectory(monitor)
add_subdirectory(tfs)
add_subdirectory(sync)
add_subdirectory(qcom)
add_subdirectory(nodes)
add_subdirectory(extconnector)
add_subdirectory(catalog)
add_subdirectory(audit)
add_subdirectory(monitorfw)
add_subdirectory(sm4)
add_subdirectory(crypt)
add_subdirectory(metrics)

add_subdirectory(scalar)
add_subdirectory(function)
add_subdirectory(index)
add_subdirectory(parser)
add_subdirectory(scheduler)
add_subdirectory(executor)
add_subdirectory(new-stream)
add_subdirectory(planner)
add_subdirectory(qworker)
add_subdirectory(geometry)
add_subdirectory(command)
add_subdirectory(tmqtt)
add_subdirectory(tss)
add_subdirectory(decimal)
add_subdirectory(txnode)
add_subdirectory(totp)

if(BUILD_PYUDF)
    add_subdirectory(pyudf)
endif()
