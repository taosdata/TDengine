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

aux_source_directory(src CATALOG_SRC)
add_library(catalog STATIC ${CATALOG_SRC})
target_include_directories(
    catalog
    PUBLIC "${TD_SOURCE_DIR}/include/libs/catalog"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(BUILD_TEST)
    target_compile_definitions(catalog PRIVATE BUILD_TEST)
endif()

target_link_libraries(
    catalog
    PRIVATE os util transport qcom nodes extconnector
)

if(UNIX AND NOT APPLE)
    # catalog participates in static-library cycles through parser/planner/executor.
    # Ask CMake to rescan the link interface enough times for GNU ld.
    set_property(TARGET catalog PROPERTY LINK_INTERFACE_MULTIPLICITY 3)
endif()

if(TD_ENTERPRISE AND UNIX AND NOT APPLE)
    # catalog's static link closure pulls in enterprise extconnector, which
    # references nodesRemotePlanToSQL from libnodes.a after libnodes.a may
    # already have been scanned. Force extraction for catalog consumers.
    target_link_options(catalog INTERFACE "LINKER:--undefined=nodesRemotePlanToSQL")
endif()

if(BUILD_TEST AND NOT TD_WINDOWS)
 target_compile_definitions(catalog PRIVATE BUILD_TEST)
 ADD_SUBDIRECTORY(test)
endif()
