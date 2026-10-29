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
    # catalog.a calls extLoadDialectDefaults. GNU ld scans libextconnector.a
    # before the last libcatalog.a in the cycle, so the defining object is
    # discarded unless the symbol is required from the start of the link.
    target_link_options(catalog INTERFACE "LINKER:--undefined=extLoadDialectDefaults")
endif()

if(TD_ENTERPRISE AND UNIX AND NOT APPLE)
    # catalog's static link closure pulls in enterprise extconnector, which
    # references nodesRemotePlanToSQL from libnodes.a after libnodes.a may
    # already have been scanned. Force extraction for catalog consumers.
    target_link_options(catalog INTERFACE "LINKER:--undefined=nodesRemotePlanToSQL")
    # catalog objects call extconnector after the last libextconnector.a is scanned.
    target_link_options(catalog INTERFACE
      "LINKER:--undefined=extLoadDialectDefaults"
      "LINKER:--undefined=extConnectorOpen"
      "LINKER:--undefined=extConnectorClose"
      "LINKER:--undefined=extConnectorGetTableSchema"
      "LINKER:--undefined=extConnectorFreeTableSchema"
      "LINKER:--undefined=extConnectorCloneTableSchema")
endif()

if(UNIX AND NOT APPLE)
    # catalog.c/ctgAsync.c/ctgCache.c reference extLoadDialectDefaults from
    # libextconnector.a, which precedes libcatalog.a in the static-library
    # cycle (extconnector -> nodes -> planner -> catalog). GNU ld may scan the
    # last libcatalog.a occurrence after the last libextconnector.a occurrence,
    # leaving the symbol unresolved. Force extraction for catalog consumers.
    target_link_options(catalog INTERFACE "LINKER:--undefined=extLoadDialectDefaults")
endif()

if(BUILD_TEST AND NOT TD_WINDOWS)
 target_compile_definitions(catalog PRIVATE BUILD_TEST)
 ADD_SUBDIRECTORY(test)
endif()
