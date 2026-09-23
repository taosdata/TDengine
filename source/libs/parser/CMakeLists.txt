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

aux_source_directory(src PARSER_SRC)
set(PARSER_SRC
  src/parAstCreater.c
  src/parAstParser.c
  src/parAuthenticator.c
  src/parCalcConst.c
  src/parInsertSml.c
  src/parInsertSql.c
  src/parInsertStmt.c
  src/parInsertUtil.c
  src/parser.c
  src/parTokenizer.c
  src/parTranslater.c
  src/parUtil.c
  src/taos_lemon_sql.tab.c
)

IF(TD_ENTERPRISE)
        LIST(APPEND PARSER_SRC ${TD_ENTERPRISE_DIR}/source/plugins/view/src/parserView.c)
        LIST(APPEND PARSER_SRC ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/parserExtSource.c)
ENDIF()

message(STATUS "Debugging ............................")
message(STATUS "CMAKE_CURRENT_BINARY_DIR:${CMAKE_CURRENT_BINARY_DIR}")

set(_depends
    ${CMAKE_CURRENT_BINARY_DIR}/taos_lemon_sql.c
    ${CMAKE_CURRENT_BINARY_DIR}/taos_lemon_token.h
)
add_custom_command(
    OUTPUT ${_depends}
    COMMAND $<TARGET_FILE:lemon>
        -T$<TARGET_PROPERTY:lemon,SOURCE_DIR>/lempar.c
        -d${CMAKE_CURRENT_BINARY_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/inc/sql.y
    COMMAND "${CMAKE_COMMAND}" -E rename ${CMAKE_CURRENT_BINARY_DIR}/sql.c ${CMAKE_CURRENT_BINARY_DIR}/taos_lemon_sql.c
    COMMAND "${CMAKE_COMMAND}" -E rename ${CMAKE_CURRENT_BINARY_DIR}/sql.h ${CMAKE_CURRENT_BINARY_DIR}/taos_lemon_token.h
    COMMAND "${CMAKE_COMMAND}" -E echo "generated files are in ${CMAKE_CURRENT_BINARY_DIR}/{taos_lemon_sql.c,taos_lemon_token.h}"
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/inc/sql.y
            lemon
            $<TARGET_PROPERTY:lemon,SOURCE_DIR>/lempar.c
    COMMENT "Generating sql.c using lemon"
)

add_custom_target(lemon_sql
    DEPENDS ${_depends}
)

add_library(parser STATIC ${PARSER_SRC})
add_dependencies(parser lemon_sql)
set_property(TARGET parser PROPERTY LINK_INTERFACE_MULTIPLICITY 3)
target_include_directories(
        parser
        PUBLIC "${TD_SOURCE_DIR}/include/libs/parser"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
        PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
)

target_link_libraries(
        parser
        PRIVATE os util nodes catalog function scalar planner geometry transport qcom decimal
)

# parser belongs to a cyclic static-archive group (parser/scalar/function/
# catalog/extconnector/nodes/planner/index/geometry). CMake repeats the
# whole group LINK_INTERFACE_MULTIPLICITY times on the link line; the
# default of 2 is not enough for some consumers (clientMonitorTest needs
# parseJsontoTagData from a libparser.a occurrence after the last
# libscalar.a; bseTest and mnode tests need extConnector* after the last
# libcatalog.a). CMake takes the max of this property across the cycle's
# members, so setting it here covers every consumer of the group.
set_property(TARGET parser PROPERTY LINK_INTERFACE_MULTIPLICITY 3)

if(BUILD_TEST)
        ADD_SUBDIRECTORY(test)
endif()
