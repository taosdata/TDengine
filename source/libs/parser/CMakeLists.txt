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
        LIST(APPEND PARSER_SRC ${TD_ENTERPRISE_DIR}/src/plugins/view/src/parserView.c)
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
target_include_directories(
        parser
        PUBLIC "${TD_SOURCE_DIR}/include/libs/parser"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        parser
        PRIVATE os util nodes catalog function scalar planner geometry transport qcom decimal
)

if(${BUILD_TEST})
        ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
