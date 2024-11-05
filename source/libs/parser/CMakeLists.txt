aux_source_directory(src PARSER_SRC)

IF(TD_ENTERPRISE)
        LIST(APPEND PARSER_SRC ${TD_ENTERPRISE_DIR}/src/plugins/view/src/parserView.c)
ENDIF()

add_custom_command(
    OUTPUT ${TD_SOURCE_DIR}/source/libs/parser/src/sql.c
    COMMAND echo "Running lemon process in ${TD_SOURCE_DIR}/source/libs/parser/inc"
    COMMAND ${TD_CONTRIB_DIR}/deps-download/lemon-prefix/src/lemon/tool/lemon sql.y || true
    COMMAND echo "copy sql.c from ${TD_SOURCE_DIR}/source/libs/parser/inc/sql.c to ${TD_SOURCE_DIR}/source/libs/parser/src/"
    COMMAND cp ${TD_SOURCE_DIR}/source/libs/parser/inc/sql.c ${TD_SOURCE_DIR}/source/libs/parser/src/
    COMMAND echo "lemon process completed."
    DEPENDS ${TD_SOURCE_DIR}/source/libs/parser/inc/sql.y
    WORKING_DIRECTORY ${TD_SOURCE_DIR}/source/libs/parser/inc
    COMMENT "Generating sql.c using lemon"
)

add_custom_target(lemon_target
    DEPENDS ${TD_SOURCE_DIR}/source/libs/parser/src/sql.c
)

list(APPEND PARSER_SRC ${TD_SOURCE_DIR}/source/libs/parser/src/sql.c)

add_library(parser STATIC ${PARSER_SRC})
add_dependencies(parser lemon_target)
target_include_directories(
        parser
        PUBLIC "${TD_SOURCE_DIR}/include/libs/parser"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        parser
        PRIVATE os util nodes catalog function scalar geometry transport qcom
)

if(${BUILD_TEST})
        ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
