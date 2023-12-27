aux_source_directory(src PARSER_SRC)

IF (TD_ENTERPRISE)
  LIST(APPEND PARSER_SRC ${TD_ENTERPRISE_DIR}/src/plugins/view/src/parserView.c)
ENDIF ()

add_library(parser STATIC ${PARSER_SRC})
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
