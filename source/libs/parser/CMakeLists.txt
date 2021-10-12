aux_source_directory(src PARSER_SRC)
add_library(parser ${PARSER_SRC})
target_include_directories(
        parser
        PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/parser"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        parser
        PRIVATE os util common catalog executor transport
)

ADD_SUBDIRECTORY(test)
