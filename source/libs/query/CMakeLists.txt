aux_source_directory(src QUERY_SRC)
add_library(query ${QUERY_SRC})
target_include_directories(
    query
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/query"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
   query
   PRIVATE os util common transport
)
