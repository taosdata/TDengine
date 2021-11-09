aux_source_directory(src MNODE_SRC)
add_library(transaction ${MNODE_SRC})
target_include_directories(
    transaction
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/mnode/transaction"
    private "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    transaction
    PRIVATE os
    PRIVATE common
    PRIVATE util
    PRIVATE sdb
)
