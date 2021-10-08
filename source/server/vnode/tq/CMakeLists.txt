aux_source_directory(src TQ_SRC)
add_library(tq ${TQ_SRC})
target_include_directories(
    tq
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/vnode/tq"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${CMAKE_SOURCE_DIR}/include/os"
)

target_link_libraries(
    os
)
