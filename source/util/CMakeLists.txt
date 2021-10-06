aux_source_directory(src UTIL_SRC)
add_library(util ${UTIL_SRC})
target_include_directories(
    util
    PUBLIC "${CMAKE_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    util
    PRIVATE os
    PUBLIC zlib
    PUBLIC lz4_static
)
