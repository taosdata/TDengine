aux_source_directory(src INDEX_SRC)
add_library(index ${INDEX_SRC})
target_include_directories(
    index
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/index"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)