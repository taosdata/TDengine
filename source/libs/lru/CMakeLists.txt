aux_source_directory(src LRU_SRC)
add_library(lru ${LRU_SRC})
target_include_directories(
    lru
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/lru"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)