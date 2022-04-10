aux_source_directory(src CACHE_SRC)
add_library(cache STATIC ${CACHE_SRC})
target_include_directories(
    cache
    PUBLIC "${TD_SOURCE_DIR}/include/libs/cache"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)