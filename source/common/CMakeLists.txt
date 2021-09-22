aux_source_directory(src COMMON_SRC)
add_library(common ${COMMON_SRC})
target_include_directories(
    common
    PUBLIC "${CMAKE_SOURCE_DIR}/include/common"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)