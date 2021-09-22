aux_source_directory(src CLIENT_SRC)
add_library(taos ${CLIENT_SRC})
target_include_directories(
    taos
    PUBLIC "${CMAKE_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)