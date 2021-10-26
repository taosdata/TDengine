aux_source_directory(src CLIENT_SRC)
add_library(taos ${CLIENT_SRC})
target_include_directories(
    taos
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    taos
    INTERFACE api
)
