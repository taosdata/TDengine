aux_source_directory(src TSIM_SRC)
add_executable(tsim ${TSIM_SRC})
target_link_libraries(
    tsim
    PUBLIC ${TAOS_LIB}
    PUBLIC util
    PUBLIC common
    PUBLIC os
    PUBLIC cjson
)
target_include_directories(
    tsim
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
