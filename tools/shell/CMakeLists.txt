aux_source_directory(src SHELL_SRC)

add_executable(shell ${SHELL_SRC})
target_link_libraries(
    shell
    PUBLIC taos
    PRIVATE os common transport util
)
target_include_directories(
    shell
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(shell PROPERTIES OUTPUT_NAME taos)
