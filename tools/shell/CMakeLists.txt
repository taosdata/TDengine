aux_source_directory(src SHELL_SRC)
list(REMOVE_ITEM SHELL_SRC src/shellWindows.c)
list(REMOVE_ITEM SHELL_SRC src/shellDarwin.c)

add_executable(shell ${SHELL_SRC})
target_link_libraries(
    shell
    PUBLIC taos
    PUBLIC util
    PUBLIC common
    PUBLIC os
)
target_include_directories(
    shell
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

SET_TARGET_PROPERTIES(shell PROPERTIES OUTPUT_NAME taos)
