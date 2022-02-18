aux_source_directory(src DAEMON_SRC)
add_executable(taosd ${DAEMON_SRC})
target_link_libraries(
    taosd
    PUBLIC dnode
    PUBLIC config
    PUBLIC util
    PUBLIC os
)
