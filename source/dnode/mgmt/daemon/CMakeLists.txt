aux_source_directory(src DAEMON_SRC)
add_executable(taosd ${DAEMON_SRC})

target_include_directories(
    taosd
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(taosd dnode)
