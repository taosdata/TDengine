add_executable(lz4_test "")
target_sources(lz4_test
    PRIVATE
    "main.c"
)
target_link_libraries(lz4_test lz4_static)