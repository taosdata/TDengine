message("contrib test/rocksdb:" ${BUILD_DEPENDENCY_TESTS})

add_executable(rocksdbTest "")
target_sources(rocksdbTest
    PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/main.c"
)
target_link_libraries(rocksdbTest rocksdb)
