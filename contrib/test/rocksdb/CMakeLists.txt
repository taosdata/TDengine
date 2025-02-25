message("contrib test/rocksdb:" ${BUILD_DEPENDENCY_TESTS})

add_executable(rocksdbTest "")
target_sources(rocksdbTest
    PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/main.c"
)
target_link_libraries(rocksdbTest rocksdb)

# Simple test for quick verification of RocksDB functionality
add_executable(simple_rocksdb_test "")
target_sources(simple_rocksdb_test
    PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/simple_test.c"
)
target_link_libraries(simple_rocksdb_test rocksdb)
