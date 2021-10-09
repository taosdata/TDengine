# googletest
if(${BUILD_TEST})
    add_subdirectory(googletest)
endif(${BUILD_TEST})

# cJson
add_subdirectory(cJson)
target_include_directories(
    cjson
    # see https://stackoverflow.com/questions/25676277/cmake-target-include-directories-prints-an-error-when-i-try-to-add-the-source
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cJson>
)

# lz4
add_subdirectory(lz4/build/cmake)
target_include_directories(
    lz4_static
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lib
)

# zlib
add_subdirectory(zlib)
target_include_directories(
    zlib
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
)

# leveldb
if(${BUILD_WITH_LEVELDB})
option(LEVELDB_BUILD_TESTS "" OFF)
    add_subdirectory(leveldb)
    target_include_directories(
        leveldb
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/leveldb/include>
    )
endif(${BUILD_WITH_LEVELDB})

# rocksdb
if(${BUILD_WITH_ROCKSDB})
    option(WITH_TESTS "" OFF)
    option(WITH_BENCHMARK_TOOLS "" OFF)
    add_subdirectory(rocksdb)
    target_include_directories(
        rocksdb
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include>
    )
endif(${BUILD_WITH_ROCKSDB})
