# ================================================================================================
# DEPENDENCIES
# ================================================================================================
# googletest
if(${BUILD_TEST})
    add_subdirectory(googletest)
endif(${BUILD_TEST})

# cJson
# see https://stackoverflow.com/questions/37582508/silence-cmp0048-warnings-in-vendored-projects
set(CMAKE_PROJECT_INCLUDE_BEFORE "${CMAKE_SUPPORT_DIR}/EnableCMP0048.txt.in")
option(ENABLE_CJSON_TEST "Enable building cJSON test" OFF)
option(CJSON_OVERRIDE_BUILD_SHARED_LIBS "Override BUILD_SHARED_LIBS with CJSON_BUILD_SHARED_LIBS" ON)
option(CJSON_BUILD_SHARED_LIBS "Overrides BUILD_SHARED_LIBS if CJSON_OVERRIDE_BUILD_SHARED_LIBS is enabled" OFF)
add_subdirectory(cJson)
target_include_directories(
    cjson
    # see https://stackoverflow.com/questions/25676277/cmake-target-include-directories-prints-an-error-when-i-try-to-add-the-source
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cJson>
)
unset(CMAKE_PROJECT_INCLUDE_BEFORE)

# lz4
add_subdirectory(lz4/build/cmake)
target_include_directories(
    lz4_static
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lib
)

# zlib
set(CMAKE_PROJECT_INCLUDE_BEFORE "${CMAKE_SUPPORT_DIR}/EnableCMP0048.txt.in")
add_subdirectory(zlib)
target_include_directories(
    zlib
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
)
unset(CMAKE_PROJECT_INCLUDE_BEFORE)

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
# To support rocksdb build on ubuntu: sudo apt-get install libgflags-dev
if(${BUILD_WITH_ROCKSDB})
    option(WITH_TESTS "" OFF)
    option(WITH_BENCHMARK_TOOLS "" OFF)
    option(ROCKSDB_BUILD_SHARED "Build shared versions of the RocksDB libraries" OFF)
    add_subdirectory(rocksdb)
    target_include_directories(
        rocksdb
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include>
    )
endif(${BUILD_WITH_ROCKSDB})

# lucene
# To support build on ubuntu: sudo apt-get install libboost-all-dev
if(${BUILD_WITH_LUCENE})
    option(ENABLE_TEST "Enable the tests" OFF)
    add_subdirectory(lucene)
    target_include_directories(
      lucene++ 
      PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/lucene/include>
    ) 
     
endif(${BUILD_WITH_LUCENE})

# ================================================================================================
# DEPENDENCY TEST
# ================================================================================================
if(${BUILD_DEPENDENCY_TESTS})
    add_subdirectory(test)
endif(${BUILD_DEPENDENCY_TESTS})
