# lemon
add_subdirectory(lemon)
add_subdirectory(TSZ)
add_subdirectory(libaes)
add_subdirectory(libmqtt)

# leveldb
if(BUILD_WITH_LEVELDB)
    option(LEVELDB_BUILD_TESTS "" OFF)
    add_subdirectory(leveldb EXCLUDE_FROM_ALL)
    target_include_directories(
        leveldb
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/leveldb/include>
    )
endif()

MESSAGE(STATUS "build with dependency tests: ${BUILD_DEPENDENCY_TESTS}")
if(BUILD_DEPENDENCY_TESTS)
    add_subdirectory(test EXCLUDE_FROM_ALL)
endif()

