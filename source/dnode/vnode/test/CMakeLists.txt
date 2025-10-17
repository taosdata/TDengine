MESSAGE(STATUS "tq unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)

IF(NOT TD_WINDOWS)
    add_executable(tqTest tqTest.cpp)
    DEP_ext_gtest(tqTest)
    target_include_directories(tqTest
            PUBLIC
            "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )

    TARGET_LINK_LIBRARIES(
            tqTest
            PUBLIC os util common vnode
    )

    enable_testing()

    add_test(
            NAME tq_test
            COMMAND tqTest
    )
ENDIF()


ADD_EXECUTABLE(bseTest bseTest.cpp)
DEP_ext_gtest(bseTest)
TARGET_LINK_LIBRARIES(
         bseTest
         PUBLIC os util common vnode
)

TARGET_INCLUDE_DIRECTORIES(
         bseTest
         PUBLIC "${TD_SOURCE_DIR}/include/common"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

ADD_EXECUTABLE(tsdbCacheTest tsdbCacheTest.cpp)
DEP_ext_gtest(tsdbCacheTest)
TARGET_LINK_LIBRARIES(
         tsdbCacheTest
         PUBLIC os util common vnode
)

TARGET_INCLUDE_DIRECTORIES(
         tsdbCacheTest
         PUBLIC "${TD_SOURCE_DIR}/include/common"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/tsdb"
)

TARGET_COMPILE_OPTIONS(tsdbCacheTest PRIVATE -fpermissive -Wno-error)

enable_testing()

add_test(
         NAME tsdb_cache_test
         COMMAND tsdbCacheTest
)
