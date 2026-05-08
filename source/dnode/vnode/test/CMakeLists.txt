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
            PUBLIC os util common
    )
    # Handle circular dependencies by listing libraries multiple times
    # This works on all platforms (Linux, macOS, Windows)
    target_link_libraries(tqTest PUBLIC vnode mgmt_mnode vnode mgmt_mnode)

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

IF(NOT TD_WINDOWS)
    ADD_EXECUTABLE(tsdbCacheTest tsdbCacheTest.cpp)
    DEP_ext_gtest(tsdbCacheTest)
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        target_compile_options(tsdbCacheTest PRIVATE -fpermissive -Wno-error)
    endif()
    TARGET_LINK_LIBRARIES(
             tsdbCacheTest
             PUBLIC os util common vnode
    )

    TARGET_INCLUDE_DIRECTORIES(
             tsdbCacheTest
             PUBLIC "${TD_SOURCE_DIR}/include/common"
             PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
             PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/tsdb"
             PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    )
ENDIF()

# ADD_EXECUTABLE(tsdbRepairTest tsdbRepairTest.cpp)
# DEP_ext_gtest(tsdbRepairTest)
# TARGET_LINK_LIBRARIES(
#          tsdbRepairTest
#          PUBLIC os util common vnode
# )

# TARGET_INCLUDE_DIRECTORIES(
#          tsdbRepairTest
#          PUBLIC "${TD_SOURCE_DIR}/include/common"
#          PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
#          PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/tsdb"
#          PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )

IF(NOT TD_WINDOWS)
    # vtable-ref-vtable resolution logic test
    ADD_EXECUTABLE(vtableResolveTest vtableResolveTest.cpp)
    DEP_ext_gtest(vtableResolveTest)
    TARGET_LINK_LIBRARIES(vtableResolveTest PUBLIC os util common)
    TARGET_INCLUDE_DIRECTORIES(
        vtableResolveTest
        PUBLIC "${TD_SOURCE_DIR}/include/common"
        PUBLIC "${TD_SOURCE_DIR}/include/util"
    )
    add_test(
        NAME vtableResolveTest
        COMMAND vtableResolveTest
    )
ENDIF()
