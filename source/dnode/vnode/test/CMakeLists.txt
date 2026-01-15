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
    # Use linker groups for circular dependencies
    target_link_options(tqTest PRIVATE "-Wl,--start-group")
    target_link_libraries(tqTest PUBLIC vnode mgmt_mnode)
    target_link_options(tqTest PRIVATE "-Wl,--end-group")

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
