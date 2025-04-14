# tdbTest
if(${TD_LINUX})
    add_executable(tdbTest "tdbTest.cpp")
    target_link_libraries(tdbTest tdb gtest_main)
endif()

# tdbUtilTest
add_executable(tdbUtilTest "tdbUtilTest.cpp")
target_link_libraries(tdbUtilTest tdb gtest_main)
add_test(
    NAME tdbUtilTest
    COMMAND tdbUtilTest
)

# overflow pages testing
add_executable(tdbExOVFLTest "tdbExOVFLTest.cpp")
target_link_libraries(tdbExOVFLTest tdb gtest_main)

# page defragment testing
add_executable(tdbPageDefragmentTest "tdbPageDefragmentTest.cpp")
target_link_libraries(tdbPageDefragmentTest tdb gtest_main)

# page recycling testing
add_executable(tdbPageRecycleTest "tdbPageRecycleTest.cpp")
target_link_libraries(tdbPageRecycleTest tdb gtest_main)

# page flush testing
add_executable(tdbPageFlushTest "tdbPageFlushTest.cpp")
target_link_libraries(tdbPageFlushTest tdb gtest_main)
add_test(
    NAME tdbPageFlushTest
    COMMAND tdbPageFlushTest
)
