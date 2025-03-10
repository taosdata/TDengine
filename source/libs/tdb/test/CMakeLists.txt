# tdbTest
add_executable(tdbTest "tdbTest.cpp")
target_link_libraries(tdbTest tdb gtest_main)
add_test(
    NAME tdbTest
    COMMAND tdbTest
)

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
add_test(
    NAME tdbExOVFLTest
    COMMAND tdbExOVFLTest
)

# page defragment testing
add_executable(tdbPageDefragmentTest "tdbPageDefragmentTest.cpp")
target_link_libraries(tdbPageDefragmentTest tdb gtest_main)
add_test(
    NAME tdbPageDefragmentTest
    COMMAND tdbPageDefragmentTest
)

# page recycling testing
add_executable(tdbPageRecycleTest "tdbPageRecycleTest.cpp")
target_link_libraries(tdbPageRecycleTest tdb gtest_main)
add_test(
    NAME tdbPageRecycleTest
    COMMAND tdbPageRecycleTest
)

# page flush testing
add_executable(tdbPageFlushTest "tdbPageFlushTest.cpp")
target_link_libraries(tdbPageFlushTest tdb gtest_main)
add_test(
    NAME tdbPageFlushTest
    COMMAND tdbPageFlushTest
)
