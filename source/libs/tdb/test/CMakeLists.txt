# tdbTest
add_executable(tdbTest "tdbTest.cpp")
target_link_libraries(tdbTest tdb gtest gtest_main)

# tdbUtilTest
add_executable(tdbUtilTest "tdbUtilTest.cpp")
target_link_libraries(tdbUtilTest tdb gtest gtest_main)

# overflow pages testing
add_executable(tdbExOVFLTest "tdbExOVFLTest.cpp")
target_link_libraries(tdbExOVFLTest tdb gtest gtest_main)

# page defragment testing
add_executable(tdbPageDefragmentTest "tdbPageDefragmentTest.cpp")
target_link_libraries(tdbPageDefragmentTest tdb gtest gtest_main)

# page recycling testing
add_executable(tdbPageRecycleTest "tdbPageRecycleTest.cpp")
target_link_libraries(tdbPageRecycleTest tdb gtest gtest_main)

