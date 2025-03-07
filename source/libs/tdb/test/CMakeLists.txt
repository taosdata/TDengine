# test_tdb
add_executable(test_tdb "tdbTest.cpp")
target_link_libraries(test_tdb tdb gtest gtest_main)

# test_tdbutil
add_executable(test_tdbutil "tdbUtilTest.cpp")
target_link_libraries(test_tdbutil tdb gtest gtest_main)

# overflow pages testing
add_executable(test_tdboverflow "tdbExOVFLTest.cpp")
target_link_libraries(test_tdboverflow tdb gtest gtest_main)

# page defragment testing
add_executable(test_tdbdefragment "tdbPageDefragmentTest.cpp")
target_link_libraries(test_tdbdefragment tdb gtest gtest_main)

# page recycling testing
add_executable(test_tdbrecyle "tdbPageRecycleTest.cpp")
target_link_libraries(test_tdbrecyle tdb gtest gtest_main)

