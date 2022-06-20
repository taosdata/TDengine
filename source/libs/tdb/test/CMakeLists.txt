# tdbTest
add_executable(tdbTest "tdbTest.cpp")
target_link_libraries(tdbTest tdb gtest gtest_main)

# tdbUtilTest
add_executable(tdbUtilTest "tdbUtilTest.cpp")
target_link_libraries(tdbUtilTest tdb gtest gtest_main)

# tdbUtilTest
add_executable(tdbExOVFLTest "tdbExOVFLTest.cpp")
target_link_libraries(tdbExOVFLTest tdb gtest gtest_main)

