# tdbMPoolTest
add_executable(tdbMPoolTest "tdbMPoolTest.cpp")
target_link_libraries(tdbMPoolTest tdb gtest gtest_main)

# tdbTest
add_executable(tdbTest "tdbTest.cpp")
target_link_libraries(tdbTest tdb gtest gtest_main)