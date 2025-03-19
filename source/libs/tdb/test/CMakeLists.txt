# tdbTest
add_executable(tdbTest "tdbTest.cpp")
DEP_ext_gtest(tdbTest)
target_link_libraries(tdbTest PRIVATE tdb)

# tdbUtilTest
add_executable(tdbUtilTest "tdbUtilTest.cpp")
DEP_ext_gtest(tdbUtilTest)
target_link_libraries(tdbUtilTest PRIVATE tdb)

# overflow pages testing
add_executable(tdbExOVFLTest "tdbExOVFLTest.cpp")
DEP_ext_gtest(tdbExOVFLTest)
target_link_libraries(tdbExOVFLTest PRIVATE tdb)

# page defragment testing
add_executable(tdbPageDefragmentTest "tdbPageDefragmentTest.cpp")
DEP_ext_gtest(tdbPageDefragmentTest)
target_link_libraries(tdbPageDefragmentTest PRIVATE tdb)

# page recycling testing
add_executable(tdbPageRecycleTest "tdbPageRecycleTest.cpp")
DEP_ext_gtest(tdbPageRecycleTest)
target_link_libraries(tdbPageRecycleTest PRIVATE tdb)

