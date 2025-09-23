# tdbTest
if(${TD_LINUX})
    add_executable(tdbTest "tdbTest.cpp")
    DEP_ext_gtest(tdbTest)
    target_link_libraries(tdbTest PRIVATE tdb)
endif()

# tdbUtilTest
add_executable(tdbUtilTest "tdbUtilTest.cpp")
DEP_ext_gtest(tdbUtilTest)
target_link_libraries(tdbUtilTest PRIVATE tdb)
add_test(
    NAME tdbUtilTest
    COMMAND tdbUtilTest
)

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

# page flush testing
add_executable(tdbPageFlushTest "tdbPageFlushTest.cpp")
DEP_ext_gtest(tdbPageFlushTest)
target_link_libraries(tdbPageFlushTest PRIVATE tdb)

# btree to stack testing
add_executable(tdbBtreeToStackTest "tdbBtreeToStackTest.cpp")
DEP_ext_gtest(tdbBtreeToStackTest)
target_link_libraries(tdbBtreeToStackTest PRIVATE tdb)

add_test(
    NAME tdbPageFlushTest
    COMMAND tdbPageFlushTest
)
