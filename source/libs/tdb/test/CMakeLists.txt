# tdbTest
add_executable(tdbTest "tdbTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbTest PUBLIC tdb)
    DEP_ext_gtest(tdbTest)
endif()                         # }

# tdbUtilTest
add_executable(tdbUtilTest "tdbUtilTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbUtilTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbUtilTest PUBLIC tdb)
    DEP_ext_gtest(tdbUtilTest)
endif()                         # }

# overflow pages testing
add_executable(tdbExOVFLTest "tdbExOVFLTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbExOVFLTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbExOVFLTest PUBLIC tdb)
    DEP_ext_gtest(tdbExOVFLTest)
endif()                         # }

# page defragment testing
add_executable(tdbPageDefragmentTest "tdbPageDefragmentTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbPageDefragmentTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbPageDefragmentTest PUBLIC tdb)
    DEP_ext_gtest(tdbPageDefragmentTest)
endif()                         # }

# page recycling testing
add_executable(tdbPageRecycleTest "tdbPageRecycleTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbPageRecycleTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbPageRecycleTest PUBLIC tdb)
    DEP_ext_gtest(tdbPageRecycleTest)
endif()                         # }
