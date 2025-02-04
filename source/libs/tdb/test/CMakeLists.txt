# tdbTest
add_executable(tdbTest "tdbTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbTest PUBLIC tdb)
    DEP_ext_gtest(tdbTest)
    DEP_ext_cjson_LIB(tdbTest)
endif()                         # }

# tdbUtilTest
add_executable(tdbUtilTest "tdbUtilTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbUtilTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbUtilTest PUBLIC tdb)
    DEP_ext_gtest(tdbUtilTest)
    DEP_ext_cjson_LIB(tdbUtilTest)
endif()                         # }

# overflow pages testing
add_executable(tdbExOVFLTest "tdbExOVFLTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbExOVFLTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbExOVFLTest PUBLIC tdb)
    DEP_ext_gtest(tdbExOVFLTest)
    DEP_ext_cjson_LIB(tdbExOVFLTest)
endif()                         # }

# page defragment testing
add_executable(tdbPageDefragmentTest "tdbPageDefragmentTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbPageDefragmentTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbPageDefragmentTest PUBLIC tdb)
    DEP_ext_gtest(tdbPageDefragmentTest)
    DEP_ext_cjson_LIB(tdbPageDefragmentTest)
endif()                         # }

# page recycling testing
add_executable(tdbPageRecycleTest "tdbPageRecycleTest.cpp")
if(${FALLBACK})                 # {
    target_link_libraries(tdbPageRecycleTest tdb gtest gtest_main)
else()                          # }{
    target_link_libraries(tdbPageRecycleTest PUBLIC tdb)
    DEP_ext_gtest(tdbPageRecycleTest)
    DEP_ext_cjson_LIB(tdbPageRecycleTest)
endif()                         # }
