MESSAGE(STATUS "vnode unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

# add_executable(tqTest "")
# target_sources(tqTest
#     PRIVATE
#     "tqMetaTest.cpp"
# )
# target_include_directories(tqTest
#     PUBLIC
#     "${TD_SOURCE_DIR}/include/server/vnode/tq"
#     "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )

# target_link_libraries(tqTest
#     tq
#     gtest_main
# )
# enable_testing()
# add_test(
#     NAME tq_test
#     COMMAND tqTest
# )

# ADD_EXECUTABLE(tsdbSmaTest tsdbSmaTest.cpp)
# TARGET_LINK_LIBRARIES(
#         tsdbSmaTest
#         PUBLIC os util common vnode gtest_main
# )

# TARGET_INCLUDE_DIRECTORIES(
#         tsdbSmaTest
#         PUBLIC "${TD_SOURCE_DIR}/include/common"
#         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../src/inc"
#         PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
# )