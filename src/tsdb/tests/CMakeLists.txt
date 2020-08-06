AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

add_executable(tsdbTests ${SOURCE_LIST})
target_link_libraries(tsdbTests gtest gtest_main pthread common tsdb tutil trpc)

add_test(NAME unit COMMAND ${CMAKE_CURRENT_BINARY_DIR}/tsdbTests)