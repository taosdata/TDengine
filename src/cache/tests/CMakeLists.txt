AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

add_executable(cacheTests ${SOURCE_LIST})
target_link_libraries(cacheTests gtest gtest_main pthread common cache tutil )

add_test(NAME unit COMMAND ${CMAKE_CURRENT_BINARY_DIR}/cacheTests)