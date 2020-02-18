aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

message(STATUS "COMMON: ${SOURCE_LIST}")

add_executable(commonTests ${SOURCE_LIST})

target_link_libraries(commonTests gtest gtest_main pthread common)

add_test(
    NAME
      unit   
    COMMAND
      ${CMAKE_CURRENT_BINARY_DIR}/commonTests
)