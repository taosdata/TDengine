add_executable(indexTest "")
target_sources(indexTest
  PRIVATE 
  "../src/index.c"  
  "indexTests.cpp" 
)
target_include_directories ( indexTest
  PUBLIC
  "${CMAKE_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 
target_link_libraries (indexTest
  os  
  util
  common
  gtest_main
  index
)

add_test(
  NAME index_test
  COMMAND indexTest
)
