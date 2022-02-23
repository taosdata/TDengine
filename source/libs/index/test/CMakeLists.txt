add_executable(indexTest "")
add_executable(fstTest "")
add_executable(fstUT "")

target_sources(indexTest
  PRIVATE 
  "indexTests.cc" 
)
target_sources(fstTest
  PRIVATE 
  "fstTest.cc" 
)

target_sources(fstUT
  PRIVATE 
  "fstUT.cc" 
)
target_include_directories ( indexTest
  PUBLIC
  "${CMAKE_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 
target_include_directories ( fstTest 
  PUBLIC
  "${CMAKE_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_include_directories ( fstUT 
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
target_link_libraries (fstTest
  os  
  util
  common
  gtest_main
  index
)
target_link_libraries (fstUT
  os  
  util
  common
  gtest_main
  index
)


#add_test(
#  NAME index_test
#  COMMAND indexTest
#)
