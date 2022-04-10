add_executable(indexTest "")
add_executable(fstTest "")
add_executable(fstUT "")
add_executable(UtilUT "")
add_executable(jsonUT "")

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
target_sources(UtilUT
  PRIVATE 
  "utilUT.cc" 
)

target_sources(jsonUT
  PRIVATE 
  "jsonUT.cc" 
)
target_include_directories ( indexTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 
target_include_directories ( fstTest 
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_include_directories ( fstUT 
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_include_directories ( UtilUT 
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_include_directories (jsonUT
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
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

target_link_libraries (UtilUT
  os  
  util
  common
  gtest_main
  index
)

target_link_libraries (jsonUT
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
