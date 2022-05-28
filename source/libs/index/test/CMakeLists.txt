add_executable(idxTest "")
add_executable(idxFstTest "")
add_executable(idxFstUT "")
add_executable(idxUtilUT "")
add_executable(idxJsonUT "")

target_sources(idxTest
  PRIVATE 
  "indexTests.cc" 
)
target_sources(idxFstTest
  PRIVATE 
  "fstTest.cc" 
)

target_sources(idxFstUT
  PRIVATE 
  "fstUT.cc" 
)
target_sources(idxUtilUT
  PRIVATE 
  "utilUT.cc" 
)

target_sources(idxJsonUT
  PRIVATE 
  "jsonUT.cc" 
)
target_include_directories (idxTest
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 
target_include_directories (idxFstTest 
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_include_directories (idxFstUT 
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_include_directories (idxUtilUT 
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_include_directories (idxJsonUT
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/index" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 
target_link_libraries (idxTest
  os  
  util
  common
  gtest_main
  index
)
target_link_libraries (idxFstTest
  os  
  util
  common
  gtest_main
  index
)
target_link_libraries (idxFstUT
  os  
  util
  common
  gtest_main
  index
)

target_link_libraries (idxUtilUT
  os  
  util
  common
  gtest_main
  index
)

target_link_libraries (idxJsonUT
  os  
  util
  common
  gtest_main
  index
)

if(NOT TD_WINDOWS)
  add_test(
    NAME idxtest
    COMMAND idxTest
  )
endif(NOT TD_WINDOWS)
add_test(
  NAME idxJsonUT
  COMMAND idxJsonUT 
)
add_test(
  NAME idxUtilUT 
  COMMAND idxUtilUT 
)
add_test(
  NAME idxFstUT 
  COMMAND idxFstUT 
)
