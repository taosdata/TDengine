add_executable(transportTest "")
add_executable(transUT "")
add_executable(svrBench "")
add_executable(cliBench "")
add_executable(httpBench "")

target_sources(transUT
  PRIVATE
  "transUT.cpp"
)

target_sources(transportTest 
  PRIVATE
  "transportTests.cpp"
)

target_sources(svrBench
  PRIVATE
  "svrBench.c"
)
target_sources(cliBench
  PRIVATE
  "cliBench.c"
)
target_sources(httpBench
  PRIVATE
  "http_test.c"
) 

target_include_directories(transportTest 
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries (transportTest
  os  
  util
  common
  gtest_main
  transport
  function
)

target_link_libraries (transUT
  os  
  util
  common
  gtest_main
  transport 
)

target_include_directories(transUT
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_include_directories(svrBench
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries (svrBench
  os  
  util
  common
  gtest_main
  transport 
)

target_include_directories(cliBench
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_include_directories(httpBench
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries (cliBench
  os  
  util
  common
  gtest_main
  transport 
)

target_link_libraries(httpBench
  os  
  util
  common
  gtest_main
  transport 
)

add_test(
  NAME transUT 
  COMMAND transUT 
)
add_test(
  NAME transUtilUt 
  COMMAND transportTest
)
