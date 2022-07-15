add_executable(transportTest "")
add_executable(transUT "")
add_executable(pushServer "")

target_sources(transUT
  PRIVATE
  "transUT.cpp"
)

target_sources(transportTest 
  PRIVATE
  "transportTests.cpp"
)

target_sources(pushServer
  PRIVATE
  "pushServer.c"
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

target_include_directories(pushServer
  PUBLIC
  "${TD_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

target_link_libraries (pushServer
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

