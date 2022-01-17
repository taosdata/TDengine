add_executable(transportTest "")
target_sources(transportTest 
  PRIVATE
  "transportTests.cc"
)  

target_include_directories(transportTest 
  PUBLIC
  "${CMAKE_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_link_libraries (transportTest
  os  
  util
  common
  gtest_main
  transport 
)


