add_executable(transportTest "")
add_executable(client "")
add_executable(server "")
add_executable(transUT "")

target_sources(transUT
  PRIVATE
  "transUT.cc"
)  

target_sources(transportTest 
  PRIVATE
  "transportTests.cc"
)  
target_sources (client
  PRIVATE
  "rclient.c" 
)   
target_sources (server
  PRIVATE
  "rserver.c" 
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
target_link_libraries (transUT
  os  
  util
  common
  gtest_main
  transport 
)

target_include_directories(client
  PUBLIC
  "${CMAKE_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_link_libraries (client
  os  
  util
  common
  gtest_main
  transport 
)
target_include_directories(server
  PUBLIC
  "${CMAKE_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 

target_include_directories(transUT
  PUBLIC
  "${CMAKE_SOURCE_DIR}/include/libs/transport" 
  "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
) 


target_link_libraries (server
  os  
  util
  common
  gtest_main
  transport 
)

