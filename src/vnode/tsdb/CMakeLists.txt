aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src SOURCE_LIST)

add_library(tsdb STATIC ${SOURCE_LIST})
target_link_libraries(tsdb common tutil)

target_include_directories(tsdb 
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc" 
  PUBLIC "${CMAKE_SOURCE_DIR}/src/inc"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/util/inc"
  PUBLIC "${CMAKE_SOURCE_DIR}/src/os/linux/inc"
  )

add_subdirectory(tests)