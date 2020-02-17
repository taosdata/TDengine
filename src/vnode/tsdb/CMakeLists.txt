aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src SOURCE_LIST)

message(STATUS "tsdb source files: ${SOURCE_LIST}")

add_library(tsdb STATIC ${SOURCE_LIST})

target_link_libraries(tsdb common)

target_include_directories(tsdb PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc")