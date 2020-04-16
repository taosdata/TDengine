aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src SOURCE_LIST)

add_library(lz4 ${SOURCE_LIST})
target_include_directories(lz4 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/inc)