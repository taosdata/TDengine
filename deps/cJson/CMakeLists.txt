aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src SOURCE_LIST)

add_library(cJson ${SOURCE_LIST})
target_include_directories(cJson PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/inc)