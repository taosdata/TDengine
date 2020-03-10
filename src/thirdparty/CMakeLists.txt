aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src SOURCE_LIST)

add_library(thirdparty ${SOURCE_LIST})
target_include_directories(thirdparty PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/inc)