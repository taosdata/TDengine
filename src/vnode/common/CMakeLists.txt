aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src SOURCE_LIST)

list(REMOVE_ITEM SOURCE_LIST ${CMAKE_CURRENT_SOURCE_DIR}/src/vnodePeer.c)

message(STATUS "Common source file ${SOURCE_LIST}")

add_library(common ${SOURCE_LIST})
target_include_directories(common PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc")
