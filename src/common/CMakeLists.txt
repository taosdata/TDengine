aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/src SOURCE_LIST)

add_library(common ${SOURCE_LIST})
target_include_directories(
    common 
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/inc
        PUBLIC ${CMAKE_SOURCE_DIR}/src/inc
        PUBLIC ${CMAKE_SOURCE_DIR}/src/os/linux/inc
        )