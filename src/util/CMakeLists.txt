aux_source_directory(source UTIL_SRC)
add_library(util ${UTIL_SRC})
target_include_directories(
    util
    PUBLIC "${CMAKE_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_SOURCE_DIR}/include"
)
target_link_libraries(
    util
    PRIVATE os
)