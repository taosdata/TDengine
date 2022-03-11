aux_source_directory(src OS_SRC)
add_library(os STATIC ${OS_SRC})
target_include_directories(
    os
    PUBLIC "${CMAKE_SOURCE_DIR}/include/os"
    PRIVATE "${CMAKE_SOURCE_DIR}/include"
    PRIVATE "${CMAKE_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_SOURCE_DIR}/contrib/pthread-win32"
)
target_link_libraries(
    os pthread dl rt m
)
