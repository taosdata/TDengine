aux_source_directory(src OS_SRC)
add_library(os STATIC ${OS_SRC})
target_include_directories(
    os
    PUBLIC "${CMAKE_SOURCE_DIR}/include/os"
    PUBLIC "${CMAKE_SOURCE_DIR}/include"
    PUBLIC "${CMAKE_SOURCE_DIR}/include/util"
    PUBLIC "${CMAKE_SOURCE_DIR}/contrib/pthread-win32"
    PUBLIC "${CMAKE_SOURCE_DIR}/contrib/gnuregex"
)
target_link_libraries(
    os pthread dl rt m
)
