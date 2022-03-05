aux_source_directory(src OS_SRC)
add_library(os STATIC ${OS_SRC})
target_include_directories(
    os
    PUBLIC "${CMAKE_SOURCE_DIR}/include/os"
    PRIVATE "${CMAKE_SOURCE_DIR}/include/common"
    PRIVATE "${CMAKE_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

IF (NOT TD_DARWIN)
target_link_libraries(
    os pthread dl rt m
)
ELSE ()
target_link_libraries(
    os pthread dl m
)
ENDIF ()
