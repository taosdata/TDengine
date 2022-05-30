aux_source_directory(src FUNCTION_SRC)
list(REMOVE_ITEM FUNCTION_SRC src/udfd.c)
add_library(function STATIC ${FUNCTION_SRC})
target_include_directories(
    function
    PUBLIC
        "${TD_SOURCE_DIR}/include/libs/function"
        "${TD_SOURCE_DIR}/include/util"
        "${TD_SOURCE_DIR}/include/common"
        "${TD_SOURCE_DIR}/include/client"
        "${TD_SOURCE_DIR}/contrib/libuv/include"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    function
    PRIVATE os util common nodes scalar qcom transport
    PUBLIC uv_a
)

add_executable(runUdf test/runUdf.c)
target_include_directories(
        runUdf
        PUBLIC
            "${TD_SOURCE_DIR}/include/libs/function"
            "${TD_SOURCE_DIR}/contrib/libuv/include"
            "${TD_SOURCE_DIR}/include/util"
            "${TD_SOURCE_DIR}/include/common"
            "${TD_SOURCE_DIR}/include/client"
            "${TD_SOURCE_DIR}/include/os"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
        runUdf
        PUBLIC uv_a
        PRIVATE os util common nodes function
)

add_library(udf1 STATIC MODULE test/udf1.c)
target_include_directories(
        udf1
        PUBLIC
            "${TD_SOURCE_DIR}/include/libs/function"
            "${TD_SOURCE_DIR}/include/util"
            "${TD_SOURCE_DIR}/include/common"
            "${TD_SOURCE_DIR}/include/client"
            "${TD_SOURCE_DIR}/include/os"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    udf1 PUBLIC os)

add_library(udf2 STATIC MODULE test/udf2.c)
target_include_directories(
        udf2
        PUBLIC
            "${TD_SOURCE_DIR}/include/libs/function"
            "${TD_SOURCE_DIR}/include/util"
            "${TD_SOURCE_DIR}/include/common"
            "${TD_SOURCE_DIR}/include/client"
            "${TD_SOURCE_DIR}/include/os"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    udf2 PUBLIC os
)

#SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/build/bin)
add_executable(udfd src/udfd.c)
target_include_directories(
        udfd
        PUBLIC
            "${TD_SOURCE_DIR}/include/libs/function"
            "${TD_SOURCE_DIR}/contrib/libuv/include"
            "${TD_SOURCE_DIR}/include/util"
            "${TD_SOURCE_DIR}/include/common"
            "${TD_SOURCE_DIR}/include/libs/transport"
            "${TD_SOURCE_DIR}/include/client"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        udfd
        PUBLIC uv_a
        PRIVATE os util common nodes function
)

