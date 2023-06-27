aux_source_directory(src FUNCTION_SRC)
aux_source_directory(src/detail FUNCTION_SRC_DETAIL)
list(REMOVE_ITEM FUNCTION_SRC src/udfd.c)
add_library(function STATIC ${FUNCTION_SRC} ${FUNCTION_SRC_DETAIL})
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

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
  ADD_DEFINITIONS(-DTD_JEMALLOC_ENABLED -I${CMAKE_BINARY_DIR}/build/include -L${CMAKE_BINARY_DIR}/build/lib -Wl,-rpath,${CMAKE_BINARY_DIR}/build/lib -ljemalloc)
  SET(LINK_JEMALLOC "-L${CMAKE_BINARY_DIR}/build/lib -ljemalloc")
ELSE ()
  SET(LINK_JEMALLOC "")
ENDIF ()

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEPENDENCIES(function jemalloc)
ENDIF ()

target_link_libraries(
    function
    PRIVATE os 
    PRIVATE util 
    PRIVATE common 
    PRIVATE nodes 
    PRIVATE qcom 
    PRIVATE scalar 
    PRIVATE geometry 
    PRIVATE transport 
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

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEPENDENCIES(runUdf jemalloc)
ENDIF ()

target_link_libraries(
        runUdf
        PUBLIC uv_a
        PRIVATE os util common nodes function ${LINK_JEMALLOC}
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

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEPENDENCIES(udf1 jemalloc)
ENDIF ()

target_link_libraries(
    udf1 PUBLIC os ${LINK_JEMALLOC})


add_library(udf1_dup STATIC MODULE test/udf1_dup.c)
target_include_directories(
        udf1_dup
        PUBLIC
            "${TD_SOURCE_DIR}/include/libs/function"
            "${TD_SOURCE_DIR}/include/util"
            "${TD_SOURCE_DIR}/include/common"
            "${TD_SOURCE_DIR}/include/client"
            "${TD_SOURCE_DIR}/include/os"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEPENDENCIES(udf1_dup jemalloc)
ENDIF ()

target_link_libraries(
    udf1_dup PUBLIC os ${LINK_JEMALLOC})

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

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEPENDENCIES(udf2 jemalloc)
ENDIF ()

target_link_libraries(
    udf2 PUBLIC os ${LINK_JEMALLOC}
)

add_library(udf2_dup STATIC MODULE test/udf2_dup.c)
target_include_directories(
        udf2_dup
        PUBLIC
            "${TD_SOURCE_DIR}/include/libs/function"
            "${TD_SOURCE_DIR}/include/util"
            "${TD_SOURCE_DIR}/include/common"
            "${TD_SOURCE_DIR}/include/client"
            "${TD_SOURCE_DIR}/include/os"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEPENDENCIES(udf2_dup jemalloc)
ENDIF ()

target_link_libraries(
    udf2_dup PUBLIC os ${LINK_JEMALLOC}
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

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    ADD_DEPENDENCIES(udfd jemalloc)
ENDIF ()

target_link_libraries(
    udfd
    PUBLIC uv_a
    PRIVATE os util common nodes function ${LINK_JEMALLOC}
    )

