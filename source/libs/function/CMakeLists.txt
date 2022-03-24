aux_source_directory(src FUNCTION_SRC)
list(REMOVE_ITEM FUNCTION_SRC src/udfd.c)
add_library(function STATIC ${FUNCTION_SRC})
target_include_directories(
    function
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/function"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    function
    PRIVATE os util common nodes
)

add_executable(runUdf test/runUdf.c)
target_include_directories(
        runUdf
        PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/function"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
        runUdf
        PRIVATE os util common nodes function
)

add_library(udf1 MODULE test/udf1.c)
target_include_directories(
        udf1
        PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/function"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

#SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/build/bin)
add_executable(udfd src/udfd.c)
target_include_directories(
        udfd
        PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/function"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        udfd
        PRIVATE os util common nodes function
)

