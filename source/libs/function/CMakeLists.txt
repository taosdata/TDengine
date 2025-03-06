aux_source_directory(src FUNCTION_SRC)
aux_source_directory(src/detail FUNCTION_SRC_DETAIL)
list(REMOVE_ITEM FUNCTION_SRC src/udfd.c)

if(COMPILER_SUPPORT_AVX2)
    MESSAGE(STATUS "AVX2 instructions is ACTIVATED")
    set_source_files_properties(src/detail/tminmaxavx.c PROPERTIES COMPILE_FLAGS -mavx2)
endif()

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

target_link_libraries(udf1 PUBLIC os)

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

target_link_libraries(udf1_dup PUBLIC os)

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

target_link_libraries(udf2 PUBLIC os)

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

target_link_libraries(udf2_dup PUBLIC os)

set(TARGET_NAMES
    change_udf_normal
    change_udf_no_init
    change_udf_no_process
    change_udf_no_destroy
    change_udf_init_failed
    change_udf_process_failed
    change_udf_destory_failed
)

set(COMPILE_DEFINITIONS
    CHANGE_UDF_NORMAL
    CHANGE_UDF_NO_INIT
    CHANGE_UDF_NO_PROCESS
    CHANGE_UDF_NO_DESTROY
    CHANGE_UDF_INIT_FAILED
    CHANGE_UDF_PROCESS_FAILED
    CHANGE_UDF_DESTORY_FAILED
)

foreach(index RANGE 0 6)
    list(GET TARGET_NAMES ${index} target_name)
    list(GET COMPILE_DEFINITIONS ${index} compile_def)

    add_library(${target_name} STATIC MODULE test/change_udf.c)
    target_include_directories(
        ${target_name}
        PUBLIC
        "${TD_SOURCE_DIR}/include/libs/function"
        "${TD_SOURCE_DIR}/include/util"
        "${TD_SOURCE_DIR}/include/common"
        "${TD_SOURCE_DIR}/include/client"
        "${TD_SOURCE_DIR}/include/os"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    )
    target_compile_definitions(${target_name} PRIVATE ${compile_def})
    target_link_libraries(
        ${target_name} PUBLIC os
    )
endforeach()

# SET(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/build/bin)
add_executable(udfd src/udfd.c)

if(${TD_DARWIN})
    target_compile_options(udfd PRIVATE -Wno-error=deprecated-non-prototype)
endif()

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
