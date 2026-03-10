include_directories("${TD_SOURCE_DIR}/include/libs/function")
include_directories("${TD_SOURCE_DIR}/include/util")
include_directories("${TD_SOURCE_DIR}/include/common")
include_directories("${TD_SOURCE_DIR}/include/client")
include_directories("${TD_SOURCE_DIR}/include/os")
include_directories("${CMAKE_CURRENT_SOURCE_DIR}/../inc")

add_executable(runUdf runUdf.c)
target_link_libraries(
    runUdf
    PRIVATE os util common nodes function
)

add_library(udf1 STATIC MODULE udf1.c)
target_link_libraries(udf1 PUBLIC os)

add_library(udf2 STATIC MODULE udf2.c)
target_link_libraries(udf2 PUBLIC os)

add_library(udf1_dup STATIC MODULE udf1_dup.c)
target_link_libraries(udf1_dup PUBLIC os)

add_library(udf2_dup STATIC MODULE udf2_dup.c)
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
    add_library(${target_name} STATIC MODULE change_udf.c)
    target_compile_definitions(${target_name} PRIVATE ${compile_def})
    target_link_libraries(${target_name} PUBLIC os)
endforeach()
