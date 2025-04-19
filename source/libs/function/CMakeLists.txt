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
    PRIVATE decimal
    PUBLIC transport
)

# SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/build/bin)

if(${BUILD_WITH_UDF})
add_executable(taosudf src/udfd.c)

if(${TD_DARWIN})
    target_compile_options(taosudf PRIVATE -Wno-error=deprecated-non-prototype)
endif()

target_include_directories(
    taosudf
    PUBLIC
    "${TD_SOURCE_DIR}/include/libs/function"
    "${TD_SOURCE_DIR}/include/util"
    "${TD_SOURCE_DIR}/include/common"
    "${TD_SOURCE_DIR}/include/libs/transport"
    "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    taosudf
    PRIVATE os util common nodes function
)

if(UNIX AND NOT APPLE)
    # ref: https://cmake.org/cmake/help/latest/release/3.4.html#deprecated-and-removed-features
    set_target_properties(taosudf PROPERTIES
        ENABLE_EXPORTS ON
    )
endif()

if(${BUILD_TEST})
    add_subdirectory(test)
endif()

endif(${BUILD_WITH_UDF})
