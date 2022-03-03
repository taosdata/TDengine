aux_source_directory(src COMMON_SRC)
add_library(common STATIC ${COMMON_SRC})
target_include_directories(
    common
    PUBLIC "${CMAKE_SOURCE_DIR}/include/common"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    common
    PUBLIC os
    PUBLIC util
    INTERFACE api
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
