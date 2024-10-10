aux_source_directory(src TOS_SRC)

add_library(tcs STATIC ${TOS_SRC})
target_include_directories(
    tcs
    PUBLIC "${TD_SOURCE_DIR}/include/libs/tcs"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    tcs
    PUBLIC az
    PUBLIC common
    # PUBLIC cjson
    # PUBLIC os
    # PUBLIC util
    # PUBLIC crypt
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
