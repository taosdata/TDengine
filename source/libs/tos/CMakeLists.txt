aux_source_directory(src TOS_SRC)

add_library(tos STATIC ${TOS_SRC})
target_include_directories(
    tos
    PUBLIC "${TD_SOURCE_DIR}/include/libs/tos"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    tos
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
