aux_source_directory(src COMMON_SRC)
add_library(common STATIC ${COMMON_SRC})
target_include_directories(
    common
    PUBLIC "${TD_SOURCE_DIR}/include/common"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
IF(${TD_WINDOWS})
    PRIVATE "${TD_SOURCE_DIR}/contrib/pthread-win32"
    PRIVATE "${TD_SOURCE_DIR}/contrib/gnuregex"
ENDIF ()
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
