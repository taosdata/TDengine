configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/version.c.in" "${CMAKE_CURRENT_SOURCE_DIR}/src/version.c")
aux_source_directory(src UTIL_SRC)
add_library(util STATIC ${UTIL_SRC})
target_include_directories(
    util
    PUBLIC "${CMAKE_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    util
    PRIVATE os
    PUBLIC lz4_static
    PUBLIC api cjson
)

if(${BUILD_TEST})
    IF (NOT TD_DARWIN)
        ADD_SUBDIRECTORY(test)
    ENDIF ()
endif(${BUILD_TEST})

