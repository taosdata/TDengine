configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/version.c.in" "${CMAKE_CURRENT_SOURCE_DIR}/src/version.c")
aux_source_directory(src UTIL_SRC)
add_library(util STATIC ${UTIL_SRC})

if(DEFINED GRANT_CFG_INCLUDE_DIR)
    add_definitions(-DGRANTS_CFG)
endif()

if(${ASSERT_NOT_CORE})
    ADD_DEFINITIONS(-DASSERT_NOT_CORE)
    MESSAGE(STATUS "disable assert core")
else()
    MESSAGE(STATUS "enable assert core")
endif(${ASSERT_NOT_CORE})

if(${BUILD_WITH_ANALYSIS})
    add_definitions(-DUSE_ANAL)
endif()

target_include_directories(
    util
    PUBLIC "${TD_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/common"
    PRIVATE "${GRANT_CFG_INCLUDE_DIR}"
    PRIVATE "${TD_SOURCE_DIR}/utils/TSZ/sz/inc"
    PRIVATE "${TD_SOURCE_DIR}/utils/TSZ/zstd/"
    PRIVATE "${TD_SOURCE_DIR}/contrib/lzma2/"
    PRIVATE "${TD_SOURCE_DIR}/contrib/pcre2/"
)

target_link_directories(
    util
    PUBLIC "${TD_SOURCE_DIR}/contrib/lzma2"
    PUBLIC "${TD_SOURCE_DIR}/contrib/pcre2"
)

if(TD_LINUX)
    target_link_libraries(
        util
        PUBLIC os common
        PUBLIC lz4_static fast-lzma2 pcre2-8
        PUBLIC api cjson geos_c TSZ
    )
else()
    target_link_libraries(
        util
        PUBLIC os common
        PUBLIC lz4_static pcre2-8
        PUBLIC api cjson geos_c TSZ
    )
endif()

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
