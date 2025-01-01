configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/version.c.in" "${CMAKE_CURRENT_SOURCE_DIR}/src/version.c")
aux_source_directory(src UTIL_SRC)
IF(COMPILER_SUPPORT_AVX2)
    MESSAGE(STATUS "AVX2 instructions is ACTIVATED")
    set_source_files_properties(src/tdecompressavx.c PROPERTIES COMPILE_FLAGS -mavx2)
ENDIF()
add_library(util STATIC ${UTIL_SRC})

add_dependencies(util ext_lz4)
target_include_directories(util PUBLIC "${TD_CONTRIB_EXT_LZ4_INC_DIR}")
target_link_directories(util PUBLIC "${TD_CONTRIB_EXT_LZ4_LIB_DIR}")
target_link_libraries(util PUBLIC lz4)

add_dependencies(util ext_cjson)
target_include_directories(util PUBLIC "${TD_CONTRIB_EXT_CJSON_INC_DIR}/cjson")
target_link_directories(util PUBLIC "${TD_CONTRIB_EXT_CJSON_LIB_DIR}")
target_link_libraries(util PUBLIC cjson)

add_dependencies(util ext_geos)
target_include_directories(util PUBLIC "${TD_CONTRIB_EXT_GEOS_INC_DIR}")

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
    add_definitions(-DUSE_ANALYTICS)
endif()

target_include_directories(
    util
    PUBLIC "${TD_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${GRANT_CFG_INCLUDE_DIR}"
    PRIVATE "${TD_SOURCE_DIR}/utils/TSZ/sz/inc"
    PRIVATE "${TD_SOURCE_DIR}/utils/TSZ/zstd/"
    PRIVATE "${TD_SOURCE_DIR}/contrib/pcre2/"
)

target_link_directories(
    util
    PUBLIC "${TD_SOURCE_DIR}/contrib/pcre2"
)

if(TD_LINUX)
    target_link_libraries(
        util
        PUBLIC os common
        PUBLIC pcre2-8
        PUBLIC api TSZ
    )
else()
    target_link_libraries(
        util
        PUBLIC os common
        PUBLIC pcre2-8
        PUBLIC api TSZ
    )
endif()

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
