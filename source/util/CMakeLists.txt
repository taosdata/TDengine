if(${FALLBACK})                   # {
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/version.c.in" "${CMAKE_CURRENT_SOURCE_DIR}/src/version.c")
else()                            # }{
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/version.c.in" "${CMAKE_CURRENT_BINARY_DIR}/generated_version.c")
endif()                           # }
aux_source_directory(src UTIL_SRC)
IF(COMPILER_SUPPORT_AVX2)
    MESSAGE(STATUS "AVX2 instructions is ACTIVATED")
    set_source_files_properties(src/tdecompressavx.c PROPERTIES COMPILE_FLAGS -mavx2)
ENDIF()
add_library(util STATIC ${UTIL_SRC})
if(NOT ${FALLBACK})               # {
    set_source_files_properties(
        ${CMAKE_CURRENT_SOURCE_DIR}/src/version.tab.c
        PROPERTIES INCLUDE_DIRECTORIES ${CMAKE_CURRENT_BINARY_DIR}
    )
endif()                           # }

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

if(${FALLBACK})                 # {
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
else()                          # }{
    target_include_directories(
        util
        PUBLIC "${TD_SOURCE_DIR}/include/util"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
        PRIVATE "${TD_SOURCE_DIR}/include/common"
        PRIVATE "${GRANT_CFG_INCLUDE_DIR}"
        PRIVATE "${TD_SOURCE_DIR}/utils/TSZ/sz/inc"  # freemine: to remove?
        PRIVATE "${TD_SOURCE_DIR}/utils/TSZ/zstd/"   # freemine: to remove?
    )

    DEP_ext_lz4(util)

    DEP_ext_geos(util)
    if(NOT ${TD_WINDOWS})          # {
        target_link_libraries(
            util
            PUBLIC m stdc++
        )
    endif()                        # }

    DEP_ext_pcre2(util)

    if(${BUILD_WITH_UV_TRANS})
        if(${BUILD_WITH_UV})
            DEP_ext_libuv(util)
            add_definitions(-DUSE_UV)
        endif(${BUILD_WITH_UV})
    endif(${BUILD_WITH_UV_TRANS})

    add_dependencies(util TSZ)

    if(TD_LINUX)
        target_link_libraries(
            util
            PUBLIC os common
            PUBLIC api TSZ
        )
    else()
        target_link_libraries(
            util
            PUBLIC os common
            PUBLIC api TSZ
        )
    endif()
endif()                         # }

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
