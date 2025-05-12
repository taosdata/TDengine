configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/version.c.in" "${CMAKE_CURRENT_SOURCE_DIR}/src/version.c")
aux_source_directory(src UTIL_SRC)
IF(COMPILER_SUPPORT_AVX2)
    MESSAGE(STATUS "AVX2 instructions is ACTIVATED")
    set_source_files_properties(src/tdecompressavx.c PROPERTIES COMPILE_FLAGS -mavx2)
ENDIF()
add_library(util STATIC ${UTIL_SRC})
DEP_ext_lz4(util)
if(TD_LINUX)
  DEP_ext_xxhash(util)
  DEP_ext_lzma2(util)
endif()
DEP_ext_geos(util)
if(${BUILD_PCRE2})          # {
  DEP_ext_pcre2(util)
  target_compile_definitions(util PRIVATE -DPCRE2_STATIC)
endif()                     # }

if(DEFINED GRANT_CFG_INCLUDE_DIR)
    add_definitions(-DGRANTS_CFG)
endif()

if(${ASSERT_NOT_CORE})
    ADD_DEFINITIONS(-DASSERT_NOT_CORE)
    MESSAGE(STATUS "disable assert core")
else()
    MESSAGE(STATUS "enable assert core")
endif(${ASSERT_NOT_CORE})

target_include_directories(
    util
    PUBLIC "${TD_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/common"
    PRIVATE "${GRANT_CFG_INCLUDE_DIR}"
)

target_link_libraries(
    util
    PUBLIC os common decimal
    PUBLIC api
    PUBLIC TSZ
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
