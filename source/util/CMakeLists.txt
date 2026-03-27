configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/version.c.in" "${CMAKE_CURRENT_BINARY_DIR}/version.c.tmp")
set(_version_c "${CMAKE_CURRENT_SOURCE_DIR}/src/version.c")
set(_need_update TRUE)
if(EXISTS "${_version_c}")
    file(STRINGS "${CMAKE_CURRENT_BINARY_DIR}/version.c.tmp" _new_ver LIMIT_COUNT 1)
    file(STRINGS "${_version_c}" _old_ver LIMIT_COUNT 1)
    if("${_new_ver}" STREQUAL "${_old_ver}")
        set(_need_update FALSE)
    endif()
endif()
if(_need_update)
    execute_process(COMMAND ${CMAKE_COMMAND} -E copy
        "${CMAKE_CURRENT_BINARY_DIR}/version.c.tmp" "${_version_c}")
endif()

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
if (NOT ${TD_WINDOWS})
  DEP_ext_ssl(util)
endif()
DEP_ext_geos(util)
if(${BUILD_PCRE2})          # {
  DEP_ext_pcre2(util)
  target_compile_definitions(util PRIVATE -DPCRE2_STATIC)
endif()                     # }

if(DEFINED GRANT_CFG_INCLUDE_DIR)
    add_definitions(-DGRANTS_CFG)
endif()

target_include_directories(
    util
    PUBLIC "${TD_SOURCE_DIR}/include/util"
    PUBLIC "${TD_SOURCE_DIR}/contrib/libaes/inc"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/common"
    PRIVATE "${GRANT_CFG_INCLUDE_DIR}"
)

target_link_libraries(
    util
    PUBLIC os common decimal
    PUBLIC api
    PUBLIC TSZ
    PUBLIC aes
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
