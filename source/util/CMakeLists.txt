configure_file("${CMAKE_CURRENT_SOURCE_DIR}/src/version.c.in" "${CMAKE_CURRENT_SOURCE_DIR}/src/version.c")
aux_source_directory(src UTIL_SRC)
add_library(util STATIC ${UTIL_SRC})
if (DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
endif()

IF (${ASSERT_NOT_CORE})
    ADD_DEFINITIONS(-DASSERT_NOT_CORE)
    MESSAGE(STATUS "disable assert core")
ELSE ()
    MESSAGE(STATUS "enable assert core")
ENDIF (${ASSERT_NOT_CORE})

target_include_directories(
    util
    PUBLIC "${TD_SOURCE_DIR}/include/util"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/common"
    PRIVATE "${GRANT_CFG_INCLUDE_DIR}"
    PRIVATE "${TD_SOURCE_DIR}/utils/TSZ/sz/inc"
)
target_link_libraries(
    util
    PUBLIC os
    PUBLIC lz4_static
    PUBLIC api cjson geos_c TSZ
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
