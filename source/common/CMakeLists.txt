aux_source_directory(src COMMON_SRC)
aux_source_directory(src/msg COMMON_MSG_SRC)

LIST(APPEND COMMON_SRC ${COMMON_MSG_SRC})

if(TD_ENTERPRISE)
  LIST(APPEND COMMON_SRC ${TD_ENTERPRISE_DIR}/src/plugins/common/src/tglobal.c)
endif()

add_library(common STATIC ${COMMON_SRC})
DEP_ext_cjson(common)
add_dependencies(common lemon_sql)

message(STATUS "Debugging ............................")
message(STATUS "PROJECT_BINARY_DIR:${PROJECT_BINARY_DIR}")

target_include_directories(common
    PRIVATE
        ${CMAKE_SOURCE_DIR}/utils/TSZ/sz/inc      # TODO: tweak for defines.h, potential cyclic-dependencies, and need to be refactored!!!
    PUBLIC
        ${PROJECT_BINARY_DIR}/source/libs/parser    # TODO: tweak for taos_lemon_token.h, potential cyclic-dependencies!!!
)

if(DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
endif()

if(${BUILD_WITH_ANALYSIS})
  add_definitions(-DUSE_ANALYTICS)
endif()

if(TD_GRANT)
  ADD_DEFINITIONS(-D_GRANT)
endif()

if(TD_STORAGE)
  ADD_DEFINITIONS(-D_STORAGE)
  TARGET_LINK_LIBRARIES(common PRIVATE storage)
endif()

if(TD_ENTERPRISE)
  if(${BUILD_WITH_S3})
    add_definitions(-DUSE_S3)
  ELSEIF(${BUILD_WITH_COS})
    add_definitions(-DUSE_COS)
  endif()
endif()

target_include_directories(
  common
  PUBLIC "$ENV{HOME}/.cos-local.2/include"
  PUBLIC "${TD_SOURCE_DIR}/include/common"

  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
  PRIVATE "${GRANT_CFG_INCLUDE_DIR}"
)

if(${TD_WINDOWS})
  # This is the Windows Shell32 library, needed by RocksDB
  find_library(SHLWAPI_LIBRARY NAMES shlwapi)
  if(SHLWAPI_LIBRARY)
      message(STATUS "Found Shlwapi library: ${SHLWAPI_LIBRARY}")
  else()
      message(WARNING "Shlwapi library not found.")
  endif()

  # This is the Windows RPC library, also needed by RocksDB
  find_library(RPC_LIB rpcrt4 HINTS ${SYSTEM_SYSTEM_PATH})
  if(RPC_LIB)
      message(STATUS "Found rpcrt4 library: ${RPC_LIB}")
  else()
      message(FATAL_ERROR "rpcrt4 library not found.")
  endif()

  target_link_libraries(
    common

    PUBLIC ${SHLWAPI_LIBRARY}
    PUBLIC ${RPC_LIB}

    PUBLIC os
    PUBLIC util
    INTERFACE api
  )

else()
  target_link_libraries(
    common

    PUBLIC os
    PUBLIC util
    INTERFACE api
  )
  DEP_ext_curl(common)
  DEP_ext_ssl(common)
endif()

if(${BUILD_S3})
  if(${BUILD_WITH_S3})
    DEP_ext_libs3(common)
    DEP_ext_curl(common)
    DEP_ext_ssl(common)
    DEP_ext_libxml2(common)
    add_definitions(-DUSE_S3)
  endif()

  if(${BUILD_WITH_COS})
    set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
    find_library(APR_LIBRARY apr-1 PATHS /usr/local/apr/lib/)
    find_library(APR_UTIL_LIBRARY aprutil-1 PATHS /usr/local/apr/lib/)
    find_library(MINIXML_LIBRARY mxml)
    target_link_libraries(
      common

      # s3
      PUBLIC cos_c_sdk_static
      PUBLIC ${APR_UTIL_LIBRARY}
      PUBLIC ${APR_LIBRARY}
      PUBLIC ${MINIXML_LIBRARY}
      PUBLIC ${CURL_LIBRARY}
    )

    # s3
    FIND_PROGRAM(APR_CONFIG_BIN NAMES apr-config apr-1-config PATHS /usr/bin /usr/local/bin /usr/local/apr/bin/)

    if(APR_CONFIG_BIN)
      EXECUTE_PROCESS(
        COMMAND ${APR_CONFIG_BIN} --includedir
        OUTPUT_VARIABLE APR_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
      )
    endif()

    include_directories(${APR_INCLUDE_DIR})

    add_definitions(-DUSE_COS)
  endif(${BUILD_WITH_COS})
endif()

if(${BUILD_TEST})
  ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
