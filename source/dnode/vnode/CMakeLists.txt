# vnode
add_library(vnode STATIC "")
set(
  VNODE_SOURCE_FILES
  "src/vnd/vnodeOpen.c"
  "src/vnd/vnodeBufPool.c"
  "src/vnd/vnodeCfg.c"
  "src/vnd/vnodeCommit.c"
  "src/vnd/vnodeQuery.c"
  "src/vnd/vnodeModule.c"
  "src/vnd/vnodeCos.c"
  "src/vnd/vnodeSvr.c"
  "src/vnd/vnodeSync.c"
  "src/vnd/vnodeSnapshot.c"
  "src/vnd/vnodeRetention.c"
  "src/vnd/vnodeInitApi.c"

    # meta
    "src/meta/metaOpen.c"
    "src/meta/metaIdx.c"
    "src/meta/metaTable.c"
    "src/meta/metaSma.c"
    "src/meta/metaQuery.c"
    "src/meta/metaCommit.c"
    "src/meta/metaEntry.c"
    "src/meta/metaSnapshot.c"
    "src/meta/metaCache.c"
    "src/meta/metaTtl.c"

    # sma
    "src/sma/smaEnv.c"
    "src/sma/smaUtil.c"
    "src/sma/smaOpen.c"
    "src/sma/smaCommit.c"
    "src/sma/smaRollup.c"
    "src/sma/smaSnapshot.c"
    "src/sma/smaTimeRange.c"

  # # tsdb
  # "src/tsdb/tsdbCommit.c"
  # "src/tsdb/tsdbFile.c"
  # "src/tsdb/tsdbFS.c"
  # "src/tsdb/tsdbOpen.c"
  # "src/tsdb/tsdbMemTable.c"
  # "src/tsdb/tsdbRead.c"
  # "src/tsdb/tsdbCache.c"
  # "src/tsdb/tsdbWrite.c"
  # "src/tsdb/tsdbReaderWriter.c"
  # "src/tsdb/tsdbUtil.c"
  # "src/tsdb/tsdbSnapshot.c"
  # "src/tsdb/tsdbCacheRead.c"
  # "src/tsdb/tsdbRetention.c"
  # "src/tsdb/tsdbDiskData.c"
  # "src/tsdb/tsdbMergeTree.c"
  # "src/tsdb/tsdbDataIter.c"

    # tq
    "src/tq/tq.c"
    "src/tq/tqUtil.c"
    "src/tq/tqScan.c"
    "src/tq/tqMeta.c"
    "src/tq/tqRead.c"
    "src/tq/tqOffset.c"
    "src/tq/tqPush.c"
    "src/tq/tqSink.c"
    "src/tq/tqCommit.c"
    "src/tq/tqStreamTask.c"
    "src/tq/tqHandleSnapshot.c"
    "src/tq/tqCheckInfoSnapshot.c"
    "src/tq/tqOffsetSnapshot.c"
    "src/tq/tqStreamStateSnap.c"
    "src/tq/tqStreamTaskSnap.c"
    
)

aux_source_directory("src/tsdb/" TSDB_SOURCE_FILES)
list(
  APPEND
  VNODE_SOURCE_FILES
  ${TSDB_SOURCE_FILES}
)

target_sources(
    vnode
    PRIVATE
    ${VNODE_SOURCE_FILES}
)

IF (TD_VNODE_PLUGINS)
  target_sources(
    vnode
    PRIVATE
    ${TD_ENTERPRISE_DIR}/src/plugins/vnode/src/tsdbCompact.c
    ${TD_ENTERPRISE_DIR}/src/plugins/vnode/src/vnodeCompact.c
  )
ENDIF ()

# IF (NOT ${TD_LINUX})
# target_include_directories(
#     vnode
#     PUBLIC "inc"
#     PUBLIC "src/inc"
#     PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar"
#     PUBLIC "${TD_SOURCE_DIR}/contrib/rocksdb/include"
# )
# ELSE()
# target_include_directories(
#     vnode
#     PUBLIC "inc"
#     PUBLIC "src/inc"
#     PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar"
# )
#ENDIF(NOT ${TD_LINUX})

if (${BUILD_CONTRIB})
    target_include_directories(
        vnode
        PUBLIC "inc"
        PUBLIC "src/inc"
        PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar"
        PUBLIC "${TD_SOURCE_DIR}/contrib/rocksdb/include"
    )
else()
    target_include_directories(
        vnode
        PUBLIC "inc"
        PUBLIC "src/inc"
        PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar"
    )
    if (${TD_LINUX})
        target_include_directories(
            vnode 
            PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
        )
        target_link_directories(
            vnode 
            PUBLIC "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static"
        )
    endif()
endif()

target_link_libraries(
    vnode
    PUBLIC os
    PUBLIC util
    PUBLIC common
    PUBLIC tfs
    PUBLIC wal
    PUBLIC qworker
    PUBLIC sync
    PUBLIC executor
    PUBLIC scheduler
    PUBLIC tdb
    PUBLIC audit

    # PUBLIC bdb
    # PUBLIC scalar
    PUBLIC rocksdb
    PUBLIC transport
    PUBLIC stream
    PUBLIC index
)

if(${BUILD_S3})

if(${BUILD_WITH_S3})
  target_include_directories(
    vnode

    PUBLIC "$ENV{HOME}/.cos-local.2/include"
    )

  set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  set(CMAKE_PREFIX_PATH $ENV{HOME}/.cos-local.2)
  find_library(S3_LIBRARY s3)
  find_library(CURL_LIBRARY curl)
  find_library(SSL_LIBRARY ssl $ENV{HOME}/.cos-local.2/lib64 NO_DEFAULT_PATH)
  find_library(CRYPTO_LIBRARY crypto $ENV{HOME}/.cos-local.2/lib64 NO_DEFAULT_PATH)
  target_link_libraries(
    vnode

    # s3
    PUBLIC ${S3_LIBRARY}
    PUBLIC ${CURL_LIBRARY}
    PUBLIC ${SSL_LIBRARY}
    PUBLIC ${CRYPTO_LIBRARY}
    PUBLIC xml2
  )

  add_definitions(-DUSE_S3)
endif()

if(${BUILD_WITH_COS})

set(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
find_library(APR_LIBRARY apr-1 PATHS /usr/local/apr/lib/)
find_library(APR_UTIL_LIBRARY aprutil-1 PATHS /usr/local/apr/lib/)
find_library(MINIXML_LIBRARY mxml)
find_library(CURL_LIBRARY curl)
target_link_libraries(
    vnode

    # s3
    PUBLIC cos_c_sdk_static
    PUBLIC ${APR_UTIL_LIBRARY}
    PUBLIC ${APR_LIBRARY}
    PUBLIC ${MINIXML_LIBRARY}
    PUBLIC ${CURL_LIBRARY}
)

# s3
FIND_PROGRAM(APR_CONFIG_BIN NAMES apr-config apr-1-config PATHS /usr/bin /usr/local/bin /usr/local/apr/bin/)
IF (APR_CONFIG_BIN)
  EXECUTE_PROCESS(
    COMMAND ${APR_CONFIG_BIN} --includedir
    OUTPUT_VARIABLE APR_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
ENDIF()
include_directories (${APR_INCLUDE_DIR})
target_include_directories(
  vnode
  PUBLIC "${TD_SOURCE_DIR}/contrib/cos-c-sdk-v5/cos_c_sdk"
  PUBLIC "$ENV{HOME}/.cos-local.1/include"
  )

  add_definitions(-DUSE_COS)
endif(${BUILD_WITH_COS})

endif()

IF (TD_GRANT)
  TARGET_LINK_LIBRARIES(vnode PUBLIC grant)
ENDIF ()

target_compile_definitions(vnode PUBLIC -DMETA_REFACT)

if(${BUILD_WITH_INVERTEDINDEX})
    add_definitions(-DUSE_INVERTED_INDEX)
endif(${BUILD_WITH_INVERTEDINDEX})

if(${BUILD_WITH_ROCKSDB})
    add_definitions(-DUSE_ROCKSDB)
endif(${BUILD_WITH_ROCKSDB})

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
