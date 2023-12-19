# vnode
add_subdirectory(src/tqCommon)
add_library(vnode STATIC "")
set(
  VNODE_SOURCE_FILES
  "src/vnd/vnodeOpen.c"
  "src/vnd/vnodeBufPool.c"
  "src/vnd/vnodeCfg.c"
  "src/vnd/vnodeCommit.c"
  "src/vnd/vnodeQuery.c"
  "src/vnd/vnodeModule.c"
  "src/vnd/vnodeSvr.c"
  "src/vnd/vnodeSync.c"
  "src/vnd/vnodeSnapshot.c"
  "src/vnd/vnodeRetention.c"
  "src/vnd/vnodeInitApi.c"
  "src/vnd/vnodeAsync.c"
  "src/vnd/vnodeHash.c"

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
    ${TD_ENTERPRISE_DIR}/src/plugins/vnode/src/tsdbCompactMonitor.c
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
        PUBLIC "${TD_SOURCE_DIR}/include/dnode/vnode"
        PUBLIC "${TD_SOURCE_DIR}/contrib/rocksdb/include"
    )
else()
    target_include_directories(
        vnode
        PUBLIC "inc"
        PUBLIC "src/inc"
        PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar"
        PUBLIC "${TD_SOURCE_DIR}/include/dnode/vnode"
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
    PUBLIC tqCommon
)

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
