# vnode
add_library(vnode STATIC "")
target_sources(
    vnode
    PRIVATE
    # vnode
    "src/vnd/vnodeOpen.c"
    "src/vnd/vnodeArenaMAImpl.c"
    "src/vnd/vnodeBufferPool.c"
    # "src/vnd/vnodeBufferPool2.c"
    "src/vnd/vnodeCfg.c"
    "src/vnd/vnodeCommit.c"
    "src/vnd/vnodeInt.c"
    "src/vnd/vnodeQuery.c"
    "src/vnd/vnodeStateMgr.c"
    "src/vnd/vnodeModule.c"
    "src/vnd/vnodeSvr.c"

    # meta
    "src/meta/metaOpen.c"
    "src/meta/metaIdx.c"
    "src/meta/metaTable.c"
    "src/meta/metaTDBImpl.c"
    # "src/meta/metaBDBImpl.c"

    # tsdb
    "src/tsdb/tsdbTDBImpl.c"
    "src/tsdb/tsdbCommit.c"
    "src/tsdb/tsdbCompact.c"
    "src/tsdb/tsdbFile.c"
    "src/tsdb/tsdbFS.c"
    "src/tsdb/tsdbMain.c"
    "src/tsdb/tsdbMemTable.c"
    "src/tsdb/tsdbOptions.c"
    "src/tsdb/tsdbRead.c"
    "src/tsdb/tsdbReadImpl.c"
    "src/tsdb/tsdbScan.c"
    "src/tsdb/tsdbSma.c"
    "src/tsdb/tsdbWrite.c"

    # tq
    "src/tq/tq.c"
    "src/tq/tqCommit.c"
    "src/tq/tqMetaStore.c"
    "src/tq/tqOffset.c"
    "src/tq/tqPush.c"
    "src/tq/tqRead.c"
)
target_include_directories(
    vnode
    PUBLIC "inc"
    PRIVATE "src/inc"
    PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar"
    
)
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
    #PUBLIC bdb
    #PUBLIC scalar
    PUBLIC transport
    PUBLIC stream
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
