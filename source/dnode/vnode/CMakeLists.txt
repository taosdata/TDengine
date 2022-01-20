aux_source_directory(src/meta META_SRC)
aux_source_directory(src/tq TQ_SRC)
aux_source_directory(src/tsdb TSDB_SRC)
aux_source_directory(src/vnd VND_SRC)
list(APPEND
    VNODE_SRC
    ${META_SRC}
    ${TQ_SRC}
    ${TSDB_SRC}
    ${VND_SRC}
)

add_library(vnode STATIC ${VNODE_SRC})
target_include_directories(
    vnode
    PUBLIC inc
    PRIVATE src/inc
)
target_link_libraries(
    vnode
    PUBLIC os
    PUBLIC util
    PUBLIC common
    PUBLIC transport
    PUBLIC bdb
    PUBLIC tfs
    PUBLIC wal
    PUBLIC scheduler
    PUBLIC qworker
)

if(${BUILD_TEST})
    # add_subdirectory(test)
endif(${BUILD_TEST})
