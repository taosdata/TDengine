set(META_DB_IMPL_LIST "BDB" "TDB")
set(META_DB_IMPL "BDB" CACHE STRING "Use BDB as the default META implementation")
set_property(CACHE META_DB_IMPL PROPERTY STRINGS ${META_DB_IMPL_LIST})

if(META_DB_IMPL IN_LIST META_DB_IMPL_LIST)
    message(STATUS "META DB Impl: ${META_DB_IMPL}==============")
else()
    message(FATAL_ERROR "Invalid META DB IMPL: ${META_DB_IMPL}==============")
endif()

aux_source_directory(src/meta META_SRC)
if(${META_DB_IMPL} STREQUAL "BDB")
    list(REMOVE_ITEM META_SRC "src/meta/metaTDBImpl.c")
elseif(${META_DB_IMPL} STREQUAL "TDB")
    list(REMOVE_ITEM META_SRC "src/meta/metaBDBImpl.c")
endif()

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
    PUBLIC tfs
    PUBLIC wal
    PUBLIC scheduler
    PUBLIC executor
    PUBLIC stream
    PUBLIC qworker
    PUBLIC sync
)

if(${META_DB_IMPL} STREQUAL "BDB")
    target_link_libraries(vnode PUBLIC bdb)
elseif(${META_DB_IMPL} STREQUAL "TDB")
    target_link_libraries(vnode PUBLIC tdb)
endif()

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
