aux_source_directory(src TSDB_SRC)
if(0)
    add_library(tsdb ${TSDB_SRC})
else(0)
    add_library(tsdb "")
    target_sources(tsdb
        PRIVATE
        "src/tsdbCommit.c"
        "src/tsdbMain.c"
        "src/tsdbMemTable.c"
        "src/tsdbOptions.c"
        "src/tsdbWrite.c"
        "src/tsdbReadImpl.c"
        "src/tsdbFile.c"
        "src/tsdbFS.c"
    )
endif(0)

target_include_directories(
    tsdb
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/vnode/tsdb"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    tsdb
    PUBLIC os
    PUBLIC util
    PUBLIC common
    PUBLIC tkv
    PUBLIC tfs
    PUBLIC meta
)