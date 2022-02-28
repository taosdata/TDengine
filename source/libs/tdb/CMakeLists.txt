# tdb
add_library(tdb "")
target_sources(tdb
    PRIVATE
    "src/db/tdbPCache.c"
    "src/db/tdbPFile.c"
    "src/db/tdbUtil.c"
    "src/db/tdbBtree.c"
    "src/db/tdbDb.c"
    "src/db/tdbEnv.c"
)

target_include_directories(
    tdb
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src/inc"
)
target_link_libraries(
    tdb
    PUBLIC os
    PUBLIC util
)

# for tdb_sqlite
add_library(tdb_sqlite "")
target_sources(tdb_sqlite
    PRIVATE
    "src/sqlite/pcache.c"
    "src/sqlite/pcache1.c"
    "src/sqlite/pager.c"
)
target_include_directories(tdb_sqlite PUBLIC "src/sqliteinc")

# for test
if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
