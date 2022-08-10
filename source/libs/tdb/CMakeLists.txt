# tdb
add_library(tdb STATIC "")
target_sources(tdb
    PRIVATE
    "src/db/tdbPCache.c"
    "src/db/tdbPager.c"
    "src/db/tdbUtil.c"
    "src/db/tdbBtree.c"
    "src/db/tdbDb.c"
    "src/db/tdbTable.c"
    "src/db/tdbTxn.c"
    "src/db/tdbPage.c"
    "src/db/tdbOs.c"
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

# for test
if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
