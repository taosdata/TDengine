
set(TDB_SUBDIRS "db")
foreach(TDB_SUBDIR ${TDB_SUBDIRS})
    aux_source_directory("src/${TDB_SUBDIR}" TDB_SRC)
endforeach()

add_library(tdb STATIC ${TDB_SRC})

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

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
