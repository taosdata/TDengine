set(META_DB_IMPL_LIST "BDB" "SQLITE")
set(META_DB_IMPL "BDB" CACHE STRING "Use BDB as the default META implementation")
set_property(CACHE META_DB_IMPL PROPERTY STRINGS ${META_DB_IMPL_LIST})

if(META_DB_IMPL IN_LIST META_DB_IMPL_LIST)
    message(STATUS "META DB Impl: ${META_DB_IMPL}==============")
else()
    message(FATAL_ERROR "Invalid META DB IMPL: ${META_DB_IMPL}==============")
endif()

aux_source_directory(src META_SRC)
if(${META_DB_IMPL} STREQUAL "BDB")
    list(REMOVE_ITEM META_SRC "src/metaSQLiteImpl.c")
elseif(${META_DB_IMPL} STREQUAL "SQLITE")
    list(REMOVE_ITEM META_SRC "src/metaBDBImpl.c")
endif()

message("${META_SRC}")

add_library(meta STATIC ${META_SRC})
target_include_directories(
    meta
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/vnode/meta"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    meta
    PUBLIC common
)

if(${META_DB_IMPL} STREQUAL "BDB")
    target_link_libraries(
        meta
        PUBLIC bdb
    )
elseif(${META_DB_IMPL} STREQUAL "SQLITE")
    target_link_libraries(
        meta
        PUBLIC sqlite
    )
endif()


if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
