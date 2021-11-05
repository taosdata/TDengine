aux_source_directory(src META_SRC)
add_library(meta STATIC ${META_SRC})
target_include_directories(
    meta
    PUBLIC "${CMAKE_SOURCE_DIR}/include/server/vnode/meta"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    meta
    PUBLIC common
    PUBLIC tkv
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
