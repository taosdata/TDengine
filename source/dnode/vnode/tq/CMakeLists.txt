aux_source_directory(src TQ_SRC)
add_library(tq ${TQ_SRC})
target_include_directories(
    tq
    PUBLIC "${CMAKE_SOURCE_DIR}/include/dnode/vnode/tq"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    tq
    PUBLIC wal
    PUBLIC os
    PUBLIC util
    PUBLIC common
    PUBLIC transport
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
