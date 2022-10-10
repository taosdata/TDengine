aux_source_directory(src SYNC_SRC)
add_library(sync STATIC ${SYNC_SRC})

target_link_libraries(
    sync
    PUBLIC common
    PUBLIC transport
    PUBLIC util
    PUBLIC wal
)

target_include_directories(
    sync
    PUBLIC "${TD_SOURCE_DIR}/include/libs/sync"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(BUILD_TEST AND BUILD_SYNC_TEST)
    add_subdirectory(test)
endif()
