aux_source_directory(src SYNC_TEST_SRC)
add_library(sync_test_lib STATIC ${SYNC_TEST_SRC})

target_link_libraries(
    sync_test_lib
    PUBLIC sync
)

target_include_directories(
    sync_test_lib
    PUBLIC "${TD_SOURCE_DIR}/include/libs/sync"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
