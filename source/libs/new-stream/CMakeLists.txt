aux_source_directory(src STREAM_SRC)
add_library(new-stream STATIC ${STREAM_SRC})

target_include_directories(
    new-stream
    PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
    PUBLIC "${TD_SOURCE_DIR}/include/libs/tcs"
    PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    new-stream
    PUBLIC tdb tcs
    PRIVATE os util transport qcom executor wal index
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
