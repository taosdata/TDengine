aux_source_directory(src STREAM_SRC)
add_library(stream STATIC ${STREAM_SRC})
target_include_directories(
    stream
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/stream"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    stream
    PRIVATE os util transport qcom executor
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
