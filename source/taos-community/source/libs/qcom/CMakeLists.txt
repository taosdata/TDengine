aux_source_directory(src QUERY_SRC)
add_library(qcom STATIC ${QUERY_SRC})
target_include_directories(
    qcom
    PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    qcom
    PRIVATE os util transport
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
