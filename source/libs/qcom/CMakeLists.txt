aux_source_directory(src QUERY_SRC)
add_library(qcom STATIC ${QUERY_SRC})
target_include_directories(
    qcom
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/qcom"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    qcom
    PRIVATE os util transport
)

if(${BUILD_TEST})
    if (${TD_LINUX})
        ADD_SUBDIRECTORY(test)
    endif()
endif(${BUILD_TEST})
