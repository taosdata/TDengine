aux_source_directory(src QWORKER_SRC)

add_library(qworker STATIC ${QWORKER_SRC})
target_include_directories(
    qworker
    PUBLIC "${TD_SOURCE_DIR}/include/libs/qworker"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

TARGET_LINK_LIBRARIES(qworker
        PRIVATE os util transport nodes planner qcom executor index
        )

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
