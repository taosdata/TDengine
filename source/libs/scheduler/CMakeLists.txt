aux_source_directory(src SCHEDULER_SRC)
add_library(scheduler STATIC ${SCHEDULER_SRC})

target_include_directories(
        scheduler
        PUBLIC "${TD_SOURCE_DIR}/include/libs/scheduler"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        scheduler
        PUBLIC os util nodes planner qcom common catalog transport command qworker executor index
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
