aux_source_directory(src SCHEDULER_SRC)
add_library(scheduler STATIC ${SCHEDULER_SRC})

target_include_directories(
        scheduler
        PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/scheduler"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        scheduler
        PUBLIC os util planner qcom common catalog transport
)

if(${BUILD_TEST})
    IF (NOT TD_DARWIN)
        ADD_SUBDIRECTORY(test)
    ENDIF ()
endif(${BUILD_TEST})
