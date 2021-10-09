aux_source_directory(src SCHEDULER_SRC)
add_library(scheduler ${SCHEDULER_SRC})

target_include_directories(
        scheduler
        PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/scheduler"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        scheduler
        PRIVATE os util planner
)