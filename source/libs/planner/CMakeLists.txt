aux_source_directory(src PLANNER_SRC)
add_library(planner ${PLANNER_SRC})
target_include_directories(
    planner
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/planner"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    planner
    PRIVATE os util common catalog parser transport function
)