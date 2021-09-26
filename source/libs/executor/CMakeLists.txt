aux_source_directory(src EXECUTOR_SRC)
add_library(executor ${EXECUTOR_SRC})
target_include_directories(
    executor
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/executor"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)