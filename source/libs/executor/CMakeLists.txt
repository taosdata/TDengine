aux_source_directory(src EXECUTOR_SRC)

add_library(executor STATIC ${EXECUTOR_SRC})
if(${TD_DARWIN})
    target_compile_options(executor PRIVATE -Wno-error=deprecated-non-prototype)
endif(${TD_DARWIN})

target_link_libraries(executor
        PRIVATE os util common function parser planner qcom scalar nodes index wal tdb geometry
        )

target_include_directories(
        executor
        PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
