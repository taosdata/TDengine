aux_source_directory(src EXECUTOR_SRC)
#add_library(executor ${EXECUTOR_SRC})

add_library(executor STATIC ${EXECUTOR_SRC})
#set_target_properties(executor PROPERTIES
#        IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/libexecutor.a"
#        INTERFACE_INCLUDE_DIRECTORIES "${TD_SOURCE_DIR}/include/libs/executor"
#        )

target_link_libraries(executor
            PRIVATE os util common function parser planner qcom vnode scalar nodes
        )

target_include_directories(
        executor
        PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

#if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
#endif(${BUILD_TEST})