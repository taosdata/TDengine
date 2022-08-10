aux_source_directory(src COMMAND_SRC)
add_library(command STATIC ${COMMAND_SRC})
target_include_directories(
        command
        PUBLIC "${TD_SOURCE_DIR}/include/libs/command"
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
        command
        PRIVATE os util nodes catalog function transport qcom scheduler
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
