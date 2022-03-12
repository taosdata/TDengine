aux_source_directory(src CLIENT_SRC)
add_library(taos ${CLIENT_SRC})
target_include_directories(
    taos
    PUBLIC "${CMAKE_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    taos
    INTERFACE api
    PRIVATE os util common transport nodes parser planner catalog scheduler function qcom
)

if(${BUILD_TEST})
    if (${TD_LINUX})
        ADD_SUBDIRECTORY(test)
    endif()
endif(${BUILD_TEST})
