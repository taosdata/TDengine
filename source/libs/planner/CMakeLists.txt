aux_source_directory(src PLANNER_SRC)
add_library(planner STATIC ${PLANNER_SRC})
target_include_directories(
    planner
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/planner"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    planner
    PRIVATE os util nodes catalog cjson parser function qcom
    PUBLIC transport
)

if(${BUILD_TEST})
    if(${TD_LINUX})
        ADD_SUBDIRECTORY(test)
    endif()
endif(${BUILD_TEST})
