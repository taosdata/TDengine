aux_source_directory(src PLANNER_SRC)
add_library(planner STATIC ${PLANNER_SRC})
target_include_directories(
    planner
    PUBLIC "${TD_SOURCE_DIR}/include/libs/planner"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(${FALLBACK})               # {
    target_link_libraries(
        planner
        PRIVATE os util nodes catalog cjson parser function qcom scalar index
        PUBLIC transport
    )
else()                        # }{
    target_link_libraries(
        planner
        PRIVATE os util nodes catalog parser function qcom scalar index
        PUBLIC transport
    )
endif()                       # }

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
