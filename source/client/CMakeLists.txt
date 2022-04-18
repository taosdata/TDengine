aux_source_directory(src CLIENT_SRC)
add_library(taos SHARED ${CLIENT_SRC})
target_include_directories(
    taos
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    taos
    INTERFACE api
    PRIVATE os util common transport nodes parser command planner catalog scheduler function qcom
)

set_target_properties(
    taos
    PROPERTIES
    VERSION ${TD_VER_NUMBER}
    SOVERSION ${TD_VER_NUMBER}
)

add_library(taos_static STATIC ${CLIENT_SRC})
target_include_directories(
    taos_static
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    taos_static
    INTERFACE api
    PRIVATE os util common transport nodes parser command planner catalog scheduler function qcom
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})