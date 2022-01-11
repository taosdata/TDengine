aux_source_directory(src QWORKER_SRC)
#add_library(qworker ${QWORKER_SRC})
#target_include_directories(
#    qworker
#    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/qworker"
#    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
#)
#
#target_link_libraries(
#   qworker
#   PRIVATE os util transport planner qcom executor
#)

add_library(qworker STATIC ${QWORKER_SRC})
target_include_directories(
    qworker
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/qworker"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

#set_target_properties(qworker PROPERTIES
#        IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/libqworker.a"
#        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/include/libs/qworker"
#        )

target_link_libraries(qworker
        PRIVATE os util transport planner qcom executor
        )

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})