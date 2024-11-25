aux_source_directory(src WRAPPER_SRC)
add_library(wrapper STATIC ${WRAPPER_SRC})

target_include_directories(
    wrapper
    PUBLIC "${TD_SOURCE_DIR}/include/client"
    PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    wrapper
    PUBLIC os util transport geometry
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
