aux_source_directory(src SCHEMALESS_SRC)
add_library(schemaless STATIC ${SCHEMALESS_SRC})

target_include_directories(
        schemaless
        PUBLIC "${TD_SOURCE_DIR}/include/libs/schemaless"
        PRIVATE "inc"
)

target_link_libraries(
        schemaless
        PUBLIC os util common catalog qcom
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
