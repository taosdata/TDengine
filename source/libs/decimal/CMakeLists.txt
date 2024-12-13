aux_source_directory(src DECIMAL_SRC)

add_library(decimal STATIC ${DECIMAL_SRC})

target_include_directories(
    decimal
    PUBLIC "${TD_SOURCE_DIR}/include/libs/decimal"
)
target_link_libraries(
    decimal
    PRIVATE os util common qcom nodes
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
