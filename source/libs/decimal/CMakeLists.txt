aux_source_directory(src DECIMAL_SRC)

add_library(decimal STATIC ${DECIMAL_SRC})

target_include_directories(
    decimal
    PUBLIC "${TD_SOURCE_DIR}/include/libs/decimal"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
    decimal
    PRIVATE os common wideInteger
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
ADD_SUBDIRECTORY(src/detail)
