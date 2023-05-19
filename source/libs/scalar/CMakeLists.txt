aux_source_directory(src SCALAR_SRC)

add_library(scalar STATIC ${SCALAR_SRC})
target_include_directories(
    scalar
    PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar"
    PRIVATE "${TD_SOURCE_DIR}/include/common" 
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(scalar
    PRIVATE os 
    PRIVATE util 
    PRIVATE nodes
    PRIVATE function
    PUBLIC common
    PRIVATE qcom
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
