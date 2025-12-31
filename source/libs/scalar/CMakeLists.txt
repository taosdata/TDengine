aux_source_directory(src SCALAR_SRC)

add_library(scalar STATIC ${SCALAR_SRC})
target_include_directories(
    scalar
    PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/totp/inc"
)

target_link_libraries(scalar
    PRIVATE os
    PRIVATE util
    PRIVATE common
    PRIVATE nodes
    PRIVATE function
    PRIVATE qcom
    PRIVATE parser
    PRIVATE geometry
    PRIVATE decimal
    PRIVATE totp
)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})
