aux_source_directory(src AZ_SRC)

add_library(az STATIC ${AZ_SRC})
target_include_directories(
    az
    PUBLIC "${TD_SOURCE_DIR}/include/libs/azure"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    az
    PUBLIC _azure_sdk
    PUBLIC cjson
    PUBLIC os
    PUBLIC util
    PUBLIC common
    PUBLIC crypt
)

if(${BUILD_TEST})
    add_subdirectory(test)
endif(${BUILD_TEST})
