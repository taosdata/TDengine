aux_source_directory(src CONFIG_SRC)
add_library(config STATIC ${CONFIG_SRC})
target_include_directories(
    config
    PUBLIC "${CMAKE_SOURCE_DIR}/include/libs/config"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(config os util)

if(${BUILD_TEST})
    ADD_SUBDIRECTORY(test)
endif(${BUILD_TEST})