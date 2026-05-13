aux_source_directory(src AES_SRC)

add_library(aes STATIC ${AES_SRC})

#target_compile_definitions(aes PRIVATE WITH_BROKER)

#if(${TD_DARWIN})
#    target_compile_options(aes PRIVATE -Wno-error=deprecated-non-prototype)
#endif()

target_include_directories(
    aes
    PUBLIC "${TD_SOURCE_DIR}/include/libs/taes"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
