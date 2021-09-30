if(${BUILD_TEST})
    add_subdirectory(googletest)
endif(${BUILD_TEST})

add_subdirectory(cJson)
target_include_directories(
    cJSON
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/cJson
)
add_subdirectory(lz4/build/cmake)
add_subdirectory(zlib)
target_include_directories(
    zlib
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
)
