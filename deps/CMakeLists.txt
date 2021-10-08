if(${BUILD_TEST})
    add_subdirectory(googletest)
endif(${BUILD_TEST})

add_subdirectory(cJson)
target_include_directories(
    cjson
    # see https://stackoverflow.com/questions/25676277/cmake-target-include-directories-prints-an-error-when-i-try-to-add-the-source
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cJson>
)

add_subdirectory(lz4/build/cmake)
target_include_directories(
    lz4_static
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lib
)

add_subdirectory(zlib)
target_include_directories(
    zlib
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
)
