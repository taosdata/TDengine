if(${BUILD_TEST})
    add_subdirectory(googletest)
endif(${BUILD_TEST})

add_subdirectory(cJson)
add_subdirectory(lz4/build/cmake)
