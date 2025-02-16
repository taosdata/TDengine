if(${BUILD_TEST})
    enable_testing()
    if(NOT ${FALLBACK})      # {
        SET(CMAKE_CXX_STANDARD 14)
    endif()                  # }

    add_subdirectory(qnode)
    add_subdirectory(snode)
    #add_subdirectory(mnode)
    add_subdirectory(vnode)
    add_subdirectory(sut)
endif(${BUILD_TEST})
