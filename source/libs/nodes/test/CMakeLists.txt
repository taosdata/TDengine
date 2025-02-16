
MESSAGE(STATUS "build nodes unit test")

IF(NOT TD_DARWIN)
    # GoogleTest requires at least C++11
    if(${FALLBACK})          # {
        SET(CMAKE_CXX_STANDARD 11)
    else()                   # }{
        SET(CMAKE_CXX_STANDARD 14)
    endif()                  # }
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
    
    ADD_EXECUTABLE(nodesTest ${SOURCE_LIST})
    
    TARGET_INCLUDE_DIRECTORIES(
        nodesTest
        PUBLIC "${TD_SOURCE_DIR}/include/nodes/"
        PRIVATE "${TD_SOURCE_DIR}/source/nodes/inc"
    )
    
    if(${FALLBACK})                            # {
        TARGET_LINK_LIBRARIES(
            nodesTest
            PUBLIC os util common nodes qcom gtest
        )
    else()                                     # }{
        TARGET_LINK_LIBRARIES(
            nodesTest
            PUBLIC os util common nodes qcom
        )
        DEP_ext_gtest(nodesTest)
    endif()                                    # }
    
    add_test(
        NAME nodesTest
        COMMAND nodesTest
    )
ENDIF()
