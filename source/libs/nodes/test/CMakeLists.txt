
MESSAGE(STATUS "build nodes unit test")

IF(NOT TD_DARWIN)
    # GoogleTest requires at least C++11
    SET(CMAKE_CXX_STANDARD 11)
    AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
    
    ADD_EXECUTABLE(nodesTest ${SOURCE_LIST})
    
    TARGET_INCLUDE_DIRECTORIES(
        nodesTest
        PUBLIC "${TD_SOURCE_DIR}/include/nodes/"
        PRIVATE "${TD_SOURCE_DIR}/source/nodes/inc"
    )
    
    TARGET_LINK_LIBRARIES(
        nodesTest
        PUBLIC os util common nodes qcom gtest
    )
    
    add_test(
        NAME nodesTest
        COMMAND nodesTest
    )
ENDIF()
