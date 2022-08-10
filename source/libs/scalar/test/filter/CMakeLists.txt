
MESSAGE(STATUS "build filter unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(filterTest ${SOURCE_LIST})
        TARGET_LINK_LIBRARIES(
                filterTest
                PUBLIC os util common gtest qcom function nodes scalar parser catalog transport
        )

        TARGET_INCLUDE_DIRECTORIES(
                filterTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar/"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/scalar/inc"
        )
ENDIF()
