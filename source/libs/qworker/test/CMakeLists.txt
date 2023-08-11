
MESSAGE(STATUS "build qworker unit test")
IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(qworkerTest ${SOURCE_LIST})
        TARGET_LINK_LIBRARIES(
                qworkerTest
                PUBLIC os util common transport gtest qcom nodes planner qworker executor index
        )

        TARGET_INCLUDE_DIRECTORIES(
                qworkerTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/qworker/"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/qworker/inc"
        )
ENDIF()
