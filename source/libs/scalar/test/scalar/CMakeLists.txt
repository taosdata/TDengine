
MESSAGE(STATUS "build scalar unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(scalarTest ${SOURCE_LIST})
        TARGET_LINK_LIBRARIES(
                scalarTest
                PUBLIC os util common gtest qcom function nodes scalar parser catalog transport
        )

        TARGET_INCLUDE_DIRECTORIES(
                scalarTest
                PUBLIC "${TD_SOURCE_DIR}/include/libs/scalar/"
                PUBLIC "${TD_SOURCE_DIR}/source/libs/parser/inc"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/scalar/inc"
        )
        add_test(
                NAME scalarTest
                COMMAND scalarTest
        )
ENDIF()
