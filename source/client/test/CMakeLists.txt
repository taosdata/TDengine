
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(clientTest clientTests.cpp)
TARGET_LINK_LIBRARIES(
        clientTest
        os util common transport parser catalog scheduler gtest ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(connectOptionsTest connectOptionsTest.cpp)
TARGET_LINK_LIBRARIES(
        connectOptionsTest
        os util common transport parser catalog scheduler gtest ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(tmqTest tmqTest.cpp)
TARGET_LINK_LIBRARIES(
        tmqTest
        PUBLIC os util common transport parser catalog scheduler function gtest ${TAOS_NATIVE_LIB_STATIC} qcom
)

ADD_EXECUTABLE(smlTest smlTest.cpp)
TARGET_LINK_LIBRARIES(
        smlTest
        PUBLIC os util common transport parser catalog scheduler function gtest ${TAOS_NATIVE_LIB_STATIC} qcom geometry
)

#ADD_EXECUTABLE(clientMonitorTest clientMonitorTests.cpp)
#TARGET_LINK_LIBRARIES(
#        clientMonitorTest
#        PUBLIC os util common transport monitor parser catalog scheduler function gtest ${TAOS_NATIVE_LIB_STATIC} qcom executor
#)

ADD_EXECUTABLE(userOperTest ../../../tests/script/api/passwdTest.c)
if (TD_WINDOWS)
TARGET_LINK_LIBRARIES(
        userOperTest
        PUBLIC ${TAOS_NATIVE_LIB_STATIC}
)
else()
TARGET_LINK_LIBRARIES(
        userOperTest
	PUBLIC ${TAOS_NATIVE_LIB}
)
endif()

ADD_EXECUTABLE(stmt2Test stmt2Test.cpp)
TARGET_LINK_LIBRARIES(
        stmt2Test
        os util common transport parser catalog scheduler gtest ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(stmtTest stmtTest.cpp)
TARGET_LINK_LIBRARIES(
        stmtTest
        os util common transport parser catalog scheduler gtest ${TAOS_NATIVE_LIB_STATIC} qcom executor function
)

TARGET_INCLUDE_DIRECTORIES(
        clientTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        connectOptionsTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

IF(${TD_LINUX})
        add_test(
                NAME clientTest
                COMMAND clientTest
        )
        add_test(
                NAME connectOptionsTest
                COMMAND connectOptionsTest
        )
        add_test(
                NAME stmt2Test
                COMMAND stmt2Test
        )
        add_test(
                NAME stmtTest
                COMMAND stmtTest
        )
ENDIF ()

TARGET_INCLUDE_DIRECTORIES(
        tmqTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        smlTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

#TARGET_INCLUDE_DIRECTORIES(
#        clientMonitorTest
#        PUBLIC "${TD_SOURCE_DIR}/include/client/"
#        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
#)

TARGET_INCLUDE_DIRECTORIES(
        stmt2Test
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PUBLIC "${TD_SOURCE_DIR}/include/libs/geometry"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
        stmtTest
        PUBLIC "${TD_SOURCE_DIR}/include/client/"
        PUBLIC "${TD_SOURCE_DIR}/include/libs/geometry"
        PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

add_test(
        NAME smlTest
        COMMAND smlTest
)

# add_test(
#         NAME clientMonitorTest
#         COMMAND clientMonitorTest
# )

add_test(
        NAME userOperTest
        COMMAND userOperTest
)
