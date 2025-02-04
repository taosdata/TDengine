
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(clientTest clientTests.cpp)
if(${FALLBACK})                  # {
        TARGET_LINK_LIBRARIES(
                clientTest
                os util common transport parser catalog scheduler gtest ${TAOS_LIB_STATIC} qcom executor function
        )
else()                           # }{
        TARGET_LINK_LIBRARIES(
                clientTest
                PUBLIC os util common transport parser catalog scheduler ${TAOS_LIB_STATIC} qcom executor function
        )
        DEP_ext_gtest(clientTest)
        DEP_ext_cjson_LIB(clientTest)
endif()                          # }

ADD_EXECUTABLE(connectOptionsTest connectOptionsTest.cpp)
if(${FALLBACK})                  # {
        TARGET_LINK_LIBRARIES(
                connectOptionsTest
                os util common transport parser catalog scheduler gtest ${TAOS_LIB_STATIC} qcom executor function
        )
else()                           # }{
        TARGET_LINK_LIBRARIES(
                connectOptionsTest
                PUBLIC os util common transport parser catalog scheduler ${TAOS_LIB_STATIC} qcom executor function
        )
        DEP_ext_gtest(connectOptionsTest)
        DEP_ext_cjson_LIB(connectOptionsTest)
endif()                          # }

ADD_EXECUTABLE(tmqTest tmqTest.cpp)
if(${FALLBACK})                  # {
        TARGET_LINK_LIBRARIES(
                tmqTest
                PUBLIC os util common transport parser catalog scheduler function gtest ${TAOS_LIB_STATIC} qcom
        )
else()                           # }{
        TARGET_LINK_LIBRARIES(
                tmqTest
                PUBLIC os util common transport parser catalog scheduler function ${TAOS_LIB_STATIC} qcom
        )
        DEP_ext_gtest(tmqTest)
        DEP_ext_cjson_LIB(tmqTest)
endif()                          # }

ADD_EXECUTABLE(smlTest smlTest.cpp)
if(${FALLBACK})                  # {
        TARGET_LINK_LIBRARIES(
                smlTest
                PUBLIC os util common transport parser catalog scheduler function gtest ${TAOS_LIB_STATIC} qcom geometry
        )
else()                           # }{
        TARGET_LINK_LIBRARIES(
                smlTest
                PUBLIC os util common transport parser catalog scheduler function ${TAOS_LIB_STATIC} qcom geometry
        )
        DEP_ext_gtest(smlTest)
        DEP_ext_cjson_LIB(smlTest)
endif()                          # }

#ADD_EXECUTABLE(clientMonitorTest clientMonitorTests.cpp)
#TARGET_LINK_LIBRARIES(
#        clientMonitorTest
#        PUBLIC os util common transport monitor parser catalog scheduler function gtest ${TAOS_LIB_STATIC} qcom executor
#)

ADD_EXECUTABLE(userOperTest ../../../tests/script/api/passwdTest.c)
TARGET_LINK_LIBRARIES(
        userOperTest
        PUBLIC ${TAOS_LIB}
)

ADD_EXECUTABLE(stmt2Test stmt2Test.cpp)
if(${FALLBACK})                  # {
        TARGET_LINK_LIBRARIES(
                stmt2Test
                os util common transport parser catalog scheduler gtest ${TAOS_LIB_STATIC} qcom executor function
        )
else()                           # }{
        TARGET_LINK_LIBRARIES(
                stmt2Test
                PUBLIC os util common transport parser catalog scheduler ${TAOS_LIB_STATIC} qcom executor function
        )
        DEP_ext_gtest(stmt2Test)
        DEP_ext_cjson_LIB(stmt2Test)
endif()                          # }

ADD_EXECUTABLE(stmtTest stmtTest.cpp)
if(${FALLBACK})                  # {
        TARGET_LINK_LIBRARIES(
                stmtTest
                os util common transport parser catalog scheduler gtest ${TAOS_LIB_STATIC} qcom executor function
        )
else()                           # }{
        TARGET_LINK_LIBRARIES(
                stmtTest
                PUBLIC os util common transport parser catalog scheduler ${TAOS_LIB_STATIC} qcom executor function
        )
        DEP_ext_gtest(stmtTest)
        DEP_ext_cjson_LIB(stmtTest)
endif()                          # }

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