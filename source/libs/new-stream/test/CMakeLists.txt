MESSAGE(STATUS "build new-stream unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

if(NOT ${TD_WINDOWS})
ADD_EXECUTABLE(streamRunnerTest streamRunnerTest.cpp)
DEP_ext_gtest(streamRunnerTest)

TARGET_INCLUDE_DIRECTORIES(
  streamRunnerTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
)

TARGET_LINK_LIBRARIES(
  streamRunnerTest
  PUBLIC taos os common executor function index
)

add_test(
  NAME streamRunnerTest
  COMMAND streamRunnerTest
)

ADD_EXECUTABLE(dataSinkTest dataSinkTest.cpp)
DEP_ext_gtest(dataSinkTest)
TARGET_LINK_LIBRARIES(dataSinkTest PRIVATE os util ${TAOS_NATIVE_LIB_STATIC} common pthread new-stream)

add_test(
    NAME dataSinkTest
    COMMAND dataSinkTest
)

ADD_EXECUTABLE(checkpointEncryptionTest checkpointEncryptionTest.cpp)
DEP_ext_gtest(checkpointEncryptionTest)
TARGET_INCLUDE_DIRECTORIES(
  checkpointEncryptionTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
)
TARGET_LINK_LIBRARIES(checkpointEncryptionTest PUBLIC taos os common executor function index)

add_test(
    NAME checkpointEncryptionTest
    COMMAND checkpointEncryptionTest
)

ADD_EXECUTABLE(streamTriggerTest streamTriggerTest.cpp)
DEP_ext_gtest(streamTriggerTest)
TARGET_INCLUDE_DIRECTORIES(
  streamTriggerTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamTriggerTest PUBLIC taos os common executor function index new-stream)

add_test(
    NAME streamTriggerTest
    COMMAND streamTriggerTest
)
endif(NOT ${TD_WINDOWS})
