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

ADD_EXECUTABLE(streamTriggerTaskTest streamTriggerTaskTest.cpp)
DEP_ext_gtest(streamTriggerTaskTest)
TARGET_INCLUDE_DIRECTORIES(
  streamTriggerTaskTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamTriggerTaskTest PUBLIC taos os common executor function index new-stream)

add_test(
    NAME streamTriggerTaskTest
    COMMAND streamTriggerTaskTest
)

# v3.4.2 sub-project C DS v6.1: TSDB-data-new helper unit tests.
ADD_EXECUTABLE(streamReaderTsdbV6Test streamReaderTsdbV6Test.cpp)
DEP_ext_gtest(streamReaderTsdbV6Test)
TARGET_INCLUDE_DIRECTORIES(
  streamReaderTsdbV6Test
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PUBLIC "${TD_SOURCE_DIR}/include/common"
)
TARGET_LINK_LIBRARIES(streamReaderTsdbV6Test PUBLIC taos os common executor function index)

add_test(
    NAME streamReaderTsdbV6Test
    COMMAND streamReaderTsdbV6Test
)
endif(NOT ${TD_WINDOWS})
