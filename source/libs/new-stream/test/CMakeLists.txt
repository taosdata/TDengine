MESSAGE(STATUS "build new-stream unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

if(NOT ${TD_WINDOWS})
ADD_EXECUTABLE(streamRunnerTest streamRunnerTest.cpp)
DEP_ext_gtest(streamRunnerTest)
DEP_ext_cppstub(streamRunnerTest)

TARGET_INCLUDE_DIRECTORIES(
  streamRunnerTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

TARGET_LINK_LIBRARIES(
  streamRunnerTest
  PUBLIC taos os common executor function index new-stream
)

add_test(
  NAME streamRunnerTest
  COMMAND streamRunnerTest
)

ADD_EXECUTABLE(streamNotifyTest streamNotifyTest.cpp)
DEP_ext_gtest(streamNotifyTest)
DEP_ext_cppstub(streamNotifyTest)
DEP_ext_ssl(streamNotifyTest)

TARGET_INCLUDE_DIRECTORIES(
  streamNotifyTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)

TARGET_LINK_LIBRARIES(
  streamNotifyTest
  PUBLIC taos os common executor function index new-stream
)

add_test(
  NAME streamNotifyTest
  COMMAND streamNotifyTest
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
DEP_ext_cppstub(streamTriggerTaskTest)
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

ADD_EXECUTABLE(streamTaskStatsTest streamTaskStatsTest.cpp)
DEP_ext_gtest(streamTaskStatsTest)
DEP_ext_cppstub(streamTaskStatsTest)
TARGET_INCLUDE_DIRECTORIES(
  streamTaskStatsTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/executor"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/qcom"
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamTaskStatsTest PUBLIC taos os common executor function index new-stream)
add_test(NAME streamTaskStatsTest COMMAND streamTaskStatsTest)

ADD_EXECUTABLE(streamRecalcTrackerTest streamRecalcTrackerTest.cpp)
DEP_ext_gtest(streamRecalcTrackerTest)
TARGET_INCLUDE_DIRECTORIES(
  streamRecalcTrackerTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamRecalcTrackerTest PUBLIC taos os common new-stream)
add_test(NAME streamRecalcTrackerTest COMMAND streamRecalcTrackerTest)

ADD_EXECUTABLE(streamRecalcTrackerFallbackTest streamRecalcTrackerTest.cpp ../src/streamRecalcTracker.c)
TARGET_COMPILE_DEFINITIONS(streamRecalcTrackerFallbackTest PRIVATE STREAM_RECALC_FORCE_WIDE_FALLBACK)
DEP_ext_gtest(streamRecalcTrackerFallbackTest)
TARGET_INCLUDE_DIRECTORIES(
  streamRecalcTrackerFallbackTest
  PUBLIC "${TD_SOURCE_DIR}/include/libs/new-stream"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
TARGET_LINK_LIBRARIES(streamRecalcTrackerFallbackTest PUBLIC taos os common new-stream)
add_test(NAME streamRecalcTrackerFallbackTest COMMAND streamRecalcTrackerFallbackTest)
endif(NOT ${TD_WINDOWS})
