
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)
AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

ADD_EXECUTABLE(test_client clientTests.cpp)
TARGET_LINK_LIBRARIES(
  test_client
  PUBLIC sut
)

ADD_EXECUTABLE(test_connect_options connectOptionsTest.cpp)
TARGET_LINK_LIBRARIES(
  test_connect_options
  PUBLIC sut
)

ADD_EXECUTABLE(test_tmq tmqTest.cpp)
TARGET_LINK_LIBRARIES(
  test_tmq
  PUBLIC sut
)

ADD_EXECUTABLE(test_sml smlTest.cpp)
TARGET_LINK_LIBRARIES(
  test_sml
  PUBLIC sut
)

# ADD_EXECUTABLE(test_clientmonitor clientMonitorTests.cpp)
# TARGET_LINK_LIBRARIES(
# test_clientmonitor
# PUBLIC sut
# )
ADD_EXECUTABLE(test_useroper ../../../tests/script/api/passwdTest.c)
TARGET_LINK_LIBRARIES(
  test_useroper
  PUBLIC ${TAOS_LIB}
)

ADD_EXECUTABLE(test_stmt2 stmt2Test.cpp)
TARGET_LINK_LIBRARIES(
  test_stmt2
  os util common transport parser catalog scheduler gtest ${TAOS_LIB_STATIC} qcom executor function
)

ADD_EXECUTABLE(test_stmt stmtTest.cpp)
TARGET_LINK_LIBRARIES(
  test_stmt
  os util common transport parser catalog scheduler gtest ${TAOS_LIB_STATIC} qcom executor function
)

TARGET_INCLUDE_DIRECTORIES(
  test_client
  PUBLIC "${TD_SOURCE_DIR}/include/client/"
  PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
  test_connect_options
  PUBLIC "${TD_SOURCE_DIR}/include/client/"
  PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

IF(${TD_LINUX})
  add_test(
    NAME test_client
    COMMAND test_client
  )
  add_test(
    NAME test_connect_options
    COMMAND test_connect_options
  )
  add_test(
    NAME test_stmt2
    COMMAND test_stmt2
  )
  add_test(
    NAME test_stmt
    COMMAND test_stmt
  )
ENDIF()

TARGET_INCLUDE_DIRECTORIES(
  test_tmq
  PUBLIC "${TD_SOURCE_DIR}/include/client/"
  PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
  test_sml
  PUBLIC "${TD_SOURCE_DIR}/include/client/"
  PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

# TARGET_INCLUDE_DIRECTORIES(
# test_clientmonitor
# PUBLIC "${TD_SOURCE_DIR}/include/client/"
# PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
# )
TARGET_INCLUDE_DIRECTORIES(
  test_stmt2
  PUBLIC "${TD_SOURCE_DIR}/include/client/"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/geometry"
  PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

TARGET_INCLUDE_DIRECTORIES(
  test_stmt
  PUBLIC "${TD_SOURCE_DIR}/include/client/"
  PUBLIC "${TD_SOURCE_DIR}/include/libs/geometry"
  PRIVATE "${TD_SOURCE_DIR}/source/client/inc"
)

add_test(
  NAME test_sml
  COMMAND test_sml
)

# add_test(
# NAME test_clientmonitor
# COMMAND test_clientmonitor
# )
add_test(
  NAME test_useroper
  COMMAND test_useroper
)