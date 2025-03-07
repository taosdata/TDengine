
MESSAGE(STATUS "build scheduler unit test")

IF(NOT TD_DARWIN)
  # GoogleTest requires at least C++11
  SET(CMAKE_CXX_STANDARD 11)
  AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

  ADD_EXECUTABLE(test_scheduler ${SOURCE_LIST})

  IF(TD_GRANT)
    TARGET_LINK_LIBRARIES(
      test_scheduler
      PUBLIC os util common catalog transport gtest qcom ${TAOS_LIB_STATIC} planner scheduler grant
    )
  ELSE()
    TARGET_LINK_LIBRARIES(
      test_scheduler
      PUBLIC os util common catalog transport gtest qcom ${TAOS_LIB_STATIC} planner scheduler
    )
  ENDIF()

  TARGET_INCLUDE_DIRECTORIES(
    test_scheduler
    PUBLIC "${TD_SOURCE_DIR}/include/libs/scheduler/"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/scheduler/inc"
  )
  add_test(
    NAME test_scheduler
    COMMAND test_scheduler
  )
ENDIF()