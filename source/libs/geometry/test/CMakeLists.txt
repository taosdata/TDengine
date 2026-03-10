MESSAGE(STATUS "build geometry unit test")

IF(NOT TD_DARWIN)
  # GoogleTest requires at least C++11
  SET(CMAKE_CXX_STANDARD 11)

  AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)
  ADD_EXECUTABLE(geomTest ${SOURCE_LIST})
  DEP_ext_gtest(geomTest)

  TARGET_LINK_LIBRARIES(
    geomTest
    PUBLIC os util qcom nodes geometry scalar function scalar
  )

  add_test(
    NAME geomTest
    COMMAND geomTest
  )
ENDIF()
