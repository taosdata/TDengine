
MESSAGE(STATUS "build catalog unit test")

IF(NOT TD_DARWIN)
  # GoogleTest requires at least C++11
  SET(CMAKE_CXX_STANDARD 11)
  AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

  ADD_EXECUTABLE(test_catalog ${SOURCE_LIST})
  TARGET_LINK_LIBRARIES(
    test_catalog
    PUBLIC os util common nodes catalog transport gtest qcom ${TAOS_LIB_STATIC}
  )

  TARGET_INCLUDE_DIRECTORIES(
    test_catalog
    PUBLIC "${TD_SOURCE_DIR}/include/libs/catalog/"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/catalog/inc"
  )

  add_test(
    NAME test_catalog
    COMMAND test_catalog
  )
ENDIF()
