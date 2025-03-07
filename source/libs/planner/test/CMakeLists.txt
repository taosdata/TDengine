
MESSAGE(STATUS "build planner unit test")

IF(NOT TD_DARWIN)
  # GoogleTest requires at least C++11
  SET(CMAKE_CXX_STANDARD 11)
  AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

  ADD_EXECUTABLE(test_planner
    ${SOURCE_LIST}
    "${SOURCE_LIST}/../../../parser/test/mockCatalog.cpp"
    "${SOURCE_LIST}/../../../parser/test/mockCatalogService.cpp"
  )

  IF(TD_GRANT)
    TARGET_LINK_LIBRARIES(
      test_planner
      PUBLIC os util common nodes planner parser catalog transport gtest function qcom grant
    )
  ELSE()
    TARGET_LINK_LIBRARIES(
      test_planner
      PUBLIC os util common nodes planner parser catalog transport gtest function qcom
    )
  ENDIF()

  TARGET_INCLUDE_DIRECTORIES(
    test_planner
    PUBLIC "${TD_SOURCE_DIR}/include/libs/planner/"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/planner/inc"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/test"
  )

  if(${BUILD_WINGETOPT})
    target_include_directories(
      test_planner
      PUBLIC "${TD_SOURCE_DIR}/contrib/wingetopt/src"
    )
    target_link_libraries(test_planner PUBLIC wingetopt)
  endif()

  add_test(
    NAME test_planner
    COMMAND test_planner
  )
ENDIF()