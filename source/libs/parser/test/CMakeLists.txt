
MESSAGE(STATUS "build parser unit test")

IF(NOT TD_DARWIN)
  # GoogleTest requires at least C++11
  SET(CMAKE_CXX_STANDARD 11)
  AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

  ADD_EXECUTABLE(test_parser ${SOURCE_LIST})

  TARGET_INCLUDE_DIRECTORIES(
    test_parser
    PUBLIC "${TD_SOURCE_DIR}/include/libs/parser/"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/parser/inc"
  )

  TARGET_LINK_LIBRARIES(
    test_parser
    PUBLIC os util common nodes parser catalog transport gtest function planner qcom
  )

  if(${BUILD_WINGETOPT})
    target_include_directories(
      test_parser
      PUBLIC "${TD_SOURCE_DIR}/contrib/wingetopt/src"
    )
    target_link_libraries(test_parser PUBLIC wingetopt)
  endif()

  add_test(
    NAME test_parser
    COMMAND test_parser
  )
ENDIF()