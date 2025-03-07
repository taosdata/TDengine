
MESSAGE(STATUS "build taos-CLI unit test")

IF(NOT TD_DARWIN)
  # GoogleTest requires at least C++11
  SET(CMAKE_CXX_STANDARD 11)
  AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

  ADD_EXECUTABLE(test_shell ${SOURCE_LIST})
  TARGET_LINK_LIBRARIES(
    test_shell
    PRIVATE shell_ut gtest os common transport geometry util
  )

  target_include_directories(
    shell_ut
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
  )

  add_test(
    NAME test_shell
    COMMAND test_shell
  )
ENDIF()
