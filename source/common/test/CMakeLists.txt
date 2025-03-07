
MESSAGE(STATUS "build parser unit test")

# GoogleTest requires at least C++11
SET(CMAKE_CXX_STANDARD 11)

if(TD_LINUX)
  add_executable(test_common "commonTests.cpp")
  target_link_libraries(test_common os util common gtest_main)
  target_include_directories(
    test_common
    PUBLIC "${TD_SOURCE_DIR}/include/libs/common/"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/common/inc"
  )
  add_test(
    NAME test_common
    COMMAND test_common
  )
endif()

# test_dataformat.cpp
add_executable(test_dataformat "dataformatTest.cpp")
target_link_libraries(test_dataformat gtest gtest_main util common)
target_include_directories(
  test_dataformat
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PUBLIC "${TD_SOURCE_DIR}/include/util"
)
add_test(
  NAME test_dataformat
  COMMAND test_dataformat
)

# cosCpTest.cpp
add_executable(test_coscp "cosCpTest.cpp")
target_link_libraries(test_coscp gtest gtest_main util common)
target_include_directories(
  test_coscp
  PUBLIC "${TD_SOURCE_DIR}/include/common"
  PUBLIC "${TD_SOURCE_DIR}/include/util"
)
add_test(
  NAME test_coscp
  COMMAND test_coscp
)

if(TD_LINUX)
  # cosTest.cpp
  add_executable(test_cos "cosTest.cpp")
  target_link_libraries(test_cos gtest gtest_main util common)
  target_include_directories(
    test_cos
    PUBLIC "${TD_SOURCE_DIR}/include/common"
    PUBLIC "${TD_SOURCE_DIR}/include/util"
  )
  add_test(
    NAME test_cos
    COMMAND test_cos
  )
endif()

if(${TD_LINUX})
  # tmsg test
  add_executable(test_tmsg "")
  target_sources(test_tmsg
    PRIVATE
    "tmsgTest.cpp"
    "../src/msg/tmsg.c"
  )
  target_include_directories(test_tmsg PUBLIC "${TD_SOURCE_DIR}/include/common/")
  target_link_libraries(test_tmsg PUBLIC os util gtest gtest_main)
  add_test(
    NAME test_tmsg
    COMMAND test_tmsg
  )

  # config file for msg type table
  SET(MSG_TBL_FILE ${CMAKE_CURRENT_SOURCE_DIR}/test_msgtype.ini)
  add_custom_command(TARGET test_tmsg POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${MSG_TBL_FILE} $<TARGET_FILE_DIR:test_tmsg>
  )
endif()
