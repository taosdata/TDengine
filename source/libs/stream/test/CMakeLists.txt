# bloomFilterTest

# TARGET_LINK_LIBRARIES(test_streamupdate
#   PUBLIC os util common gtest gtest_main stream executor index
# )

# TARGET_INCLUDE_DIRECTORIES(
# test_streamupdate
#   PUBLIC "${TD_SOURCE_DIR}/include/libs/stream/"
#   PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
# )

# ADD_EXECUTABLE(test_checkpoint checkpointTest.cpp)
# TARGET_LINK_LIBRARIES(
#   test_checkpoint
#   PUBLIC os common gtest stream executor qcom index transport util
# )

# TARGET_INCLUDE_DIRECTORIES(
#   test_checkpoint
#   PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
# )

# add_executable(test_backend "")

# target_sources(test_backend
#   PRIVATE
#   "backendTest.cpp"
# )

# TARGET_LINK_LIBRARIES(
# test_backend
#   PUBLIC rocksdb
#   PUBLIC os common gtest stream executor qcom index transport util
# )

# TARGET_INCLUDE_DIRECTORIES(
# test_backend
#   PUBLIC "${TD_SOURCE_DIR}/include/libs/stream/"
#   PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
# )

# add_test(
#   NAME test_streamupdate
#   COMMAND test_streamupdate
# )

# add_test(
#   NAME test_checkpoint
#   COMMAND test_checkpoint
# )
# add_test(
#   NAME test_backend
#   COMMAND test_backend
# )

# add_executable(test_backend "")

# target_sources(test_backend
#   PUBLIC
#   "backendTest.cpp"
# )

# target_include_directories(
# test_backend
#   PUBLIC "${TD_SOURCE_DIR}/include/libs/stream/"
#   PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
# )

# target_link_libraries(
# test_backend
#   PUBLIC rocksdb
#   PUBLIC os common gtest stream executor qcom index transport util
# )

MESSAGE(STATUS "build parser unit test")

IF(NOT TD_DARWIN)
  # GoogleTest requires at least C++11
  SET(CMAKE_CXX_STANDARD 11)
  AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

  ADD_EXECUTABLE(test_backend ${SOURCE_LIST})
  TARGET_LINK_LIBRARIES(
    test_backend
    PUBLIC rocksdb
    PUBLIC os common gtest stream executor qcom index transport util vnode
  )

  TARGET_INCLUDE_DIRECTORIES(
    test_backend
    PUBLIC "${TD_SOURCE_DIR}/include/libs/stream/"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
  )

  ADD_TEST(
    NAME test_backend
    COMMAND test_backend
  )
ENDIF()
