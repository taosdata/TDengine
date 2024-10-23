

# bloomFilterTest

#TARGET_LINK_LIBRARIES(streamUpdateTest
        #PUBLIC os util common gtest gtest_main stream executor index
        #)

#TARGET_INCLUDE_DIRECTORIES(
  #streamUpdateTest
  #PUBLIC "${TD_SOURCE_DIR}/include/libs/stream/"
  #PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
#)

#ADD_EXECUTABLE(checkpointTest checkpointTest.cpp)
#TARGET_LINK_LIBRARIES(
        #checkpointTest
        #PUBLIC os common gtest stream executor qcom index transport util
#)

#TARGET_INCLUDE_DIRECTORIES(
        #checkpointTest
        #PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
#)

#add_executable(backendTest "")

#target_sources(backendTest 
        #PRIVATE
        #"backendTest.cpp"
#)

#TARGET_LINK_LIBRARIES(
        #backendTest
        #PUBLIC rocksdb
        #PUBLIC os common gtest stream executor qcom index transport util
#)

#TARGET_INCLUDE_DIRECTORIES(
        #backendTest
        #PUBLIC "${TD_SOURCE_DIR}/include/libs/stream/"
        #PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
#)

#add_test(
  #NAME streamUpdateTest
  #COMMAND streamUpdateTest
#)

#add_test(
  #NAME checkpointTest
  #COMMAND checkpointTest
#)
#add_test(
  #NAME backendTest
  #COMMAND backendTest
#)


#add_executable(backendTest "")

#target_sources(backendTest
  #PUBLIC 
  #"backendTest.cpp"
#)

#target_include_directories(
  #backendTest
  #PUBLIC "${TD_SOURCE_DIR}/include/libs/stream/"
  #PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
#)

#target_link_libraries(
        #backendTest
        #PUBLIC rocksdb
        #PUBLIC os common gtest stream executor qcom index transport util
#)


MESSAGE(STATUS "build parser unit test")

IF(NOT TD_DARWIN)
        # GoogleTest requires at least C++11
        SET(CMAKE_CXX_STANDARD 11)
        AUX_SOURCE_DIRECTORY(${CMAKE_CURRENT_SOURCE_DIR} SOURCE_LIST)

        ADD_EXECUTABLE(backendTest ${SOURCE_LIST})
        TARGET_LINK_LIBRARIES(
          backendTest
          PUBLIC rocksdb
          PUBLIC os common gtest stream executor qcom index transport util vnode
        )

        TARGET_INCLUDE_DIRECTORIES(
                backendTest 
                PUBLIC "${TD_SOURCE_DIR}/include/libs/stream/"
                PRIVATE "${TD_SOURCE_DIR}/source/libs/stream/inc"
        )

        ADD_TEST(
          NAME backendTest 
          COMMAND backendTest 
        )
ENDIF ()
