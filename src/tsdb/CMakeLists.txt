CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(inc)
AUX_SOURCE_DIRECTORY(src SRC)

IF (TD_LINUX)
  ADD_LIBRARY(tsdb ${SRC})
  TARGET_LINK_LIBRARIES(tsdb common tutil)

  # Someone has no gtest directory, so comment it
  # ADD_SUBDIRECTORY(tests)
ENDIF ()
