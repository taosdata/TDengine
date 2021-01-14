CMAKE_MINIMUM_REQUIRED(VERSION 3.5)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(inc)
AUX_SOURCE_DIRECTORY(src SRC)
ADD_LIBRARY(tsdb ${SRC})
TARGET_LINK_LIBRARIES(tsdb common tutil)

IF (TD_LINUX)
  # Someone has no gtest directory, so comment it
  # ADD_SUBDIRECTORY(tests)
ENDIF ()
