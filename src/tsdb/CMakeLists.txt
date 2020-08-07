CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(inc)
AUX_SOURCE_DIRECTORY(src SRC)

IF (TD_LINUX)
  ADD_LIBRARY(tsdb ${SRC})
  TARGET_LINK_LIBRARIES(tsdb common tutil)
  # Someone has no gtest directory, so comment it
  # ADD_SUBDIRECTORY(tests)
ELSEIF (TD_WINDOWS)
SET(CMAKE_MODULE_PATH ${TD_COMMUNITY_DIR}/deps/MsvcLibX/cmake)
  include(FindWindowsSDK)
  get_mywindowssdk_include_dirs(${WINDOWSSDK_PREFERRED_DIR} WINSDK_INCLUDE_DIR)
  get_ucrt_include_dirs(${WINDOWSSDK_PREFERRED_DIR} UCRT_INCLUDE_DIR)

  Add_Definitions("-DWSDKINCLUDE=${WINSDK_INCLUDE_DIR}")

  include(FindMSVC)
  Add_Definitions("-DMSVCINCLUDE=${VC_INCLUDE_DIR}")
  
  IF ((NOT DEFINED UCRT_INCLUDE_DIR) OR (UCRT_INCLUDE_DIR STREQUAL "NOTFOUND"))
    Message(STATUS "UCRT_INCLUDE_DIR notu found, set it to vc incude dir")
    SET(UCRT_INCLUDE_DIR "${VC_INCLUDE_DIR}")
    Add_Definitions("-DUCRTINCLUDE=${VC_INCLUDE_DIR}")
  ELSE ()
    Add_Definitions("-DUCRTINCLUDE=${UCRT_INCLUDE_DIR}")
  ENDIF ()

  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/MsvcLibX/include)

  ADD_LIBRARY(tsdb ${SRC})
  TARGET_LINK_LIBRARIES(tsdb common tutil)
ENDIF ()
