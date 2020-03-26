CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

FIND_PROGRAM(TD_MVN_INSTALLED mvn)
IF (TD_MVN_INSTALLED)
  MESSAGE(STATUS "MVN is installed and JDBC will be compiled")
ELSE ()
  MESSAGE(STATUS "MVN is not installed and JDBC is not compiled")
ENDIF ()

#
# If need to set debug options
#   1.Generate debug version:
#     mkdir debug; cd debug;
#     cmake -DCMAKE_BUILD_TYPE=Debug ..
#   2.Generate release version:
#     mkdir release; cd release;
#     cmake -DCMAKE_BUILD_TYPE=Release ..
#

# Set compiler options
SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ${COMMON_FLAGS} ${DEBUG_FLAGS}")
SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} ${COMMON_FLAGS} ${RELEASE_FLAGS}")

# Set c++ compiler options
# SET(COMMON_CXX_FLAGS "${COMMON_FLAGS} -std=c++11")
# SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${COMMON_CXX_FLAGS} ${DEBUG_FLAGS}")
# SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${COMMON_CXX_FLAGS} ${RELEASE_FLAGS}")

IF (${CMAKE_BUILD_TYPE} MATCHES "Debug")
  MESSAGE(STATUS "Build Debug Version")
ELSEIF (${CMAKE_BUILD_TYPE} MATCHES "Release")
  MESSAGE(STATUS "Build Release Version")
ELSE ()
  IF (TD_WINDOWS_64)
    SET(CMAKE_BUILD_TYPE "Release")
    MESSAGE(STATUS "Build Release Version in Windows as default")
  ELSE ()
    SET(CMAKE_BUILD_TYPE "Debug")
    MESSAGE(STATUS "Build Debug Version as default")
  ENDIF()
ENDIF ()