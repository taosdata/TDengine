CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

SET(CMAKE_C_STANDARD 11)
SET(CMAKE_VERBOSE_MAKEFILE ON)

#set output directory
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/lib)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/bin)
SET(TD_TESTS_OUTPUT_DIR ${PROJECT_BINARY_DIR}/test)

MESSAGE(STATUS "Project source directory: " ${PROJECT_SOURCE_DIR})
MESSAGE(STATUS "Project binary files output path: " ${PROJECT_BINARY_DIR})
MESSAGE(STATUS "Project executable files output path: " ${EXECUTABLE_OUTPUT_PATH})
MESSAGE(STATUS "Project library files output path: " ${LIBRARY_OUTPUT_PATH})

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
  SET(CMAKE_BUILD_TYPE "Debug")
  MESSAGE(STATUS "Build Debug Version")
ELSEIF (${CMAKE_BUILD_TYPE} MATCHES "Release")
  SET(CMAKE_BUILD_TYPE "Release")
  MESSAGE(STATUS "Build Release Version")
ELSE ()
  IF (TD_WINDOWS)
    SET(CMAKE_BUILD_TYPE "Release")
    MESSAGE(STATUS "Build Release Version in Windows as default")
  ELSE ()
    SET(CMAKE_BUILD_TYPE "Debug")
    MESSAGE(STATUS "Build Debug Version as default")
  ENDIF()
ENDIF ()