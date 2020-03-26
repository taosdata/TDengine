CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

SET(TD_CLUSTER TRUE)
SET(TD_ACCOUNT TRUE)
SET(TD_COVER FALSE)
SET(TD_PAGMODE_LITE FALSE)
SET(TD_GODLL FALSE)
SET(TD_MEM_CHECK FALSE)

SET(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR}/community)
SET(TD_ENTERPRISE_DIR ${PROJECT_SOURCE_DIR}/enterprise)
MESSAGE(STATUS "Community source directory: " ${TD_COMMUNITY_DIR})
MESSAGE(STATUS "Enterprise source directory: " ${TD_ENTERPRISE_DIR})

INCLUDE(community/cmake/input.inc)
INCLUDE(community/cmake/platform.inc)
INCLUDE(community/cmake/env.inc)
INCLUDE(community/cmake/define.inc)
INCLUDE(community/cmake/install.inc)

#set output directory
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/lib)
SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/build/bin)
SET(TD_TESTS_OUTPUT_DIR ${PROJECT_BINARY_DIR}/test)

MESSAGE(STATUS "Operating system dependency directory: " ${TD_OS_DIR})
MESSAGE(STATUS "Project source directory: " ${PROJECT_SOURCE_DIR})
MESSAGE(STATUS "Project binary files output path: " ${PROJECT_BINARY_DIR})
MESSAGE(STATUS "Project executable files output path: " ${EXECUTABLE_OUTPUT_PATH})
MESSAGE(STATUS "Project library files output path: " ${LIBRARY_OUTPUT_PATH})

ADD_SUBDIRECTORY(enterprise)
ADD_SUBDIRECTORY(community/deps)
ADD_SUBDIRECTORY(community/src)
ADD_SUBDIRECTORY(community/tests)

INCLUDE(CPack)
