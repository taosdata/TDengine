CMAKE_MINIMUM_REQUIRED(VERSION 3.16)


SET(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR}/community)
SET(TD_ENTERPRISE_DIR ${PROJECT_SOURCE_DIR}/enterprise)
MESSAGE(STATUS "Community source directory: " ${TD_COMMUNITY_DIR})
MESSAGE(STATUS "Enterprise source directory: " ${TD_ENTERPRISE_DIR})

set( TD_SOURCE_DIR ${TD_COMMUNITY_DIR} )

SET(TD_ACCOUNT TRUE)
SET(TD_ADMIN TRUE)
SET(TD_TSDB_PLUGINS TRUE)
SET(TD_MQTT FALSE)
SET(TD_STORAGE TRUE)
SET(TD_TOPIC TRUE)
SET(TD_MODULE TRUE)

SET(TD_COVER FALSE)
SET(TD_MEM_CHECK FALSE)

SET(TD_PAGMODE_LITE FALSE)
SET(TD_SOMODE_STATIC FALSE)
SET(TD_POWER FALSE)
SET(TD_GODLL FALSE)

set(TD_SUPPORT_DIR "${TD_SOURCE_DIR}/cmake")
set(TD_CONTRIB_DIR "${TD_SOURCE_DIR}/contrib")

include(${TD_SUPPORT_DIR}/cmake.platform)

IF (TD_DARWIN OR TD_DARWIN64)
  MESSAGE ("Disable grant on Mac")
  SET(TD_GRANT FALSE)
ELSE ()
  SET(TD_GRANT TRUE)
ENDIF ()
IF (TD_LINUX_64 AND NOT TD_NINGSI) 
  SET(TD_USB_DONGLE TRUE)
ELSE ()
  SET(TD_USB_DONGLE FALSE)
ENDIF ()

include(${TD_SUPPORT_DIR}/cmake.define)
include(${TD_SUPPORT_DIR}/cmake.options)
include(${TD_SUPPORT_DIR}/cmake.version)
include(${TD_SUPPORT_DIR}/cmake.install)

IF (TD_WINDOWS OR TD_DARWIN) 
  SET(TD_SOMODE_STATIC TRUE)
ENDIF ()

# contrib
add_subdirectory(community/contrib)

# api
add_library(api INTERFACE)
target_include_directories(api INTERFACE "community/include/client")

# src
if(${BUILD_TEST})
    include(CTest)
    enable_testing()
endif(${BUILD_TEST})

add_subdirectory(community/source)
add_subdirectory(community/tools)
add_subdirectory(community/tests)
add_subdirectory(community/example)

# docs 
add_subdirectory(community/docs)

set( TD_SOURCE_DIR ${PROJECT_SOURCE_DIR} )

ADD_SUBDIRECTORY(enterprise)

INCLUDE(CPack)
