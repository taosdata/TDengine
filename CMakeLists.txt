CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

project(
    TDengine
    VERSION 3.0
    DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

SET(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR}/community)
SET(TD_ENTERPRISE_DIR ${PROJECT_SOURCE_DIR}/enterprise)
MESSAGE(STATUS "Community source directory: " ${TD_COMMUNITY_DIR})
MESSAGE(STATUS "Enterprise source directory: " ${TD_ENTERPRISE_DIR})

IF (CUS_NAME)
    MESSAGE(STATUS "Customized brand name: " ${CUS_NAME})
    ADD_DEFINITIONS(-DCUS_NAME="${CUS_NAME}")
ENDIF ()

IF (CUS_PROMPT)
    MESSAGE(STATUS "Customized prompt: " ${CUS_PROMPT})
    ADD_DEFINITIONS(-DCUS_PROMPT="${CUS_PROMPT}")
ENDIF ()

IF (CUS_EMAIL)
    MESSAGE(STATUS "Customized email: " ${CUS_EMAIL})
    ADD_DEFINITIONS(-DCUS_EMAIL="${CUS_EMAIL}")
ENDIF ()

IF (GRANT_VALUE)
    MESSAGE(STATUS "Input grant value: " ${GRANT_VALUE})
    ADD_DEFINITIONS(-DGRANT_VALUE="${GRANT_VALUE}")
ENDIF ()

IF (CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)
    ADD_DEFINITIONS(-I${PROJECT_SOURCE_DIR}/enterprise/packaging)
ENDIF (CUS_NAME OR CUS_PROMPT OR CUS_EMAIL)

set( TD_SOURCE_DIR ${PROJECT_SOURCE_DIR} )
include(${TD_SOURCE_DIR}/enterprise/cmake/cmake.options)
add_subdirectory(enterprise/contrib)

set( TD_SOURCE_DIR ${TD_COMMUNITY_DIR} )

set(TD_ENTERPRISE TRUE)
SET(TD_ACCOUNT TRUE)
SET(TD_ADMIN TRUE)
SET(TD_VNODE_PLUGINS TRUE)
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
SET(TD_PRIVILEGE TRUE)
SET(JEMALLOC_ENABLED TRUE)
SET(BUILD_DM_MODULE TRUE)

set(TD_SUPPORT_DIR "${TD_SOURCE_DIR}/cmake")
set(TD_CONTRIB_DIR "${TD_SOURCE_DIR}/contrib")

include(${TD_SUPPORT_DIR}/cmake.platform)

SET(TD_GRANT TRUE)

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
# if(${BUILD_TEST})
    include(CTest)
    enable_testing()
# endif(${BUILD_TEST})

ADD_DEFINITIONS(-DTD_ENTERPRISE)

add_subdirectory(community/source)
add_subdirectory(community/tools)
add_subdirectory(community/utils)
add_subdirectory(community/examples/c)

# docs 
add_subdirectory(community/docs/doxgen)

set( TD_SOURCE_DIR ${PROJECT_SOURCE_DIR} )

ADD_SUBDIRECTORY(enterprise)

INCLUDE(CPack)
