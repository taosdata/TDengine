CMAKE_MINIMUM_REQUIRED(VERSION 3.0)

project(
  TDengine
  VERSION 3.0
  DESCRIPTION "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
)

set(PROJECT_VERSION_MAJOR "3")
set(PROJECT_VERSION_MINOR "3")

set(TD_COMMUNITY_DIR ${PROJECT_SOURCE_DIR}/community)
set(TD_ENTERPRISE_DIR ${PROJECT_SOURCE_DIR}/enterprise)
set(TD_SOURCE_DIR ${TD_COMMUNITY_DIR})
set(TD_SUPPORT_DIR "${TD_SOURCE_DIR}/cmake")
set(TD_CONTRIB_DIR "${TD_SOURCE_DIR}/contrib")

message(STATUS "Community source directory: " ${TD_COMMUNITY_DIR})
message(STATUS "Enterprise source directory: " ${TD_ENTERPRISE_DIR})

include(${TD_ENTERPRISE_DIR}/cmake/cmake.options)
include(${TD_ENTERPRISE_DIR}/cmake/cmake.define)
include(${TD_SUPPORT_DIR}/cmake.platform)

set(TD_ENTERPRISE TRUE)
set(TD_ACCOUNT TRUE)
set(TD_ADMIN TRUE)
set(TD_VNODE_PLUGINS TRUE)
set(TD_MQTT FALSE)
set(TD_STORAGE TRUE)
set(TD_TOPIC TRUE)
set(TD_MODULE TRUE)
set(TD_COVER FALSE)
set(TD_MEM_CHECK FALSE)
set(TD_PAGMODE_LITE FALSE)
set(TD_SOMODE_STATIC FALSE)
set(TD_POWER FALSE)
set(TD_GODLL FALSE)
set(TD_PRIVILEGE TRUE)

# set(JEMALLOC_ENABLED TRUE)
set(BUILD_DM_MODULE TRUE)
set(TD_GRANT TRUE)

if(TD_LINUX_64 AND NOT TD_NINGSI)
  set(TD_USB_DONGLE TRUE)
else()
  set(TD_USB_DONGLE FALSE)
endif()

if(TD_WINDOWS OR TD_DARWIN)
  set(TD_SOMODE_STATIC TRUE)
endif()

include(${TD_SUPPORT_DIR}/cmake.define)
include(${TD_SUPPORT_DIR}/cmake.options)
include(${TD_SUPPORT_DIR}/cmake.version)
include(${TD_SUPPORT_DIR}/cmake.install)

add_library(api INTERFACE)
target_include_directories(api INTERFACE "${TD_SOURCE_DIR}/include/client")

add_subdirectory(community/contrib)
add_subdirectory(community/source)
add_subdirectory(community/tools)
add_subdirectory(community/utils)
add_subdirectory(community/tests)
add_subdirectory(community/docs/doxgen)

add_subdirectory(enterprise/contrib)
add_subdirectory(enterprise)

if(${BUILD_TEST})
  include(CTest)
  enable_testing()
  add_subdirectory(community/examples/c)
endif(${BUILD_TEST})

include(CPack)
