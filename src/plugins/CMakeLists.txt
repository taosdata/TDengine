CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

ADD_SUBDIRECTORY(monitor)

IF (TD_BUILD_HTTP)
  ADD_SUBDIRECTORY(http)
ELSE ()
  include(ExternalProject)
  ExternalProject_Add(
    blm3
    PREFIX          "blm3"
    SOURCE_DIR      ${CMAKE_CURRENT_SOURCE_DIR}/blm3
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND   go build
    )
ENDIF ()

IF (TD_LINUX AND TD_MQTT)
  ADD_SUBDIRECTORY(mqtt)
ENDIF ()
