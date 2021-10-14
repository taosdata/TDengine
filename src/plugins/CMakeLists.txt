CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

if(NOT WIN32)
  string(ASCII 27 Esc)
  set(ColourReset "${Esc}[m")
  set(ColourBold  "${Esc}[1m")
  set(Red         "${Esc}[31m")
  set(Green       "${Esc}[32m")
  set(Yellow      "${Esc}[33m")
  set(Blue        "${Esc}[34m")
  set(Magenta     "${Esc}[35m")
  set(Cyan        "${Esc}[36m")
  set(White       "${Esc}[37m")
  set(BoldRed     "${Esc}[1;31m")
  set(BoldGreen   "${Esc}[1;32m")
  set(BoldYellow  "${Esc}[1;33m")
  set(BoldBlue    "${Esc}[1;34m")
  set(BoldMagenta "${Esc}[1;35m")
  set(BoldCyan    "${Esc}[1;36m")
  set(BoldWhite   "${Esc}[1;37m")
endif()

ADD_SUBDIRECTORY(monitor)

IF (TD_BUILD_HTTP)
  MESSAGE("")
  MESSAGE("${Yellow} use original embedded httpd ${ColourReset}")
  MESSAGE("")
  ADD_SUBDIRECTORY(http)
ELSE ()
  MESSAGE("")
  MESSAGE("${Green} use blm3 as httpd ${ColourReset}")
  MESSAGE("")
  include(ExternalProject)
  ExternalProject_Add(blm3
    PREFIX        "blm3"
    SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/blm3
    BUILD_ALWAYS         off
    DEPENDS             taos
    BUILD_IN_SOURCE     1
    CONFIGURE_COMMAND cmake -E echo "blm3 no need cmake to config"
    BUILD_COMMAND  CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../inc CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build
    INSTALL_COMMAND cmake -E copy blm3 ${CMAKE_BINARY_DIR}/build/bin COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/ COMMAND cmake -E copy ./example/config/blm3.toml ${CMAKE_BINARY_DIR}/test/cfg/
  )
ENDIF ()

IF (TD_LINUX AND TD_MQTT)
  ADD_SUBDIRECTORY(mqtt)
ENDIF ()
