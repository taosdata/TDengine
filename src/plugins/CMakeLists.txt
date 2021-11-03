CMAKE_MINIMUM_REQUIRED(VERSION 3.0...3.20)
PROJECT(TDengine)

ADD_SUBDIRECTORY(monitor)

IF (TD_BUILD_HTTP)
  MESSAGE("")
  MESSAGE("${Yellow} use original embedded httpd ${ColourReset}")
  MESSAGE("")
  ADD_SUBDIRECTORY(http)
ELSE ()
  MESSAGE("")
  MESSAGE("${Green} use blm3 as httpd ${ColourReset}")
  EXECUTE_PROCESS(
      COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/blm3
      )
  EXECUTE_PROCESS(
      COMMAND git rev-parse --short HEAD
      RESULT_VARIABLE commit_sha1
      OUTPUT_VARIABLE blm3_commit_sha1
      )
  IF ("${blm3_commit_sha1}" STREQUAL "")
        SET(blm3_commit_sha1 "unknown")
  ELSE ()
        STRING(SUBSTRING "${blm3_commit_sha1}" 0 7 blm3_commit_sha1)
        STRING(STRIP "${blm3_commit_sha1}" blm3_commit_sha1)
  ENDIF ()
  MESSAGE("${Green} blm3 commit: ${blm3_commit_sha1} ${ColourReset}")
  EXECUTE_PROCESS(
      COMMAND cd ..
      )
  include(ExternalProject)
  ExternalProject_Add(blm3
    PREFIX        "blm3"
    SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/blm3
    BUILD_ALWAYS         off
    DEPENDS             taos
    BUILD_IN_SOURCE     1
    CONFIGURE_COMMAND cmake -E echo "blm3 no need cmake to config"
    BUILD_COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../inc CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -ldflags "-s -w -X github.com/taosdata/blm3/version.CommitID=${blm3_commit_sha1}"
    INSTALL_COMMAND cmake -E copy blm3 ${CMAKE_BINARY_DIR}/build/bin COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/ COMMAND cmake -E copy ./example/config/blm.toml ${CMAKE_BINARY_DIR}/test/cfg/
  )
ENDIF ()

IF (TD_LINUX AND TD_MQTT)
  ADD_SUBDIRECTORY(mqtt)
ENDIF ()
