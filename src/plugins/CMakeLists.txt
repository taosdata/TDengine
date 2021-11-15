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
  MESSAGE("${Green} use taosadapter as httpd ${ColourReset}")
  EXECUTE_PROCESS(
      COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
      )
  EXECUTE_PROCESS(
      COMMAND git rev-parse --short HEAD
      RESULT_VARIABLE commit_sha1
      OUTPUT_VARIABLE taosadapter_commit_sha1
      )
  IF ("${taosadapter_commit_sha1}" STREQUAL "")
        SET(taosadapter_commit_sha1 "unknown")
  ELSE ()
        STRING(SUBSTRING "${taosadapter_commit_sha1}" 0 7 taosadapter_commit_sha1)
        STRING(STRIP "${taosadapter_commit_sha1}" taosadapter_commit_sha1)
  ENDIF ()
  MESSAGE("${Green} taosadapter commit: ${taosadapter_commit_sha1} ${ColourReset}")
  EXECUTE_PROCESS(
      COMMAND cd ..
      )
  include(ExternalProject)
  ExternalProject_Add(taosadapter
    PREFIX        "taosadapter"
    SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
    BUILD_ALWAYS         off
    DEPENDS             taos
    BUILD_IN_SOURCE     1
    CONFIGURE_COMMAND cmake -E echo "taosadapter no need cmake to config"
    PATCH_COMMAND
        COMMAND     git clean -f -d
    BUILD_COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../inc CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -ldflags "-s -w -X github.com/taosdata/taosadapter/version.CommitID=${taosadapter_commit_sha1}"
    INSTALL_COMMAND
        COMMAND curl -sL https://github.com/upx/upx/releases/download/v3.96/upx-3.96-amd64_linux.tar.xz -o upx.tar.xz && tar xvJf upx.tar.xz --strip-components 1 > /dev/null && ./upx taosadapter || :
        COMMAND cmake -E copy taosadapter ${CMAKE_BINARY_DIR}/build/bin
        COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
        COMMAND cmake -E copy ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
        COMMAND cmake -E copy ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
  )
ENDIF ()

IF (TD_LINUX AND TD_MQTT)
  ADD_SUBDIRECTORY(mqtt)
ENDIF ()
