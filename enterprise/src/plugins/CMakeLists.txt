CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(TDengine)

#ADD_SUBDIRECTORY(admin)
#ADD_SUBDIRECTORY(account)
#ADD_SUBDIRECTORY(topic)

ADD_SUBDIRECTORY(grant)
#ADD_SUBDIRECTORY(tsdb)
ADD_SUBDIRECTORY(storage)
#ADD_SUBDIRECTORY(module)

IF(TD_BUILD_TAOSA_INTERNAL)
  MESSAGE("")
  MESSAGE("${Green} use taosainternal as httpd ${ColourReset}")

  EXECUTE_PROCESS(
      COMMAND  git rev-parse --abbrev-ref HEAD
      RESULT_VARIABLE result_taos_version
      OUTPUT_VARIABLE taos_version
      )

  STRING(FIND ${taos_version} release is_release_branch)

  IF ("${is_release_branch}" STREQUAL "0")
      STRING(SUBSTRING "${taos_version}" 12 -1 taos_version)
      STRING(STRIP "${taos_version}" taos_version)
  ELSE ()
      STRING(CONCAT taos_version "branch_" "${taos_version}")
      STRING(STRIP "${taos_version}" taos_version)
  ENDIF ()
  EXECUTE_PROCESS(
      COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/taosainternal
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
  MESSAGE("${Green} taosAdapter will use ${taos_version} and commit ${taosadapter_commit_sha1} as version ${ColourReset}")
  EXECUTE_PROCESS(
      COMMAND cd ..
      )

  IF (TD_LINUX)
      include(ExternalProject)
      set(_upx_prefix "$ENV{HOME}/.taos/externals/upx")
      ExternalProject_Add(upx
              PREFIX              "${_upx_prefix}"
              URL                 https://github.com/upx/upx/releases/download/v3.96/upx-3.96-${PLATFORM_ARCH_STR}_linux.tar.xz
              CONFIGURE_COMMAND   ""
              BUILD_COMMAND       ""
              INSTALL_COMMAND     ""
              )

      ExternalProject_Add(taosainternal
          PREFIX        "taosainternal"
          SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/taosainternal
          BUILD_ALWAYS         off
          DEPENDS             taos
          BUILD_IN_SOURCE     1
          CONFIGURE_COMMAND cmake -E echo "taosadapter no need cmake to config"
          PATCH_COMMAND
          COMMAND     git clean -f -d
          BUILD_COMMAND
          COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../../../community/include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -o taosadapter -ldflags "-s -w -X github.com/taosdata/taosadapter/version.Version=${taos_version} -X github.com/taosdata/taosadapter/version.CommitID=${taosadapter_commit_sha1}"
          COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../../../community/include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -o taosadapter-debug -ldflags "-X github.com/taosdata/taosadapter/version.Version=${taos_version} -X github.com/taosdata/taosadapter/version.CommitID=${taosadapter_commit_sha1}"
          INSTALL_COMMAND
          COMMAND ${_upx_prefix}/src/upx/upx taosadapter
          COMMAND cmake -E copy taosadapter ${CMAKE_BINARY_DIR}/build/bin
          COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
          COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/taosadapter/example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
          COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/taosadapter/taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
          COMMAND cmake -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
          )
      unset(_upx_prefix)

  ELSEIF (TD_WINDOWS)
      include(ExternalProject)
      set(_upx_prefix "${CMAKE_BINARY_DIR}/.taos/externals/upx")
      ExternalProject_Add(upx
              PREFIX              "${_upx_prefix}"
              URL                 https://github.com/upx/upx/releases/download/v3.96/upx-3.96-win32.zip
              CONFIGURE_COMMAND   ""
              BUILD_COMMAND       ""
              INSTALL_COMMAND     ""
              )

      ExternalProject_Add(taosadapter
              PREFIX        "taosadapter"
              SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
              BUILD_ALWAYS         off
              DEPENDS             taos
              BUILD_IN_SOURCE     1
              CONFIGURE_COMMAND cmake -E echo "taosadapter no need cmake to config"
              PATCH_COMMAND
              COMMAND     git clean -f -d
              BUILD_COMMAND
              COMMAND set CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../../../community/include/client
              COMMAND set CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib
              COMMAND go build -a -o taosadapter.exe -ldflags "-s -w -X github.com/taosdata/taosadapter/version.Version=${taos_version} -X github.com/taosdata/taosadapter/version.CommitID=${taosadapter_commit_sha1}"
              COMMAND go build -a -o taosadapter-debug.exe -ldflags "-X github.com/taosdata/taosadapter/version.Version=${taos_version} -X github.com/taosdata/taosadapter/version.CommitID=${taosadapter_commit_sha1}"
              INSTALL_COMMAND
              COMMAND ${_upx_prefix}/src/upx/upx taosadapter
              COMMAND cmake -E copy taosadapter.exe ${CMAKE_BINARY_DIR}/build/bin
              COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
              COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/taosadapter/example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
              COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/taosadapter/taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
              COMMAND cmake -E copy taosadapter-debug.exe ${CMAKE_BINARY_DIR}/build/bin
              )
      unset(_upx_prefix)
  ENDIF ()
ENDIF ()
