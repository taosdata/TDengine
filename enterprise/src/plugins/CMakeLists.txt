CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(TDengine)

#ADD_SUBDIRECTORY(admin)
#ADD_SUBDIRECTORY(account)
#ADD_SUBDIRECTORY(topic)

ADD_SUBDIRECTORY(grant)
#ADD_SUBDIRECTORY(tsdb)
ADD_SUBDIRECTORY(storage)
#ADD_SUBDIRECTORY(module)

ADD_SUBDIRECTORY(web)

if ("${CUS_PROMPT}" STREQUAL "")
    MESSAGE("CUS_PROMPT EMPTY set taos")
    SET(TAOSA_CUS_PROMPT "taos")
ELSE()
    MESSAGE("CUS_PROMPT:${CUS_PROMPT}")
    SET(TAOSA_CUS_PROMPT "${CUS_PROMPT}")
ENDIF()

if ("${CUS_NAME}" STREQUAL "")
    MESSAGE("CUS_NAME EMPTY set TDengine")
    SET(TAOSA_CUS_NAME "TDengine")
ELSE()
    MESSAGE("CUS_NAME:${CUS_NAME}")
    SET(TAOSA_CUS_NAME "${CUS_NAME}")
ENDIF()

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
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/taosainternal
            COMMAND  go mod tidy
    )

    EXECUTE_PROCESS(
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/taosainternal
            COMMAND  go list -m -f "{{.Version}}" github.com/taosdata/taosadapter/v3
            OUTPUT_VARIABLE taosadapter_commit_sha1
    )
    if (${taosadapter_commit_sha1} MATCHES "-")
        string(FIND ${taosadapter_commit_sha1} "-" last_dash REVERSE)
        math(EXPR last_dash "${last_dash} + 1")
        string(SUBSTRING ${taosadapter_commit_sha1} ${last_dash} -1 taosadapter_commit_sha1)
    endif()
    string(REGEX REPLACE "\n$" "" taosadapter_commit_sha1 "${taosadapter_commit_sha1}")
    string(REGEX REPLACE "\r$" "" taosadapter_commit_sha1 "${taosadapter_commit_sha1}")

    MESSAGE("${Green} taosAdapter will use ${taos_version} and commit ${taosadapter_commit_sha1} as version ${ColourReset}")
    EXECUTE_PROCESS(
        COMMAND cd ..
        )

    IF (TD_WINDOWS)
        MESSAGE("Building taosAdapter internal on Windows")
        include(ExternalProject)

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
            COMMAND set CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../../../community/include/client
            COMMAND set CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib
            COMMAND go mod tidy
            COMMAND go build -a -o taosadapter.exe -ldflags "-X github.com/taosdata/taosadapter/v3/version.CUS_NAME=${TAOSA_CUS_NAME} -X github.com/taosdata/taosadapter/v3/version.CUS_PROMPT=${TAOSA_CUS_PROMPT} -X github.com/taosdata/taosadapter/v3/version.Version=${taos_version} -X github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}"
            INSTALL_COMMAND
            COMMAND cmake -E copy taosadapter.exe ${CMAKE_BINARY_DIR}/build/bin
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/taosadapter/example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/taosadapter/taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
            )
    ELSE (TD_WINDOWS)
        MESSAGE("Building taosAdapter internal on non-Windows")
        include(ExternalProject)

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
            COMMAND go mod tidy
            COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../../../community/include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -o taosadapter -ldflags "-X github.com/taosdata/taosadapter/v3/version.CUS_NAME=${TAOSA_CUS_NAME} -X github.com/taosdata/taosadapter/v3/version.CUS_PROMPT=${TAOSA_CUS_PROMPT} -X github.com/taosdata/taosadapter/v3/version.Version=${taos_version} -X github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}"
            INSTALL_COMMAND
            COMMAND cmake -E copy taosadapter ${CMAKE_BINARY_DIR}/build/bin/
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/taosadapter/example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/taosadapter/taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
            )
    ENDIF (TD_WINDOWS)
ENDIF ()

IF ("${BUILD_TAOSX}" MATCHES "true")
    SET(TD_BULID_TAOSX TRUE)
    MESSAGE("Enable taosx")
    ADD_DEFINITIONS(-DBUILD_TAOSX)
ELSE ()
    SET(TD_BULID_TAOSX FALSE)
ENDIF ()

IF ("${CUS_PROMPT}" STREQUAL "")
    MESSAGE("CUS_PROMPT EMPTY set: ${CUS_PROMPT}")
    SET(TAOSX_NAME "taosx")
ELSE()
    MESSAGE("CUS_PROMPT:${CUS_PROMPT}")
    SET(TAOSX_NAME "${CUS_PROMPT}x")
ENDIF()


SET(OUTPUT_SERVICE_DIR ${CMAKE_BINARY_DIR}/build/share/srv)
FILE(MAKE_DIRECTORY ${OUTPUT_SERVICE_DIR})

IF(TD_BULID_TAOSX)
    include(ExternalProject)
    set(ENV{TAOS_LIBRARY_PATH} ${CMAKE_BINARY_DIR}/build/lib)
    set(ENV{LD_LIBRARY_PATH} ${CMAKE_BINARY_DIR}/build/lib:$ENV{LD_LIBRARY_PATH})
    set(ENV{LIBRARY_PATH} ${CMAKE_BINARY_DIR}/build/lib:$ENV{LIBRARY_PATH})
    IF (BUILD_SANITIZER)
        SET(TAOSX_BULID_COMMAND cargo build --release --no-default-features --features optin --features native-tls)
        SET(TAOSX_BINARY target/release/taosx)
    ELSE ()
        SET(TAOSX_BULID_COMMAND LIBRARY_PATH=$ENV{LIBRARY_PATH} LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH} cargo build --release)
        SET(TAOSX_BINARY target/release/taosx)
    ENDIF ()
    IF (TD_LINUX)
        IF(TD_ALPINE)
            ExternalProject_Add(taosx
                PREFIX              taosx
                SOURCE_DIR          ${CMAKE_CURRENT_SOURCE_DIR}/taosx
                BUILD_ALWAYS        off
                DEPENDS             taos
                BUILD_IN_SOURCE     1
                CONFIGURE_COMMAND
                COMMAND git status
                BUILD_COMMAND                
                COMMAND RUSTFLAGS=-Ctarget-feature=-crt-static LIBRARY_PATH=$ENV{LIBRARY_PATH} LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH} CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} cargo build --release
                INSTALL_COMMAND
                COMMAND cmake -E copy target/release/taosx ${CMAKE_BINARY_DIR}/build/bin
                COMMAND cmake -E copy target/${TAOSX_NAME}.service ${OUTPUT_SERVICE_DIR}
            )
        ELSE()
            ExternalProject_Add(taosx
               PREFIX              taosx
               SOURCE_DIR          ${CMAKE_CURRENT_SOURCE_DIR}/taosx
               BUILD_ALWAYS        off
               DEPENDS             taos
               BUILD_IN_SOURCE     1
               CONFIGURE_COMMAND
               COMMAND git status
               BUILD_COMMAND               
               COMMAND CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} ${TAOSX_BULID_COMMAND}
               INSTALL_COMMAND
               COMMAND cmake -E copy ${TAOSX_BINARY} ${CMAKE_BINARY_DIR}/build/bin
               COMMAND cmake -E copy target/${TAOSX_NAME}.service ${OUTPUT_SERVICE_DIR}
               )
        ENDIF()
    ELSEIF (TD_WINDOWS)
        ExternalProject_Add(taosx
            PREFIX              taosx
            SOURCE_DIR          ${CMAKE_CURRENT_SOURCE_DIR}/taosx
            BUILD_ALWAYS        off
            DEPENDS             taos
            BUILD_IN_SOURCE     1
            CONFIGURE_COMMAND
            COMMAND git status
            BUILD_COMMAND            
            COMMAND set LIBPATH=$ENV{LIBRARY_PATH} && set LIB=$ENV{LIBRARY_PATH} && set CUS_PROMPT=${CUS_PROMPT} && set CUS_NAME=${CUS_NAME} && set CUS_EMAIL=${CUS_EMAIL} && cargo build --release
            INSTALL_COMMAND
            COMMAND cmake -E copy target/release/taosx.exe ${CMAKE_BINARY_DIR}/build/bin
        )
    ENDIF ()
ENDIF ()

IF ("${BUILD_EXPLORER}" MATCHES "true")
    SET(TD_BULID_EXPLORER TRUE)
    MESSAGE("Enable explorer")
    ADD_DEFINITIONS(-DTD_BULID_EXPLORER)
ELSE ()
    SET(TD_BULID_EXPLORER FALSE)
ENDIF ()

if ("${CUS_PROMPT}" STREQUAL "")
    MESSAGE("CUS_PROMPT EMPTY set taos:${CUS_PROMPT}")
    SET(EXPLORER_PREFIX "taos")
ELSE()
    MESSAGE("CUS_PROMPT:${CUS_PROMPT}")
    SET(EXPLORER_PREFIX "${CUS_PROMPT}")
ENDIF()

IF(TD_BULID_EXPLORER)
    include(ExternalProject)
    set(ENV{TAOS_LIBRARY_PATH} ${CMAKE_BINARY_DIR}/build/lib)
    set(ENV{LD_LIBRARY_PATH} ${CMAKE_BINARY_DIR}/build/lib:$ENV{LD_LIBRARY_PATH})
    set(ENV{LIBRARY_PATH} ${CMAKE_BINARY_DIR}/build/lib:$ENV{LIBRARY_PATH})
    set(EXPLORER_ETC_PATH ${CMAKE_BINARY_DIR}/build/share/etc)
    file(MAKE_DIRECTORY ${EXPLORER_ETC_PATH})
    IF (TD_WINDOWS)
        ExternalProject_Add(explorer
            PREFIX              explorer
            SOURCE_DIR          ${CMAKE_CURRENT_SOURCE_DIR}/explorer
            BUILD_ALWAYS        off
            BUILD_IN_SOURCE     1
            CONFIGURE_COMMAND
            # COMMAND cargo update
            COMMAND git status
            BUILD_COMMAND
            COMMAND yarn --version || npm install -g yarn
            COMMAND set CUS_PROMPT=${CUS_PROMPT} && set CUS_NAME=${CUS_NAME} && set CUS_EMAIL=${CUS_EMAIL} && yarn && yarn build && cargo build --release
            COMMAND git checkout .
            INSTALL_COMMAND
            COMMAND cmake -E copy target/release/taos-explorer.exe ${CMAKE_BINARY_DIR}/build/bin/${EXPLORER_PREFIX}-explorer.exe
            COMMAND cmake -E copy server/examples/explorer.toml ${EXPLORER_ETC_PATH}/explorer.toml
            COMMAND cmake -E copy target/${EXPLORER_PREFIX}-explorer.service ${OUTPUT_SERVICE_DIR}
        )
    ELSE()
        ExternalProject_Add(explorer
            PREFIX              explorer
            SOURCE_DIR          ${CMAKE_CURRENT_SOURCE_DIR}/explorer
            BUILD_ALWAYS        off
            BUILD_IN_SOURCE     1
            CONFIGURE_COMMAND
            COMMAND git status
            BUILD_COMMAND
            COMMAND yarn --version || npm install -g yarn
            COMMAND yarn && CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} yarn build && CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} cargo build --release
            COMMAND git checkout .
            INSTALL_COMMAND
            COMMAND cmake -E copy target/release/taos-explorer ${CMAKE_BINARY_DIR}/build/bin/${EXPLORER_PREFIX}-explorer
            COMMAND cmake -E copy server/examples/explorer.toml ${EXPLORER_ETC_PATH}/explorer.toml
            COMMAND cmake -E copy target/${EXPLORER_PREFIX}-explorer.service ${OUTPUT_SERVICE_DIR}
        )
    ENDIF ()
ENDIF ()
