CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
PROJECT(TDengine)

#ADD_SUBDIRECTORY(admin)
#ADD_SUBDIRECTORY(account)

ADD_SUBDIRECTORY(grant)
#ADD_SUBDIRECTORY(tsdb)
ADD_SUBDIRECTORY(storage)
#ADD_SUBDIRECTORY(module)
ADD_SUBDIRECTORY(dnode)

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
    SET(taos_version ${TD_VER_NUMBER})
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
            COMMAND go build -a -o taosadapter.exe -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.CUS_NAME=${TAOSA_CUS_NAME}' -X 'github.com/taosdata/taosadapter/v3/version.CUS_PROMPT=${TAOSA_CUS_PROMPT}' -X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
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
            COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../../../community/include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -o taosadapter -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.CUS_NAME=${TAOSA_CUS_NAME}' -X 'github.com/taosdata/taosadapter/v3/version.CUS_PROMPT=${TAOSA_CUS_PROMPT}' -X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
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
                COMMAND RUSTFLAGS=-Ctarget-feature=-crt-static LIBRARY_PATH=$ENV{LIBRARY_PATH} LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH} CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} VER_NUMBER=${VER_NUMBER} cargo build --release
                INSTALL_COMMAND
                COMMAND cmake -E copy target/release/taosx ${CMAKE_BINARY_DIR}/build/bin
                COMMAND cmake -E copy target/${TAOSX_NAME}.service ${OUTPUT_SERVICE_DIR}
            )
            ExternalProject_Add(taosx-agent
                PREFIX              taosx
                SOURCE_DIR          ${CMAKE_CURRENT_SOURCE_DIR}/taosx/taosx-agent
                BUILD_ALWAYS        off
                DEPENDS             taos
                BUILD_IN_SOURCE     1
                CONFIGURE_COMMAND
                COMMAND git status
                BUILD_COMMAND
                COMMAND RUSTFLAGS=-Ctarget-feature=-crt-static LIBRARY_PATH=$ENV{LIBRARY_PATH} LD_LIBRARY_PATH=$ENV{LD_LIBRARY_PATH} CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} VER_NUMBER=${VER_NUMBER} cargo build --release
                INSTALL_COMMAND
                COMMAND cmake -E copy target/release/taosx-agent ${CMAKE_BINARY_DIR}/build/bin
                COMMAND cmake -E copy target/${TAOSX_NAME}-agent.service ${OUTPUT_SERVICE_DIR}
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
            ExternalProject_Add(taosx-agent
                PREFIX              taosx
                SOURCE_DIR          ${CMAKE_CURRENT_SOURCE_DIR}/taosx/taosx-agent
                BUILD_ALWAYS        off
                DEPENDS             taos
                BUILD_IN_SOURCE     1
                CONFIGURE_COMMAND
                COMMAND git status
                BUILD_COMMAND
                COMMAND CUS_PROMPT=${CUS_PROMPT} CUS_NAME=${CUS_NAME} CUS_EMAIL=${CUS_EMAIL} ${TAOSX_BULID_COMMAND}
                INSTALL_COMMAND
                COMMAND cmake -E copy ${TAOSX_BINARY}-agent ${CMAKE_BINARY_DIR}/build/bin
                COMMAND cmake -E copy target/${TAOSX_NAME}-agent.service ${OUTPUT_SERVICE_DIR}
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
            COMMAND set LIBPATH=$ENV{LIBRARY_PATH} && set LIB=$ENV{LIBRARY_PATH} && set CUS_PROMPT=${CUS_PROMPT} && set CUS_NAME=${CUS_NAME} && set CUS_EMAIL=${CUS_EMAIL} &&  set VER_NUMBER=${VER_NUMBER} && cargo build --release
            INSTALL_COMMAND
            COMMAND cmake -E copy target/release/taosx.exe ${CMAKE_BINARY_DIR}/build/bin
        )
        ExternalProject_Add(taosx-agent
            PREFIX              taosx
            SOURCE_DIR          ${CMAKE_CURRENT_SOURCE_DIR}/taosx/taosx-agent
            BUILD_ALWAYS        off
            DEPENDS             taos
            BUILD_IN_SOURCE     1
            CONFIGURE_COMMAND
            COMMAND git status
            BUILD_COMMAND
            COMMAND set LIBPATH=$ENV{LIBRARY_PATH} && set LIB=$ENV{LIBRARY_PATH} && set CUS_PROMPT=${CUS_PROMPT} && set CUS_NAME=${CUS_NAME} && set CUS_EMAIL=${CUS_EMAIL} &&  set VER_NUMBER=${VER_NUMBER} && cargo build --release
            INSTALL_COMMAND
            COMMAND cmake -E copy target/release/taosx-agent.exe ${CMAKE_BINARY_DIR}/build/bin
        )
    ENDIF ()
ENDIF ()

IF(TD_BUILD_KEEPER_INTERNAL)
    MESSAGE("")
    MESSAGE("${Green} build taoskeeper internal, current platform is ${PLATFORM_ARCH_STR} ${ColourReset}")

    EXECUTE_PROCESS(
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper
        COMMAND git rev-parse HEAD
        OUTPUT_VARIABLE taoskeeper_commit_sha1
    )

    IF("${taoskeeper_commit_sha1}" STREQUAL "")
        SET(taoskeeper_commit_sha1 "unknown")
    ELSE()
        STRING(STRIP "${taoskeeper_commit_sha1}" taoskeeper_commit_sha1)
    ENDIF()

    SET(taos_version ${TD_VER_NUMBER})
    MESSAGE("${Green} taoskeeper internal will use ${taos_version} and commit ${taoskeeper_commit_sha1} as version ${ColourReset}")
    MESSAGE(" current source dir is ${CMAKE_CURRENT_SOURCE_DIR}")

    IF(TD_WINDOWS)
        MESSAGE("Building taoskeeper internal on Windows")
        INCLUDE(ExternalProject)
        ExternalProject_Add(taoskinternal
            PREFIX "taoskinternal"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper
            BUILD_ALWAYS off
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake -E echo "taoskeeper internal no need cmake to config"
            PATCH_COMMAND
            COMMAND git clean -f -d
            BUILD_COMMAND
            COMMAND go build -a -ldflags "-X 'github.com/taosdata/taoskeeper/version.IsEnterprise=true' -X 'github.com/taosdata/taoskeeper/version.Version=${taos_version}' -X 'github.com/taosdata/taoskeeper/version.CommitID=${taoskeeper_commit_sha1}' -X 'github.com/taosdata/taoskeeper/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
            INSTALL_COMMAND
            COMMAND cmake -E echo "Copy taoskeeper.exe"
            COMMAND cmake -E copy taoskeeper.exe ${CMAKE_BINARY_DIR}/build/bin
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E echo "Copy taoskeeper.toml"
            COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper/config/taoskeeper_enterprise.toml ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.toml
            COMMAND cmake -E echo "Copy taoskeeper.service"
            COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper/taoskeeper.service ${CMAKE_BINARY_DIR}/test/cfg/
        )
    ELSE()
        IF(TD_DARWIN)
            MESSAGE("Building taoskeeper internal on macOS")
        ELSE()
            MESSAGE("Building taoskeeper internal on Linux")
        ENDIF()

        INCLUDE(ExternalProject)
        ExternalProject_Add(taoskinternal
            PREFIX "taoskinternal"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper
            BUILD_ALWAYS off
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake -E echo "taoskeeper internal no need cmake to config"
            PATCH_COMMAND
            COMMAND git clean -f -d
            BUILD_COMMAND
            COMMAND go build -a -ldflags "-X 'github.com/taosdata/taoskeeper/version.IsEnterprise=true' -X 'github.com/taosdata/taoskeeper/version.Version=${taos_version}' -X 'github.com/taosdata/taoskeeper/version.CommitID=${taoskeeper_commit_sha1}' -X 'github.com/taosdata/taoskeeper/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
            INSTALL_COMMAND
            COMMAND cmake -E echo "Copy taoskeeper"
            COMMAND cmake -E copy taoskeeper ${CMAKE_BINARY_DIR}/build/bin/
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E echo "Copy taoskeeper.toml"
            COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper/config/taoskeeper_enterprise.toml ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.toml
            COMMAND cmake -E echo "Copy taoskeeper.service"
            COMMAND cmake -E copy ${CMAKE_CURRENT_SOURCE_DIR}/../../../community/tools/keeper/taoskeeper.service ${CMAKE_BINARY_DIR}/test/cfg/
        )
    ENDIF()
ENDIF()
