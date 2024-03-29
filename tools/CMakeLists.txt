IF (TD_WEBSOCKET)
    IF (TD_LINUX)
        SET(websocket_lib_file "libtaosws.so")
    ELSEIF (TD_DARWIN)
        SET(websocket_lib_file "libtaosws.dylib")
    ENDIF ()
    MESSAGE("${Green} use libtaos-ws${ColourReset}")
    IF (TD_ALPINE)
        include(ExternalProject)
        ExternalProject_Add(taosws-rs
            PREFIX        "taosws-rs"
            SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/taosws-rs
            BUILD_ALWAYS        off
            DEPENDS             taos
            BUILD_IN_SOURCE     1
            CONFIGURE_COMMAND cmake -E echo "taosws-rs no need cmake to config"
            PATCH_COMMAND
            COMMAND     git clean -f -d
            BUILD_COMMAND
            COMMAND cargo update
            COMMAND RUSTFLAGS=-Ctarget-feature=-crt-static cargo build --release -p taos-ws-sys --features native-tls
            INSTALL_COMMAND
            COMMAND cp target/release/${websocket_lib_file} ${CMAKE_BINARY_DIR}/build/lib
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/build/include
            COMMAND cmake -E copy target/release/taosws.h ${CMAKE_BINARY_DIR}/build/include
            )
    ELSEIF (TD_WINDOWS)
        include(ExternalProject)
        ExternalProject_Add(taosws-rs
            PREFIX        "taosws-rs"
            SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/taosws-rs
            BUILD_ALWAYS        off
            DEPENDS             taos
            BUILD_IN_SOURCE     1
            CONFIGURE_COMMAND cmake -E echo "taosws-rs no need cmake to config"
            PATCH_COMMAND
            COMMAND     git clean -f -d
            BUILD_COMMAND
            COMMAND cargo update
            COMMAND cargo build --release -p taos-ws-sys --features native-tls-vendored
            INSTALL_COMMAND
            COMMAND cp target/release/taosws.dll ${CMAKE_BINARY_DIR}/build/lib
            COMMAND cp target/release/taosws.dll.lib ${CMAKE_BINARY_DIR}/build/lib/taosws.lib
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/build/include
            COMMAND cmake -E copy target/release/taosws.h ${CMAKE_BINARY_DIR}/build/include
            )
    ELSE()
        include(ExternalProject)
        ExternalProject_Add(taosws-rs
            PREFIX        "taosws-rs"
            SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/taosws-rs
            BUILD_ALWAYS        off
            DEPENDS             taos
            BUILD_IN_SOURCE     1
            CONFIGURE_COMMAND cmake -E echo "taosws-rs no need cmake to config"
            PATCH_COMMAND
            COMMAND     git clean -f -d
            BUILD_COMMAND
            COMMAND cargo update
            COMMAND cargo build --release -p taos-ws-sys --features native-tls-vendored
            INSTALL_COMMAND
            COMMAND cp target/release/${websocket_lib_file} ${CMAKE_BINARY_DIR}/build/lib
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/build/include
            COMMAND cmake -E copy target/release/taosws.h ${CMAKE_BINARY_DIR}/build/include
            )
    ENDIF ()
ENDIF ()

IF (TD_TAOS_TOOLS)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/tools/taos_tools/deps/avro/lang/c/src)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/client)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/common)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/util)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/os)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/libs/transport)
    ADD_SUBDIRECTORY(taos-tools)
ENDIF ()

add_subdirectory(shell)
IF (TD_BUILD_HTTP)
    MESSAGE("")
    MESSAGE("${Yellow} use original embedded httpd ${ColourReset}")
    MESSAGE("")
    #    ADD_SUBDIRECTORY(http)
ELSEIF(TD_BUILD_TAOSA_INTERNAL)
    MESSAGE("${Yellow} use taosa internal as httpd ${ColourReset}")
ELSE ()
    MESSAGE("")
    MESSAGE("${Green} use taosadapter as httpd, platform is ${PLATFORM_ARCH_STR} ${ColourReset}")

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
        STRING(CONCAT taos_version "_branch_" "${taos_version}")
        STRING(STRIP "${taos_version}" taos_version)
    ENDIF ()
    EXECUTE_PROCESS(
            COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
    )
    EXECUTE_PROCESS(
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
            COMMAND git rev-parse HEAD
            RESULT_VARIABLE commit_sha1
            OUTPUT_VARIABLE taosadapter_commit_sha1
    )
    IF ("${taosadapter_commit_sha1}" STREQUAL "")
        SET(taosadapter_commit_sha1 "unknown")
    ELSE ()
#        STRING(SUBSTRING "${taosadapter_commit_sha1}" 0 7 taosadapter_commit_sha1)
        STRING(STRIP "${taosadapter_commit_sha1}" taosadapter_commit_sha1)
    ENDIF ()
    SET(taos_version ${TD_VER_NUMBER})
    MESSAGE("${Green} taosAdapter will use ${taos_version} and commit ${taosadapter_commit_sha1} as version ${ColourReset}")
    EXECUTE_PROCESS(
            COMMAND cd ..
    )
    MESSAGE("CURRENT SOURCE DIR ${CMAKE_CURRENT_SOURCE_DIR}")

    IF (TD_WINDOWS)
        MESSAGE("Building taosAdapter on Windows")
        INCLUDE(ExternalProject)
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
            COMMAND set CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client
            COMMAND set CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib
            COMMAND go build -a -o taosadapter.exe -ldflags "-s -w -X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_DATE}'"
            COMMAND go build -a -o taosadapter-debug.exe -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_DATE}'"

            INSTALL_COMMAND
            COMMAND cmake -E echo "Comparessing taosadapter.exe"
            COMMAND cmake -E time upx taosadapter.exe
            COMMAND cmake -E echo "Copy taosadapter.exe"
            COMMAND cmake -E copy taosadapter.exe ${CMAKE_BINARY_DIR}/build/bin/taosadapter.exe
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E echo "Copy taosadapter.toml"
            COMMAND cmake -E copy ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E echo "Copy taosadapter-debug.exe"
            COMMAND cmake -E copy taosadapter-debug.exe ${CMAKE_BINARY_DIR}/build/bin
            )
    ELSEIF (TD_DARWIN)
        MESSAGE("Building taosAdapter on MACOS")
        INCLUDE(ExternalProject)
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
                COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -ldflags "-s -w -X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_DATE}'"
                COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -o taosadapter-debug -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_DATE}'"
                INSTALL_COMMAND
                COMMAND cmake -E echo "Copy taosadapter"
                COMMAND cmake -E copy taosadapter ${CMAKE_BINARY_DIR}/build/bin
                COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E echo "Copy taosadapter.toml"
                COMMAND cmake -E copy ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E copy ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E echo "Copy taosadapter-debug"
                COMMAND cmake -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
                )
    ELSE ()
        MESSAGE("Building taosAdapter on non-Windows")
        INCLUDE(ExternalProject)
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
            COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -ldflags "-s -w -X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_DATE}'"
            COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -o taosadapter-debug -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_DATE}'"
            INSTALL_COMMAND
            COMMAND cmake -E echo "Comparessing taosadapter.exe"
            COMMAND upx taosadapter || :
            COMMAND cmake -E echo "Copy taosadapter"
            COMMAND cmake -E copy taosadapter ${CMAKE_BINARY_DIR}/build/bin
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E echo "Copy taosadapter.toml"
            COMMAND cmake -E copy ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E copy ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E echo "Copy taosadapter-debug"
            COMMAND cmake -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
            )
    ENDIF ()
ENDIF ()
