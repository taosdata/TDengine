IF (TD_WEBSOCKET)
    MESSAGE("${Green} use libtaos-ws${ColourReset}")
    IF (TD_LINUX)
        IF (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/taosws-rs/target/release/libtaosws.so" OR "${CMAKE_CURRENT_SOURCE_DIR}/taosws-rs/target/release/libtaosws.so" IS_NEWER_THAN "${CMAKE_SOURCE_DIR}/.git/modules/tools/taosws-rs/FETCH_HEAD")
            include(ExternalProject)
            ExternalProject_Add(taosws-rs
                    PREFIX        "taosws-rs"
                    SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/taosws-rs
                    BUILD_ALWAYS         off
                    DEPENDS             taos
                    BUILD_IN_SOURCE     1
                    CONFIGURE_COMMAND cmake -E echo "taosws-rs no need cmake to config"
                    PATCH_COMMAND
                    COMMAND     git clean -f -d
                    BUILD_COMMAND
                    COMMAND cargo build --release -p taos-ws-sys
                    COMMAND ./taos-ws-sys/ci/package.sh
                    INSTALL_COMMAND
                    COMMAND cmake -E copy target/libtaosws/libtaosws.so ${CMAKE_BINARY_DIR}/build/lib
                    COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/build/include
                    COMMAND cmake -E copy target/libtaosws/taosws.h ${CMAKE_BINARY_DIR}/build/include
                    )
        ELSE()
            include(ExternalProject)
            ExternalProject_Add(taosws-rs
                    PREFIX        "taosws-rs"
                    SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/taosws-rs
                    BUILD_ALWAYS         on
                    DEPENDS             taos
                    BUILD_IN_SOURCE     1
                    CONFIGURE_COMMAND cmake -E echo "taosws-rs no need cmake to config"
                    PATCH_COMMAND
                    COMMAND     git clean -f -d
                    BUILD_COMMAND
                    COMMAND cargo build --release -p taos-ws-sys
                    COMMAND ./taos-ws-sys/ci/package.sh
                    INSTALL_COMMAND
                    COMMAND cmake -E copy target/libtaosws/libtaosws.so ${CMAKE_BINARY_DIR}/build/lib
                    COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/build/include
                    COMMAND cmake -E copy target/libtaosws/taosws.h ${CMAKE_BINARY_DIR}/build/include
                    )
        ENDIF ()
    ENDIF()
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
    MESSAGE("CURRENT SOURCE DIR ${CMAKE_CURRENT_SOURCE_DIR}")
    IF (TD_LINUX)
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
                BUILD_COMMAND
                COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -ldflags "-s -w -X github.com/taosdata/taosadapter/version.Version=${taos_version} -X github.com/taosdata/taosadapter/version.CommitID=${taosadapter_commit_sha1}"
                COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -o taosadapter-debug -ldflags "-X github.com/taosdata/taosadapter/version.Version=${taos_version} -X github.com/taosdata/taosadapter/version.CommitID=${taosadapter_commit_sha1}"
                INSTALL_COMMAND
                COMMAND wget -c https://github.com/upx/upx/releases/download/v3.96/upx-3.96-${PLATFORM_ARCH_STR}_linux.tar.xz -O ${CMAKE_CURRENT_SOURCE_DIR}/upx.tar.xz && tar -xvJf ${CMAKE_CURRENT_SOURCE_DIR}/upx.tar.xz -C ${CMAKE_CURRENT_SOURCE_DIR} --strip-components 1 > /dev/null && ${CMAKE_CURRENT_SOURCE_DIR}/upx taosadapter || :
                COMMAND cmake -E copy taosadapter ${CMAKE_BINARY_DIR}/build/bin
                COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E copy ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E copy ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
                )
    ELSEIF (TD_DARWIN)
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
                BUILD_COMMAND
                COMMAND    CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -ldflags "-s -w -X github.com/taosdata/taosadapter/version.Version=${taos_version} -X github.com/taosdata/taosadapter/version.CommitID=${taosadapter_commit_sha1}"
                COMMAND    CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -o taosadapter-debug -ldflags "-X github.com/taosdata/taosadapter/version.Version=${taos_version} -X github.com/taosdata/taosadapter/version.CommitID=${taosadapter_commit_sha1}"
                INSTALL_COMMAND
                COMMAND cmake -E copy taosadapter ${CMAKE_BINARY_DIR}/build/bin
                COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E copy ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E copy ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
                )
    ELSE ()
        MESSAGE("${Yellow} Windows system still use original embedded httpd ${ColourReset}")
    ENDIF ()
ENDIF ()
