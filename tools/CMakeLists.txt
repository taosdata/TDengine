IF(TD_WEBSOCKET)
    IF(TD_LINUX)
        SET(websocket_lib_file "libtaosws.so")
    ELSEIF(TD_DARWIN)
        SET(websocket_lib_file "libtaosws.dylib")
    ENDIF()

    MESSAGE("${Green} use libtaos-ws${ColourReset}")

    IF(TD_ALPINE)
        message(FATAL_ERROR         "freemine: not implemented yet")
        include(ExternalProject)
        ExternalProject_Add(taosws-rs
            PREFIX "taosws-rs"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosws-rs
            BUILD_ALWAYS off
            DEPENDS ${TAOS_LIB}
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake -E echo "taosws-rs no need cmake to config"
            PATCH_COMMAND
            COMMAND git clean -f -d
            BUILD_COMMAND
            COMMAND cargo update
            COMMAND RUSTFLAGS=-Ctarget-feature=-crt-static cargo build --release -p taos-ws-sys --features rustls
            INSTALL_COMMAND
            COMMAND cp target/release/${websocket_lib_file} ${CMAKE_BINARY_DIR}/build/lib
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/build/include
            COMMAND cmake -E copy target/release/taosws.h ${CMAKE_BINARY_DIR}/build/include
        )
    ELSEIF(TD_WINDOWS)
        message(FATAL_ERROR         "freemine: not implemented yet")
        include(ExternalProject)
        ExternalProject_Add(taosws-rs
            PREFIX "taosws-rs"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosws-rs
            BUILD_ALWAYS off
            DEPENDS ${TAOS_LIB}
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake -E echo "taosws-rs no need cmake to config"
            PATCH_COMMAND
            COMMAND git clean -f -d
            BUILD_COMMAND
            COMMAND cargo update
            COMMAND cargo build --release -p taos-ws-sys --features rustls
            INSTALL_COMMAND
            COMMAND cp target/release/taosws.dll ${CMAKE_BINARY_DIR}/build/lib
            COMMAND cp target/release/taosws.dll.lib ${CMAKE_BINARY_DIR}/build/lib/taosws.lib
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/build/include
            COMMAND cmake -E copy target/release/taosws.h ${CMAKE_BINARY_DIR}/build/include
        )
    ELSE()
        include(ExternalProject)
        if(${TD_LINUX})
            set(ext_taosws_static libtaosws.so)
        elseif(${TD_DARWIN})
            set(ext_taosws_static taosws)
        elseif(${TD_WINDOWS})
            set(ext_taosws_static taosws)
        endif()
        INIT_EXT(ext_taosws
            INC_DIR          include
            LIB              lib/${ext_taosws_static}
        )
        # GIT_REPOSITORY https://github.com/taosdata/taos-connector-rust.git
        # GIT_TAG 3.0
        get_from_local_repo_if_exists("https://github.com/taosdata/taos-connector-rust.git")
        ExternalProject_Add(ext_taosws
            GIT_REPOSITORY ${_git_url}
            GIT_TAG 3.0
            PREFIX "${_base}"
            BUILD_IN_SOURCE TRUE
            DEPENDS ${TAOS_LIB}
            CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
            CONFIGURE_COMMAND
                COMMAND "${CMAKE_COMMAND}" -E echo "taosws-rs no need cmake to config"
            PATCH_COMMAND
                COMMAND git clean -f -d
            BUILD_COMMAND
                # COMMAND cargo update        # freemine: why update?
                COMMAND cargo build --release -p taos-ws-sys --features rustls
            INSTALL_COMMAND
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/${websocket_lib_file} ${_ins}/lib/${websocket_lib_file}
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/taosws.h ${_ins}/include/taosws.h

            GIT_SHALLOW TRUE
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
        # add_dependencies(build_externals ext_taosws)     # this is for github workflow in cache-miss step.
    ENDIF()
ENDIF()

IF(TD_TAOS_TOOLS)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/tools/taos_tools/deps/avro/lang/c/src)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/client)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/common)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/util)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/os)
    INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/libs/transport)
    ADD_SUBDIRECTORY(taos-tools)
ENDIF()

add_subdirectory(shell)

IF(TD_BUILD_HTTP)
    MESSAGE("")
    MESSAGE("${Yellow} use original embedded httpd ${ColourReset}")
    MESSAGE("")

# ADD_SUBDIRECTORY(http)
ELSEIF(TD_BUILD_TAOSA_INTERNAL)
    MESSAGE("${Yellow} use taosa internal as httpd ${ColourReset}")
ELSE()
    MESSAGE("")
    MESSAGE("${Green} use taosadapter as httpd, platform is ${PLATFORM_ARCH_STR} ${ColourReset}")

    EXECUTE_PROCESS(
        COMMAND git rev-parse --abbrev-ref HEAD
        RESULT_VARIABLE result_taos_version
        OUTPUT_VARIABLE taos_version
    )

    STRING(FIND ${taos_version} release is_release_branch)

    IF("${is_release_branch}" STREQUAL "0")
        STRING(SUBSTRING "${taos_version}" 12 -1 taos_version)
        STRING(STRIP "${taos_version}" taos_version)
    ELSE()
        STRING(CONCAT taos_version "_branch_" "${taos_version}")
        STRING(STRIP "${taos_version}" taos_version)
    ENDIF()

    EXECUTE_PROCESS(
        COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
    )
    EXECUTE_PROCESS(
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
        COMMAND git rev-parse HEAD
        RESULT_VARIABLE commit_sha1
        OUTPUT_VARIABLE taosadapter_commit_sha1
    )

    IF("${taosadapter_commit_sha1}" STREQUAL "")
        SET(taosadapter_commit_sha1 "unknown")
    ELSE()
        # STRING(SUBSTRING "${taosadapter_commit_sha1}" 0 7 taosadapter_commit_sha1)
        STRING(STRIP "${taosadapter_commit_sha1}" taosadapter_commit_sha1)
    ENDIF()

    SET(taos_version ${TD_VER_NUMBER})
    MESSAGE("${Green} taosAdapter will use ${taos_version} and commit ${taosadapter_commit_sha1} as version ${ColourReset}")
    EXECUTE_PROCESS(
        COMMAND cd ..
    )
    MESSAGE("CURRENT SOURCE DIR ${CMAKE_CURRENT_SOURCE_DIR}")

    IF(TD_WINDOWS)
        MESSAGE("Building taosAdapter on Windows")
        message(FATAL_ERROR         "freemine: not implemented yet")
        INCLUDE(ExternalProject)
        ExternalProject_Add(taosadapter
            PREFIX "taosadapter"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
            BUILD_ALWAYS off
            DEPENDS ${TAOS_LIB}
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake -E echo "taosadapter no need cmake to config"
            PATCH_COMMAND
            COMMAND git clean -f -d
            BUILD_COMMAND
            COMMAND set CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client
            COMMAND set CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib
            COMMAND go build -a -o taosadapter.exe -ldflags "-s -w -X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
            COMMAND go build -a -o taosadapter-debug.exe -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"

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
    ELSEIF(TD_DARWIN)
        MESSAGE("Building taosAdapter on MACOS")
        message(FATAL_ERROR         "freemine: not implemented yet")
        INCLUDE(ExternalProject)
        ExternalProject_Add(taosadapter
            PREFIX "taosadapter"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
            BUILD_ALWAYS off
            DEPENDS ${TAOS_LIB}
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake -E echo "taosadapter no need cmake to config"
            PATCH_COMMAND
            COMMAND git clean -f -d
            BUILD_COMMAND
            COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -ldflags "-s -w -X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
            COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -o taosadapter-debug -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
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
    ELSE()
        MESSAGE("Building taosAdapter on non-Windows")
        INCLUDE(ExternalProject)
        if(${TD_LINUX})
            set(ext_taosadapter_static taosadapter.a)
        elseif(${TD_DARWIN})
            set(ext_taosadapter_static taosadapter.a)
        elseif(${TD_WINDOWS})
            set(ext_taosadapter_static taosadapter.lib)
        endif()
        INIT_EXT(ext_taosadapter
            INC_DIR          include
            LIB              lib/${ext_taosadapter_static}
        )
        # GIT_REPOSITORY https://github.com/taosdata/taosadapter.git
        # GIT_TAG 3.0
        get_from_local_repo_if_exists("https://github.com/taosdata/taosadapter.git")
        ExternalProject_Add(ext_taosadapter
            GIT_REPOSITORY ${_git_url}
            GIT_TAG 3.0
            PREFIX "${_base}"
            BUILD_IN_SOURCE TRUE
            DEPENDS ${TAOS_LIB}
            CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
            # freemine: just download for the moment
            CONFIGURE_COMMAND
                COMMAND "${CMAKE_COMMAND}" -E echo "taosadapter no need cmake to config"
            PATCH_COMMAND
                COMMAND git clean -f -d
            BUILD_COMMAND
                COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
                # COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -o taosadapter-debug -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
            INSTALL_COMMAND
                # COMMAND "${CMAKE_COMMAND}" -E echo "Comparessing taosadapter.exe"
                # COMMAND upx taosadapter || :
                COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter"
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different taosadapter ${CMAKE_BINARY_DIR}/build/bin/taosadapter
                COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter.toml"
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/taosadapter.toml
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/taosadapter.service
                # COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter-debug"
                # COMMAND "${CMAKE_COMMAND}" -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
            GIT_SHALLOW TRUE
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
        # add_dependencies(build_externals ext_taosadapter)     # this is for github workflow in cache-miss step.
    ENDIF()
ENDIF()

IF(TD_BUILD_KEEPER)
    MESSAGE("")
    MESSAGE("${Green} build taoskeeper, current platform is ${PLATFORM_ARCH_STR} ${ColourReset}")

    EXECUTE_PROCESS(
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/keeper
        COMMAND git rev-parse HEAD
        OUTPUT_VARIABLE taoskeeper_commit_sha1
    )

    IF("${taoskeeper_commit_sha1}" STREQUAL "")
        SET(taoskeeper_commit_sha1 "unknown")
    ELSE()
        STRING(STRIP "${taoskeeper_commit_sha1}" taoskeeper_commit_sha1)
    ENDIF()

    SET(taos_version ${TD_VER_NUMBER})
    MESSAGE("${Green} taoskeeper will use ${taos_version} and commit ${taoskeeper_commit_sha1} as version ${ColourReset}")
    MESSAGE(" current source dir is ${CMAKE_CURRENT_SOURCE_DIR}")

    IF(TD_WINDOWS)
        MESSAGE("Building taoskeeper on Windows")
        INCLUDE(ExternalProject)
        ExternalProject_Add(taoskeeper
            PREFIX "taoskeeper"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/keeper
            BUILD_ALWAYS off
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake -E echo "taoskeeper no need cmake to config"
            PATCH_COMMAND
            COMMAND git clean -f -d
            BUILD_COMMAND
            COMMAND go build -a -ldflags "-X 'github.com/taosdata/taoskeeper/version.Version=${taos_version}' -X 'github.com/taosdata/taoskeeper/version.Gitinfo=${taoskeeper_commit_sha1}' -X 'github.com/taosdata/taoskeeper/version.CommitID=${taoskeeper_commit_sha1}' -X 'github.com/taosdata/taoskeeper/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
            INSTALL_COMMAND
            COMMAND cmake -E echo "Comparessing taoskeeper.exe"
            COMMAND cmake -E time upx taoskeeper.exe
            COMMAND cmake -E echo "Copy taoskeeper.exe"
            COMMAND cmake -E copy taoskeeper.exe ${CMAKE_BINARY_DIR}/build/bin/taoskeeper.exe
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E echo "Copy taoskeeper.toml"
            COMMAND cmake -E copy ./config/taoskeeper.toml ${CMAKE_BINARY_DIR}/test/cfg/
        )
    ELSE()
        IF(TD_DARWIN)
            MESSAGE("Building taoskeeper on macOS")
        ELSE()
            MESSAGE("Building taoskeeper on Linux")
        ENDIF()

        INCLUDE(ExternalProject)
        ExternalProject_Add(taoskeeper
            PREFIX "taoskeeper"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/keeper
            BUILD_ALWAYS off
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake -E echo "taoskeeper no need cmake to config"
            PATCH_COMMAND
            COMMAND git clean -f -d
            BUILD_COMMAND
            COMMAND go build -a -ldflags "-X 'github.com/taosdata/taoskeeper/version.Version=${taos_version}' -X 'github.com/taosdata/taoskeeper/version.Gitinfo=${taoskeeper_commit_sha1}' -X 'github.com/taosdata/taoskeeper/version.CommitID=${taoskeeper_commit_sha1}' -X 'github.com/taosdata/taoskeeper/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
            INSTALL_COMMAND
            COMMAND cmake -E echo "Copy taoskeeper"
            COMMAND cmake -E copy taoskeeper ${CMAKE_BINARY_DIR}/build/bin
            COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E echo "Copy taoskeeper.toml"
            COMMAND cmake -E copy ./config/taoskeeper.toml ${CMAKE_BINARY_DIR}/test/cfg/
            COMMAND cmake -E echo "Copy taoskeeper.service"
            COMMAND cmake -E copy ./taoskeeper.service ${CMAKE_BINARY_DIR}/test/cfg/
        )
    ENDIF()
ENDIF()
