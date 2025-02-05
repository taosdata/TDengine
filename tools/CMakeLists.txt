IF(TD_WEBSOCKET)
    IF(TD_LINUX)
        SET(websocket_lib_file "libtaosws.so")
    ELSEIF(TD_DARWIN)
        SET(websocket_lib_file "libtaosws.dylib")
    ELSEIF(TD_WINDOWS)
        SET(websocket_lib_file "taosws.lib")
    ENDIF()

    MESSAGE("${Green} use libtaos-ws${ColourReset}")

    if(${FALLBACK})                    # {
        IF(TD_ALPINE)
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
                COMMAND cp target/release/${websocket_lib_file} ${CMAKE_BINARY_DIR}/build/lib
                COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/build/include
                COMMAND cmake -E copy target/release/taosws.h ${CMAKE_BINARY_DIR}/build/include
            )
        ENDIF()
    else()                             # }{
        set(_taosws_so              "${CMAKE_BINARY_DIR}/build/lib/${websocket_lib_file}")     # freemine: harded-coded to remove?
        set(_taosws_h               "${CMAKE_BINARY_DIR}/build/include/taosws.h")     # freemine: harded-coded to remove?
        INIT_EXT(ext_taosws
            INC_DIR           include
            LIB               lib/${websocket_lib_file}
        )
        ExternalProject_Add(ext_taosws
            GIT_REPOSITORY https://github.com/taosdata/taos-connector-rust.git
            GIT_TAG main # freemine: or fix?
            PREFIX "${_base}"
            BUILD_IN_SOURCE TRUE
            CONFIGURE_COMMAND ""
            BUILD_COMMAND
              # COMMAND cargo update
              COMMAND cargo build --release -p taos-ws-sys --features rustls
            INSTALL_COMMAND
              COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/taosws.h ${_ins}/include/taosws.h
              COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/${websocket_lib_file} ${_ins}/lib/${websocket_lib_file}
              COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/taosws.h ${_taosws_h}
              COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/${websocket_lib_file} ${_taosws_so}
            GIT_SHALLOW true
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
    endif()                            # }
ENDIF()

IF(TD_TAOS_TOOLS)
    if(${FALLBACK})                    # {
        INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/tools/taos_tools/deps/avro/lang/c/src)
        INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/client)
        INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/common)
        INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/util)
        INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/os)
        INCLUDE_DIRECTORIES(${TD_SOURCE_DIR}/include/libs/transport)
        ADD_SUBDIRECTORY(taos-tools)
    else()                             # }{
        INIT_EXT(ext_taos_tools)
        ExternalProject_Add(ext_taos_tools
            GIT_REPOSITORY https://github.com/taosdata/taos-tools.git
            GIT_TAG main # freemine: or fix?
            PREFIX "${_base}"
            DEPENDS ${TAOS_LIB}
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}            # freemine: need to refactor taos-tools.git
            CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel      # freemine: need to refactor taos-tools.git
            CMAKE_ARGS -DTAOS_BUILD_DIR:STRING=${CMAKE_BINARY_DIR}/build    # freemine: tweak
            PATCH_COMMAND
                COMMAND git restore -- src/CMakeLists.txt
                COMMAND git apply ${CMAKE_SOURCE_DIR}/contrib/taostools.diff   # freemine: tweak
            INSTALL_COMMAND ""    # freemine: need to refactor taos-tools.git INSTALL
            GIT_SHALLOW true
            EXCLUDE_FROM_ALL
            VERBATIM
        )
        add_dependencies(ext_taos_tools ${TAOS_LIB} ext_taosws)
        set(_taosBenchmark          "${CMAKE_BINARY_DIR}/build/bin/taosBenchmark")          # freemine: harded-coded to remove?
        set(_taosdump               "${CMAKE_BINARY_DIR}/build/bin/taosdump")               # freemine: harded-coded to remove?
        add_custom_command(
            OUTPUT ${_taosBenchmark}
                   ${_taosdump}
            WORKING_DIRECTORY ${_base}/src/ext_taos_tools-build/build/bin
            DEPENDS ext_taos_tools
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different taosBenchmark ${_taosBenchmark}
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different taosdump ${_taosdump}
            VERBATIM
        )
        add_custom_target(
            ext_taos_tools_builder
            ALL
            DEPENDS ${_taosBenchmark}
                    ${_taosdump}
        )
    endif()                            # }
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

    if(${FALLBACK})         # {
        EXECUTE_PROCESS(
            COMMAND cd ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
        )
        EXECUTE_PROCESS(
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
            COMMAND git rev-parse HEAD
            RESULT_VARIABLE commit_sha1
            OUTPUT_VARIABLE taosadapter_commit_sha1
        )
    else()                  # }{
        INIT_EXT(ext_taosadapter)
        ExternalProject_Add(ext_taosadapter
            GIT_REPOSITORY https://github.com/taosdata/taosadapter.git
            GIT_TAG main # freemine: or fix?
            PREFIX "${_base}"
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            INSTALL_COMMAND ""
            GIT_SHALLOW true
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
        EXECUTE_PROCESS(
            WORKING_DIRECTORY ${_base}/src/ext_taosadapter
            COMMAND git rev-parse HEAD
            RESULT_VARIABLE commit_sha1
            OUTPUT_VARIABLE taosadapter_commit_sha1
        )
    endif()                 # }

    IF("${taosadapter_commit_sha1}" STREQUAL "")
        SET(taosadapter_commit_sha1 "unknown")
    ELSE()
        # STRING(SUBSTRING "${taosadapter_commit_sha1}" 0 7 taosadapter_commit_sha1)
        STRING(STRIP "${taosadapter_commit_sha1}" taosadapter_commit_sha1)
    ENDIF()

    SET(taos_version ${TD_VER_NUMBER})
    MESSAGE("${Green} taosAdapter will use ${taos_version} and commit ${taosadapter_commit_sha1} as version ${ColourReset}")
    if(${FALLBACK})          # {
        EXECUTE_PROCESS(
            COMMAND cd ..
        )
        MESSAGE("CURRENT SOURCE DIR ${CMAKE_CURRENT_SOURCE_DIR}")
    else()                   # }{
        MESSAGE("CURRENT SOURCE DIR ${_base}/src/ext_taosadapter")
    endif()                  # }

    if(${FALLBACK})                    # {
        IF(TD_WINDOWS)
            MESSAGE("Building taosAdapter on Windows")
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
                COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"

                # COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib  go build -a -o taosadapter-debug -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
                INSTALL_COMMAND

                # COMMAND cmake -E echo "Comparessing taosadapter.exe"
                # COMMAND upx taosadapter || :
                COMMAND cmake -E echo "Copy taosadapter"
                COMMAND cmake -E copy taosadapter ${CMAKE_BINARY_DIR}/build/bin
                COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E echo "Copy taosadapter.toml"
                COMMAND cmake -E copy ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
                COMMAND cmake -E copy ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/

                # COMMAND cmake -E echo "Copy taosadapter-debug"
                # COMMAND cmake -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
            )
        ENDIF()
    else()                             # }{
        set(_cgo_cflags   "-I${CMAKE_BINARY_DIR}/build/include")       # freemine: hard-coded to remove?
        set(_cgo_ldflags  "-L${CMAKE_BINARY_DIR}/build/lib -ltaos")
        string(JOIN " " _ldflags
            "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}'"
            "-X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}'"
            "-X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
            "-r '$ORIGIN/../lib'"
        )
        set(_taosadapter            "${CMAKE_BINARY_DIR}/build/bin/taosadapter")            # freemine: harded-coded to remove?
        set(_taosadapter_service    "${CMAKE_BINARY_DIR}/test/cfg/taosadapter.service")     # freemine: harded-coded to remove?
        add_custom_command(
            OUTPUT ${_taosadapter}
                   ${_taosadapter_service}
            WORKING_DIRECTORY ${_base}/src/ext_taosadapter
            DEPENDS ${TAOS_LIB}
            COMMAND "${CMAKE_COMMAND}" -E env
                CGO_CFLAGS=${_cgo_cflags}
                CGO_LDFLAGS=${_cgo_ldflags}
                go build -ldflags ${_ldflags}
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different taosadapter ${_taosadapter}
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different taosadapter.service ${_taosadapter_service}
            VERBATIM
        )
        add_custom_target(
            ext_taosadapter_builder
            ALL
            DEPENDS ext_taosadapter
                    ${_taosadapter}
                    ${_taosadapter_service}
        )
    endif()                            # }
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

    if(${FALLBACK})          # {
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
    else()                             # }{
        set(_taoskeeper             ${CMAKE_BINARY_DIR}/build/bin/taoskeeper)
        set(_taoskeeper_toml        ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.toml)
        set(_taoskeeper_service     ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.service)
        string(JOIN " " _ldflags
            "-X 'github.com/taosdata/taoskeeper/version.Version=${taos_version}'"
            "-X 'github.com/taosdata/taoskeeper/version.Gitinfo=${taoskeeper_commit_sha1}'"
            "-X 'github.com/taosdata/taoskeeper/version.CommitID=${taoskeeper_commit_sha1}'"
            "-X 'github.com/taosdata/taoskeeper/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
            "-r '$ORIGIN/../lib'"
        )
        add_custom_command(
            OUTPUT       ${_taoskeeper}
                         ${_taoskeeper_toml}
                         ${_taoskeeper_service}
            DEPENDS ${TAOS_LIB}
            WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/keeper"
            COMMAND "${CMAKE_COMMAND}" -E env
                go build -ldflags ${_ldflags}
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different taoskeeper ${_taoskeeper}
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./config/taoskeeper.toml ${_taoskeeper_toml}
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./taoskeeper.service ${_taoskeeper_service}
            VERBATIM
        )
        add_custom_target(
            taoskeeper_builder
            ALL
            DEPENDS      ${_taoskeeper}
                         ${_taoskeeper_toml}
                         ${_taoskeeper_service}
        )
    endif()                  # }
ENDIF()
