IF(TD_WEBSOCKET)
    MESSAGE("${Green} use libtaos-ws${ColourReset}")

        if(${TD_LINUX})
            set(ext_taosws_dll      libtaosws.so)
            set(ext_taosws_lib_from libtaosws.a)
            set(ext_taosws_lib_to   libtaosws.a)
            set(ext_taosws_link     ${ext_taosws_dll})
        elseif(${TD_DARWIN})
            set(ext_taosws_dll      libtaosws.dylib)
            set(ext_taosws_lib_from libtaosws.a)
            set(ext_taosws_lib_to   libtaosws.a)
            set(ext_taosws_link     ${ext_taosws_dll})
        elseif(${TD_WINDOWS})
            set(ext_taosws_dll      taosws.dll)
            set(ext_taosws_lib_from taosws.dll.lib)
            set(ext_taosws_lib_to   taosws.lib)
            set(ext_taosws_link     ${ext_taosws_lib_to})
        endif()

        get_from_local_repo_if_exists("https://github.com/taosdata/taos-connector-rust.git")
        ExternalProject_Add(ext_taosws
            GIT_REPOSITORY ${_git_url}
            GIT_TAG ver-3.3.6.6
            GIT_SHALLOW TRUE
            BUILD_IN_SOURCE TRUE
            DEPENDS ${TAOS_LIB}
            CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
            CONFIGURE_COMMAND
                COMMAND "${CMAKE_COMMAND}" -E echo "taosws-rs no need cmake to config"
            PATCH_COMMAND
                COMMAND git clean -f -d
            BUILD_COMMAND
                COMMAND cargo build --release --locked -p taos-ws-sys --features rustls
            INSTALL_COMMAND
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/${ext_taosws_dll} ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/${ext_taosws_dll}
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/${ext_taosws_lib_from} ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}/${ext_taosws_lib_to}
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/taosws.h ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/../include/taosws.h
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
ENDIF()

IF(TD_TAOS_TOOLS)
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

    INIT_EXT(taosadapter)
    get_from_local_repo_if_exists("https://github.com/taosdata/taosadapter.git")
    ExternalProject_Add(ext_taosadapter
        GIT_REPOSITORY ${_git_url}
        PREFIX "${_base}"
        GIT_TAG ver-3.3.6.6
        GIT_SHALLOW TRUE
        BUILD_IN_SOURCE TRUE
        DEPENDS ${TAOS_NATIVE_LIB}
        CONFIGURE_COMMAND ""
        PATCH_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
    )
ELSE()
    MESSAGE("")
    MESSAGE("${Green} use taosadapter as httpd, platform is ${PLATFORM_ARCH_STR} ${ColourReset}")

    SET(taos_version ${TD_VER_NUMBER})
    MESSAGE("CURRENT SOURCE DIR ${CMAKE_CURRENT_SOURCE_DIR}")

        MESSAGE("Building taosAdapter")
        set(_cgo_cflags_list
                "-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client"
        )
        set(_cgo_ldflags_list
                "-L${CMAKE_BINARY_DIR}/build/lib ${_rpath}"
        )
        set(_ldflags_list
                "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}'"
                "-X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
        )
        string(JOIN " " _cgo_cflags ${_cgo_cflags_list})
        string(JOIN " " _cgo_ldflags ${_cgo_ldflags_list})
        string(JOIN " " _ldflags ${_ldflags_list})
        INCLUDE(ExternalProject)
        if(${TD_LINUX})
            set(ext_taosadapter_static taosadapter.a)
            set(taosadapter_exe        taosadapter)
            set(_rpath                 "-Wl,-rpath=\$ORIGIN/../lib")
            # NOTE: remove -a for the moment, add it later if any problem found to be related
            set(_shell_cmd             sh -c "go build -ldflags \"${_ldflags} -X 'github.com/taosdata/taosadapter/v3/version.CommitID=$(git rev-parse HEAD)'\"")
        elseif(${TD_DARWIN})
            set(ext_taosadapter_static taosadapter.a)
            set(taosadapter_exe        taosadapter)
            set(_rpath                 "-Wl,-rpath,\$ORIGIN/../lib")
            # NOTE: remove -a for the moment, add it later if any problem found to be related
            set(_shell_cmd             sh -c "go build -ldflags \"${_ldflags} -X 'github.com/taosdata/taosadapter/v3/version.CommitID=$(git rev-parse HEAD)'\"")
        elseif(${TD_WINDOWS})
            set(ext_taosadapter_static taosadapter.lib)
            set(taosadapter_exe        taosadapter.exe)
            set(_rpath                 "")
            # NOTE: remove -a for the moment, add it later if any problem found to be related
            set(_shell_cmd             cmd /c for /f %i in ('git rev-parse HEAD') do go build -ldflags "${_ldflags} -X 'github.com/taosdata/taosadapter/v3/version.CommitID=%i'")
        endif()

        MESSAGE("BUILD COMMAND ${_shell_cmd}")
        # GIT_REPOSITORY https://github.com/taosdata/taosadapter.git
        # GIT_TAG ver-3.3.6.6
        INIT_EXT(taosadapter)
        get_from_local_repo_if_exists("https://github.com/taosdata/taosadapter.git")
        ExternalProject_Add(ext_taosadapter
            GIT_REPOSITORY ${_git_url}
            PREFIX "${_base}"
            GIT_TAG ver-3.3.6.6
            GIT_SHALLOW TRUE
            BUILD_IN_SOURCE TRUE
            DEPENDS ${TAOS_NATIVE_LIB}
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
            CONFIGURE_COMMAND
                COMMAND "${CMAKE_COMMAND}" -E echo "taosadapter no need cmake to config"
            PATCH_COMMAND
                COMMAND git clean -f -d
            BUILD_COMMAND
                COMMAND "${CMAKE_COMMAND}" -E env "CGO_CFLAGS=${_cgo_cflags}"
                                                  "CGO_LDFLAGS=${_cgo_ldflags}"
                        ${_shell_cmd}
            INSTALL_COMMAND
                # COMMAND "${CMAKE_COMMAND}" -E echo "Comparessing taosadapter.exe"
                # COMMAND upx taosadapter || :
                COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter"
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${taosadapter_exe} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${taosadapter_exe}
                COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter.toml"
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/taosadapter.toml
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/taosadapter.service
                # COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter-debug"
                # COMMAND "${CMAKE_COMMAND}" -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
        add_custom_target(build_ext_taosadapter ALL)
        add_dependencies(build_ext_taosadapter ext_taosadapter)
ENDIF()

IF(TD_BUILD_KEEPER)
    MESSAGE("")
    MESSAGE("${Green} build taoskeeper, current platform is ${PLATFORM_ARCH_STR} ${ColourReset}")

    EXECUTE_PROCESS(
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

        IF(TD_DARWIN)
            MESSAGE("Building taoskeeper on macOS")
            set(taoskeeper_exe          taoskeeper)
        ELSEIF(TD_WINDOWS)
            MESSAGE("Building taoskeeper on Windows")
            set(taoskeeper_exe          taoskeeper.exe)
        ELSE()
            MESSAGE("Building taoskeeper on Linux")
            set(taoskeeper_exe          taoskeeper)
        ENDIF()

        set(_ldflags_list
            "-X 'github.com/taosdata/taoskeeper/version.Version=${taos_version}'"
            "-X 'github.com/taosdata/taoskeeper/version.Gitinfo=${taoskeeper_commit_sha1}'"
            "-X 'github.com/taosdata/taoskeeper/version.CommitID=${taoskeeper_commit_sha1}'"
            "-X 'github.com/taosdata/taoskeeper/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
        )
        string(JOIN " " _ldflags ${_ldflags_list})

        ExternalProject_Add(taoskeeper
            PREFIX "taoskeeper"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/keeper
            BUILD_ALWAYS off
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND cmake -E echo "taoskeeper no need cmake to config"
            PATCH_COMMAND
            COMMAND git clean -f -d
            BUILD_COMMAND
            # NOTE: remove -a for the moment, add it later if any problem found to be related
            # TODO: tweak the pitfall, that fail to rebuild even source file is modified
            COMMAND go build -ldflags "${_ldflags}"
            INSTALL_COMMAND
            COMMAND cmake -E echo "Copy taoskeeper"
            COMMAND cmake -E copy_if_different ${taoskeeper_exe} ${CMAKE_BINARY_DIR}/build/bin/${taoskeeper_exe}
            COMMAND cmake -E echo "Copy taoskeeper.toml"
            COMMAND cmake -E copy_if_different ./config/taoskeeper.toml ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.toml
            COMMAND cmake -E echo "Copy taoskeeper.service"
            COMMAND cmake -E copy_if_different ./taoskeeper.service ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.service
        )
    add_custom_target(build_taoskeeper ALL)
    add_dependencies(build_taoskeeper taoskeeper)
ENDIF()
