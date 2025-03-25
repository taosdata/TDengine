IF(TD_WEBSOCKET)
    MESSAGE("${Green} use libtaos-ws${ColourReset}")

    IF(TD_ALPINE)
        message(FATAL_ERROR         "freemine: not implemented yet")
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
    ELSE()
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

        macro(DEP_ext_taosws tgt)        # {
          target_include_directories(${tgt}
              PUBLIC ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/../include/taosws)
          target_link_libraries(${tgt}
              PRIVATE ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}/${ext_taosws_link})
          add_dependencies(${tgt} ext_taosws)
        endmacro()                       # }

        get_from_local_repo_if_exists("https://github.com/taosdata/taos-connector-rust.git")
        ExternalProject_Add(ext_taosws
            GIT_REPOSITORY ${_git_url}
            # freemine: GIT_TAG 3.0           # freemine: better not use branch name!!!
            # freemine: GIT_SHALLOW TRUE
            GIT_TAG 76004da2573eb3e854657168834ea73723326177
            GIT_SHALLOW FALSE
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
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/${ext_taosws_dll} ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/${ext_taosws_dll}
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/${ext_taosws_lib_from} ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}/${ext_taosws_lib_to}
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/taosws.h ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/../include/taosws/taosws.h
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
        add_library(ext_taosws_imp STATIC IMPORTED)
        set_target_properties(ext_taosws_imp PROPERTIES
            IMPORTED_LOCATION
                ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/${ext_taosws_dll}
                ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}/${ext_taosws_lib_to}
                ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/../include/taosws/taosws.h
        )
    ENDIF()
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

    # NOTE: TD_VER_GIT or TD_VER_GIT_INTERNAL in cmake/cmake.version?
    EXECUTE_PROCESS(
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

    # freemine: IF(TD_WINDOWS)
    # freemine:     MESSAGE("Building taosAdapter on Windows")
    # freemine:     message(FATAL_ERROR         "freemine: not implemented yet")
    # freemine:     INCLUDE(ExternalProject)
    # freemine:     ExternalProject_Add(taosadapter
    # freemine:         PREFIX "taosadapter"
    # freemine:         SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
    # freemine:         BUILD_ALWAYS off
    # freemine:         DEPENDS ${TAOS_LIB}
    # freemine:         BUILD_IN_SOURCE 1
    # freemine:         CONFIGURE_COMMAND cmake -E echo "taosadapter no need cmake to config"
    # freemine:         PATCH_COMMAND
    # freemine:         COMMAND git clean -f -d
    # freemine:         BUILD_COMMAND
    # freemine:         COMMAND set CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client
    # freemine:         COMMAND set CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib
    # freemine:         COMMAND go build -a -o taosadapter.exe -ldflags "-s -w -X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
    # freemine:         COMMAND go build -a -o taosadapter-debug.exe -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"

    # freemine:         INSTALL_COMMAND
    # freemine:         COMMAND cmake -E echo "Comparessing taosadapter.exe"
    # freemine:         COMMAND cmake -E time upx taosadapter.exe
    # freemine:         COMMAND cmake -E echo "Copy taosadapter.exe"
    # freemine:         COMMAND cmake -E copy taosadapter.exe ${CMAKE_BINARY_DIR}/build/bin/taosadapter.exe
    # freemine:         COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
    # freemine:         COMMAND cmake -E echo "Copy taosadapter.toml"
    # freemine:         COMMAND cmake -E copy ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
    # freemine:         COMMAND cmake -E echo "Copy taosadapter-debug.exe"
    # freemine:         COMMAND cmake -E copy taosadapter-debug.exe ${CMAKE_BINARY_DIR}/build/bin
    # freemine:     )
    # freemine: # freemine: ELSEIF(TD_DARWIN)
    # freemine: # freemine:     MESSAGE("Building taosAdapter on MACOS")
    # freemine: # freemine:     message(FATAL_ERROR         "freemine: not implemented yet")
    # freemine: # freemine:     INCLUDE(ExternalProject)
    # freemine: # freemine:     ExternalProject_Add(taosadapter
    # freemine: # freemine:         PREFIX "taosadapter"
    # freemine: # freemine:         SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/taosadapter
    # freemine: # freemine:         BUILD_ALWAYS off
    # freemine: # freemine:         DEPENDS ${TAOS_LIB}
    # freemine: # freemine:         BUILD_IN_SOURCE 1
    # freemine: # freemine:         CONFIGURE_COMMAND cmake -E echo "taosadapter no need cmake to config"
    # freemine: # freemine:         PATCH_COMMAND
    # freemine: # freemine:         COMMAND git clean -f -d
    # freemine: # freemine:         BUILD_COMMAND
    # freemine: # freemine:         COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -ldflags "-s -w -X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
    # freemine: # freemine:         COMMAND CGO_CFLAGS=-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client CGO_LDFLAGS=-L${CMAKE_BINARY_DIR}/build/lib go build -a -o taosadapter-debug -ldflags "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}' -X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}' -X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
    # freemine: # freemine:         INSTALL_COMMAND
    # freemine: # freemine:         COMMAND cmake -E echo "Copy taosadapter"
    # freemine: # freemine:         COMMAND cmake -E copy taosadapter ${CMAKE_BINARY_DIR}/build/bin
    # freemine: # freemine:         COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
    # freemine: # freemine:         COMMAND cmake -E echo "Copy taosadapter.toml"
    # freemine: # freemine:         COMMAND cmake -E copy ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/
    # freemine: # freemine:         COMMAND cmake -E copy ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/
    # freemine: # freemine:         COMMAND cmake -E echo "Copy taosadapter-debug"
    # freemine: # freemine:         COMMAND cmake -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
    # freemine: # freemine:     )
    # freemine: ELSE()
        MESSAGE("Building taosAdapter on non-Windows")
        INCLUDE(ExternalProject)
        if(${TD_LINUX})
            set(ext_taosadapter_static taosadapter.a)
            set(taosadapter_exe        taosadapter)
            set(_rpath                 "-Wl,-rpath=\$ORIGIN/../lib")
        elseif(${TD_DARWIN})
            set(ext_taosadapter_static taosadapter.a)
            set(taosadapter_exe        taosadapter)
            set(_rpath                 "-Wl,-rpath,\$ORIGIN/../lib")
        elseif(${TD_WINDOWS})
            set(ext_taosadapter_static taosadapter.lib)
            set(taosadapter_exe        taosadapter.exe)
            set(_rpath                 "")
        endif()
        set(_cgo_cflags_list
            "-I${CMAKE_CURRENT_SOURCE_DIR}/../include/client"
        )
        set(_cgo_ldflags_list
            "-L${CMAKE_BINARY_DIR}/build/lib ${_rpath}"
        )
        set(_ldflags_list
            "-X 'github.com/taosdata/taosadapter/v3/version.Version=${taos_version}'"
            "-X 'github.com/taosdata/taosadapter/v3/version.CommitID=${taosadapter_commit_sha1}'"
            "-X 'github.com/taosdata/taosadapter/v3/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
        )
        string(JOIN " " _cgo_cflags ${_cgo_cflags_list})
        string(JOIN " " _cgo_ldflags ${_cgo_ldflags_list})
        string(JOIN " " _ldflags ${_ldflags_list})

        # GIT_REPOSITORY https://github.com/taosdata/taosadapter.git
        # GIT_TAG 3.0
        get_from_local_repo_if_exists("https://github.com/taosdata/taosadapter.git")
        ExternalProject_Add(ext_taosadapter
            GIT_REPOSITORY ${_git_url}
            # freemine: GIT_TAG main   # freemine: better not use branch name
            # freemine: GIT_SHALLOW TRUE
            GIT_TAG 4c4f7a911a9c933e8ff9f9519667d65bed696310
            GIT_SHALLOW FALSE
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
                # freemine: remove -a option
                COMMAND "${CMAKE_COMMAND}" -E env "CGO_CFLAGS=${_cgo_cflags}"
                                                  "CGO_LDFLAGS=${_cgo_ldflags}"
                        go build -ldflags "${_ldflags}"
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
    # freemine: ENDIF()
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

    # freemine: IF(TD_WINDOWS)
    # freemine:     MESSAGE("Building taoskeeper on Windows")
    # freemine:     INCLUDE(ExternalProject)
    # freemine:     ExternalProject_Add(taoskeeper
    # freemine:         PREFIX "taoskeeper"
    # freemine:         SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/keeper
    # freemine:         BUILD_ALWAYS off
    # freemine:         BUILD_IN_SOURCE 1
    # freemine:         CONFIGURE_COMMAND cmake -E echo "taoskeeper no need cmake to config"
    # freemine:         PATCH_COMMAND
    # freemine:         COMMAND git clean -f -d
    # freemine:         BUILD_COMMAND
    # freemine:         COMMAND go build -a -ldflags "-X 'github.com/taosdata/taoskeeper/version.Version=${taos_version}' -X 'github.com/taosdata/taoskeeper/version.Gitinfo=${taoskeeper_commit_sha1}' -X 'github.com/taosdata/taoskeeper/version.CommitID=${taoskeeper_commit_sha1}' -X 'github.com/taosdata/taoskeeper/version.BuildInfo=${TD_VER_OSTYPE}-${TD_VER_CPUTYPE} ${TD_VER_DATE}'"
    # freemine:         INSTALL_COMMAND
    # freemine:         COMMAND cmake -E echo "Comparessing taoskeeper.exe"
    # freemine:         COMMAND cmake -E time upx taoskeeper.exe
    # freemine:         COMMAND cmake -E echo "Copy taoskeeper.exe"
    # freemine:         COMMAND cmake -E copy taoskeeper.exe ${CMAKE_BINARY_DIR}/build/bin/taoskeeper.exe
    # freemine:         COMMAND cmake -E make_directory ${CMAKE_BINARY_DIR}/test/cfg/
    # freemine:         COMMAND cmake -E echo "Copy taoskeeper.toml"
    # freemine:         COMMAND cmake -E copy ./config/taoskeeper.toml ${CMAKE_BINARY_DIR}/test/cfg/
    # freemine:     )
    # freemine: ELSE()
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
            COMMAND go build -a -ldflags "${_ldflags}"
            INSTALL_COMMAND
            COMMAND cmake -E echo "Copy taoskeeper"
            COMMAND cmake -E copy_if_different ${taoskeeper_exe} ${CMAKE_BINARY_DIR}/build/bin/${taoskeeper_exe}
            COMMAND cmake -E echo "Copy taoskeeper.toml"
            COMMAND cmake -E copy_if_different ./config/taoskeeper.toml ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.toml
            COMMAND cmake -E echo "Copy taoskeeper.service"
            COMMAND cmake -E copy_if_different ./taoskeeper.service ${CMAKE_BINARY_DIR}/test/cfg/taoskeeper.service
        )
    # freemine: ENDIF()
		# freemine: make taoskeeper buildable
		add_custom_target(build_taoskeeper ALL)
		add_dependencies(build_taoskeeper taoskeeper)
ENDIF()
