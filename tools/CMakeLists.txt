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
            GIT_TAG main
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
        GIT_TAG main
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

    # NOTE: do NOTE remove these lines until we know they are not useful any longer!!!
    # EXECUTE_PROCESS(
    #     COMMAND git rev-parse --abbrev-ref HEAD
    #     RESULT_VARIABLE result_taos_version
    #     OUTPUT_VARIABLE taos_version
    # )

    # STRING(FIND ${taos_version} release is_release_branch)

    # IF("${is_release_branch}" STREQUAL "0")
    #     STRING(SUBSTRING "${taos_version}" 12 -1 taos_version)
    #     STRING(STRIP "${taos_version}" taos_version)
    # ELSE()
    #     STRING(CONCAT taos_version "_branch_" "${taos_version}")
    #     STRING(STRIP "${taos_version}" taos_version)
    # ENDIF()

    # NOTE: TD_VER_GIT or TD_VER_GIT_INTERNAL in cmake/version.cmake?
    SET(taos_version ${TD_VER_NUMBER})
    MESSAGE("CURRENT SOURCE DIR ${CMAKE_CURRENT_SOURCE_DIR}")

    MESSAGE("Building taosAdapter")

    if(${TD_LINUX})
        set(ext_taosadapter_static taosadapter.a)
        set(taosadapter_exe        taosadapter)
    elseif(${TD_DARWIN})
        set(ext_taosadapter_static taosadapter.a)
        set(taosadapter_exe        taosadapter)
    elseif(${TD_WINDOWS})
        set(ext_taosadapter_static taosadapter.lib)
        set(taosadapter_exe        taosadapter.exe)
    endif()

    # NOTE: for taos.h, but what'bout others, such as taoserror.h!!!
    # TODO: for TDengine.git, moving public headers, such as taoserror.h/taos.h, to the same public header directory!!!
    set(_taos_h_dir       "${CMAKE_CURRENT_SOURCE_DIR}/../include/client")

    # NOTE: where to find DSO built by ${TAOS_NATIVE_LIB}
    set(_taos_lib_dir     "${CMAKE_BINARY_DIR}/build/lib")

    # NOTE: CMakeLists.txt to be copied to taosadapter-src-tree, to make it builtable by cmake
    #       so that we can pass arguments via CMAKE_ARGS in a structural way
    # TODO: shall we make taosadapter to provide CMakeLists.txt in it's own way
    #       so that we don't have to hand-make such process
    set(_cmakelists       "${CMAKE_CURRENT_SOURCE_DIR}/../cmake/in/taosadapter.CMakeLists.txt.in")

    # GIT_REPOSITORY https://github.com/taosdata/taosadapter.git
    # GIT_TAG 3.0
    # get_from_local_repo_if_exists("https://github.com/taosdata/taosadapter.git")

    # NOTE: it's strongly suggested to use main branch of taosadapter.git during developping process
    #       and use by tag only when building release version
    #       thus we use _git_url literally
    # TODO: switch among develop-build and release-build process, smoothly and safely?
    set(_git_url "https://github.com/taosdata/taosadapter.git")

    ExternalProject_Add(ext_taosadapter
        GIT_REPOSITORY ${_git_url}
        GIT_TAG main
        GIT_SHALLOW TRUE       # NOTE: if you specify branch other than main, please change this to FALSE
        BUILD_IN_SOURCE TRUE
        DEPENDS ${TAOS_NATIVE_LIB}
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${CMAKE_BINARY_DIR}/build/lib
        CMAKE_ARGS -DTAOS_H_DIR:STRING=${_taos_h_dir}
        CMAKE_ARGS -DTAOS_LIB_DIR:STRING=${_taos_lib_dir}
        CMAKE_ARGS -DTAOS_VERSION:STRING=${taos_version}
        CMAKE_ARGS -DTD_VER_OSTYPE:STRING=${TD_VER_OSTYPE}
        CMAKE_ARGS -DTD_VER_CPUTYPE:STRING=${TD_VER_CPUTYPE}
        CMAKE_ARGS -DTD_VER_DATE:STRING=${TD_VER_DATE}
        CMAKE_ARGS -DTD_GO_BUILD_OPTIONS:STRING=${TAOSADAPTER_BUILD_OPTIONS}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${_cmakelists} ./CMakeLists.txt
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E echo "NOTE: we don't install in this way"
            # COMMAND "${CMAKE_COMMAND}" -E echo "Comparessing taosadapter.exe"
            # COMMAND upx taosadapter || :
            COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter"
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${taosadapter_exe} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${taosadapter_exe}
            COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter.toml"
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./example/config/taosadapter.toml ${CMAKE_BINARY_DIR}/test/cfg/taosadapter.toml
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./taosadapter.service ${CMAKE_BINARY_DIR}/test/cfg/taosadapter.service
            # COMMAND "${CMAKE_COMMAND}" -E echo "Copy taosadapter-debug"
            # COMMAND "${CMAKE_COMMAND}" -E copy taosadapter-debug ${CMAKE_BINARY_DIR}/build/bin
        EXCLUDE_FROM_ALL FALSE
        VERBATIM
    )
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
            COMMAND go build -a -ldflags "${_ldflags}"
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
