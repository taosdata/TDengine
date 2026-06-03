option(TD_EXTERNALS_USE_ONLY "external dependencies use only, otherwise download-build-install" OFF)
option(TD_ALIGN_EXTERNAL "keep externals' CMAKE_BUILD_TYPE align with the main project" ON)
option(EXTERNALS_USE_CCACHE "Use ccache for ExternalProject builds (set OFF if ccache corrupts .o files)" ON)

# When EXTERNALS_USE_CCACHE is OFF, prepend CCACHE_DISABLE=1 to external
# build commands so ccache passes compilations through without caching.
# Two forms: _EXT_ENV_PREFIX for direct COMMAND, _EXT_CCACHE_EXPORT for sh -c.
if(EXTERNALS_USE_CCACHE)
    set(_EXT_ENV_PREFIX)
    set(_EXT_CCACHE_EXPORT "")
else()
    set(_EXT_ENV_PREFIX ${CMAKE_COMMAND} -E env CCACHE_DISABLE=1)
    set(_EXT_CCACHE_EXPORT "export CCACHE_DISABLE=1 && ")
    message(STATUS "ccache disabled for ExternalProject builds (EXTERNALS_USE_CCACHE=OFF)")
endif()

# Keep TD_EXTERNALS_USE_ONLY synchronized with BUILD_CONTRIB across re-configures.
# Without this, cache may keep TD_EXTERNALS_USE_ONLY=ON from a previous
# BUILD_CONTRIB=OFF configure, causing later BUILD_CONTRIB=ON builds to skip
# ExternalProject dependencies unexpectedly.
if(BUILD_CONTRIB)
    set(TD_EXTERNALS_USE_ONLY OFF CACHE BOOL
        "external dependencies use only, otherwise download-build-install" FORCE)
else()
    set(TD_EXTERNALS_USE_ONLY ON CACHE BOOL
        "external dependencies use only, otherwise download-build-install" FORCE)
endif()

# eg.: cmake -B debug -DCMAKE_BUILD_TYPE:STRING=Debug
#      TD_CONFIG_NAME will be `Debug`
#   for multi-configuration tools, such as `Visual Studio ...`
#      cmake --build build --config Release
#      TD_CONFIG_NAME will be `Release`
set(TD_CONFIG_NAME "$<IF:$<STREQUAL:z$<CONFIG>,z>,$<IF:$<STREQUAL:z${CMAKE_BUILD_TYPE},z>,Debug,${CMAKE_BUILD_TYPE}>,$<CONFIG>>")
# Configure-time resolved equivalent of TD_CONFIG_NAME.
# Generator expressions in TD_CONFIG_NAME are only evaluated at build time,
# so file(EXISTS) / if(NOT EXISTS) checks need this plain-string version.
if(CMAKE_BUILD_TYPE STREQUAL "")
    set(TD_CONFIG_NAME_RESOLVED "Debug")
else()
    set(TD_CONFIG_NAME_RESOLVED "${CMAKE_BUILD_TYPE}")
endif()
if(NOT TD_ALIGN_EXTERNAL)
    if(NOT TD_WINDOWS)
        set(TD_CONFIG_NAME "Release")
        set(TD_CONFIG_NAME_RESOLVED "Release")
    endif()
endif()

set(TD_EXTERNALS_BASE_DIR "${CMAKE_SOURCE_DIR}/.externals" CACHE PATH "path where external dependencies reside")
message(STATUS "TD_EXTERNALS_BASE_DIR:${TD_EXTERNALS_BASE_DIR}")

set(TD_INTERNALS_BASE_DIR "${CMAKE_SOURCE_DIR}/.internals" CACHE PATH "path where internal dependencies reside")
message(STATUS "TD_INTERNALS_BASE_DIR:${TD_INTERNALS_BASE_DIR}")

set(TD_ROCKSDB_DEPS_DIR "${TD_SOURCE_DIR}/deps/${TD_DEPS_DIR}/rocksdb_static")
set(TD_ROCKSDB_USE_DEPS OFF)
set(TD_ROCKSDB_USE_EXTERNAL OFF)
set(TD_ROCKSDB_BUILD_FROM_SOURCE OFF)
if(TD_USE_ROCKSDB)
    if(BUILD_ROCKSDB)
        if(NOT BUILD_CONTRIB)
            message(FATAL_ERROR
                "[rocksdb] Invalid option combination: BUILD_ROCKSDB=ON requires BUILD_CONTRIB=ON.\n"
                "  Either set -DBUILD_CONTRIB=ON to enable building all externals from source,\n"
                "  or set -DBUILD_ROCKSDB=OFF to use a prebuilt RocksDB.")
        endif()
        # BUILD_CONTRIB=ON + BUILD_ROCKSDB=ON: download and compile via ExternalProject
        set(TD_ROCKSDB_USE_EXTERNAL ON)
        set(TD_ROCKSDB_BUILD_FROM_SOURCE ON)
    elseif(ROCKSDB_USE_DEPS)
        # Use prebuilt rocksdb from deps/ directory
        if(NOT EXISTS "${TD_ROCKSDB_DEPS_DIR}")
            message(FATAL_ERROR
                "[rocksdb] ROCKSDB_USE_DEPS=ON but prebuilt deps not found at:\n"
                "  ${TD_ROCKSDB_DEPS_DIR}\n"
                "  Either provide the prebuilt library or set -DROCKSDB_USE_DEPS=OFF.")
        endif()
        set(TD_ROCKSDB_USE_DEPS ON)
    else()
        # ROCKSDB_USE_DEPS=OFF: use previously-built ExternalProject artifacts from .externals/
        set(TD_ROCKSDB_USE_EXTERNAL ON)
    endif()
endif()
message(STATUS
    "[rocksdb] TD_USE_ROCKSDB=${TD_USE_ROCKSDB}, BUILD_ROCKSDB=${BUILD_ROCKSDB}, "
    "BUILD_CONTRIB=${BUILD_CONTRIB}, ROCKSDB_USE_DEPS=${ROCKSDB_USE_DEPS}, "
    "use_deps=${TD_ROCKSDB_USE_DEPS}, use_external=${TD_ROCKSDB_USE_EXTERNAL}, "
    "build_from_source=${TD_ROCKSDB_BUILD_FROM_SOURCE}"
)

include(ExternalProject)
set_directory_properties(PROPERTIES EP_UPDATE_DISCONNECTED TRUE)

add_custom_target(build_externals)

macro(DEP_td_rocksdb tgt)   # {
    if(TD_USE_ROCKSDB)
        if(TD_ROCKSDB_USE_EXTERNAL)
            DEP_ext_rocksdb(${tgt})
        elseif(TD_ROCKSDB_USE_DEPS)
            target_include_directories(${tgt} PUBLIC "${TD_ROCKSDB_DEPS_DIR}")
            target_link_libraries(${tgt} PRIVATE "${TD_ROCKSDB_DEPS_DIR}/librocksdb.a")
        endif()
    endif()
endmacro()                  # }

macro(INIT_DIRS name base_dir)     # {
    set(_base            "${base_dir}/build/${CMAKE_BUILD_TYPE}/${name}") # per-build-type isolation (source+stamp+build)
    set(_ins             "${base_dir}/install/${name}/${TD_CONFIG_NAME}")  # where all installed stuffs locate
    set(${name}_base     "${_base}")
    set(${name}_source   "${_base}/src/${name}")
    set(${name}_build    "${_base}/src/${name}-build")
    set(${name}_install  "${_ins}")
endmacro()                         # }

# eg.: INIT_EXT(ext_zlib)
# initialization all variables to be used by external project and those relied on
macro(INIT_EXT name)               # {
    INIT_DIRS(${name} ${TD_EXTERNALS_BASE_DIR})
    set(${name}_inc_dir  "")
    set(${name}_libs     "")
    set(${name}_have_dev          FALSE)
    set(${name}_build_contrib     FALSE)

    set(options)
    set(oneValueArgs INC_DIR)
    set(multiValueArgs LIB CHK_NAME)
    cmake_parse_arguments(arg_INIT_EXT
        "${options}" "${oneValueArgs}" "${multiValueArgs}"
        ${ARGN}
    )

    if(NOT "${HAVE_DEV_${arg_INIT_EXT_CHK_NAME}}")
      set(${name}_have_dev   FALSE)
    else()
      set(${name}_have_dev   TRUE)
    endif()

    if(BUILD_CONTRIB OR TD_EXTERNALS_USE_ONLY OR NOT ${${name}_have_dev})
      set(${name}_build_contrib     TRUE)
    else()
      set(${name}_build_contrib     FALSE)
    endif()

    message(STATUS
      "[external] ${name}: BUILD_CONTRIB=${BUILD_CONTRIB}, "
      "HAVE_DEV_${arg_INIT_EXT_CHK_NAME}=${${name}_have_dev}, "
      "build_from_source=${${name}_build_contrib}"
    )

    if(${${name}_build_contrib})
      set(${name}_inc_dir      "${_ins}/${arg_INIT_EXT_INC_DIR}")
      foreach(v ${arg_INIT_EXT_LIB})
        list(APPEND ${name}_libs         "${_ins}/${v}")
      endforeach()

      if(NOT TD_EXTERNALS_USE_ONLY)
        add_library(${name}_imp STATIC IMPORTED)
      endif()
    else()
      set(${name}_libs "${${arg_INIT_EXT_CHK_NAME}_LIBNAMES}")
    endif()

    # eg.: DEP_ext_zlib(tgt)
    #      make tgt depend on ext_zlib, and call target_include_directories/target_link_libraries accordingly
    #      NOTE: currently, full path to the target's artifact is used, such as libz.a
    macro(DEP_${name} tgt)           # {
        cmake_language(CALL DEP_${name}_INC ${tgt})
        cmake_language(CALL DEP_${name}_LIB ${tgt})
        if(NOT TD_WINDOWS)
            target_link_libraries(${tgt} PUBLIC stdc++)
        endif()
    endmacro()                       # }
    macro(DEP_${name}_INC tgt)               # {
        if(${${name}_build_contrib})
            target_include_directories(${tgt} PUBLIC "${${name}_inc_dir}")
            if(NOT TD_EXTERNALS_USE_ONLY)     # {
                foreach(v ${${name}_libs})
                    set_target_properties(${name}_imp PROPERTIES
                        IMPORTED_LOCATION "${v}"
                    )
                endforeach()
                add_dependencies(${tgt} ${name})
            endif()                           # }
            add_definitions(-D_${name})
            if("z${name}" STREQUAL "zext_gtest")
                target_compile_features(${tgt} PUBLIC cxx_std_11)
                target_link_libraries(${tgt} PRIVATE Threads::Threads)
            endif()
        else()
            if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
                # make homebrew-installed-libs available
                target_include_directories(${tgt} PUBLIC "${BREW_PREFIX}/include")
            endif()
        endif()
    endmacro()                               # }
    macro(DEP_${name}_LIB tgt)               # {
        if(${${name}_build_contrib})
            if(NOT TD_EXTERNALS_USE_ONLY)     # {
                foreach(v ${${name}_libs})
                    set_target_properties(${name}_imp PROPERTIES
                        IMPORTED_LOCATION "${v}"
                    )
                endforeach()
                add_dependencies(${tgt} ${name})
            endif()                           # }
            foreach(v ${${name}_libs})
                target_link_libraries(${tgt} PRIVATE "${v}")
            endforeach()
            if(NOT TD_WINDOWS)       # {
              if("z${name}" STREQUAL "zext_libuv")
                  target_link_libraries(${tgt} PUBLIC dl)
              endif()
            endif()                     # }
        else()
            foreach(v ${${name}_libs})
                target_link_libraries(${tgt} PRIVATE "${v}")
            endforeach()
            if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
                # make homebrew-installed-libs available
                target_link_directories(${tgt} PUBLIC "${BREW_PREFIX}/lib")
            endif()
        endif()

        if(${TD_WINDOWS})
            if("z${name}" STREQUAL "zext_curl")
                target_link_libraries(${tgt} PRIVATE crypt32 wldap32 normaliz secur32 bcrypt)
            endif()
        endif()

        add_definitions(-D_${name})
    endmacro()                               # }
endmacro()                         # }

set(LOCAL_REPO "" CACHE STRING "local repositories storage to use")
set(LOCAL_URL "" CACHE STRING "local archives storage to use")

# Bridge BUILD_DEPS_MIRROR_URL → LOCAL_URL for backward compatibility.
# tsdb-builder passes BUILD_DEPS_MIRROR_URL; cmake code here uses LOCAL_URL.
# Must use CACHE ... FORCE because LOCAL_URL is already a CACHE variable;
# plain set() cannot overwrite it.
if(DEFINED BUILD_DEPS_MIRROR_URL AND "${LOCAL_URL}" STREQUAL "")
  set(LOCAL_URL "${BUILD_DEPS_MIRROR_URL}" CACHE STRING "local archives storage to use" FORCE)
endif()

# When BUILD_USE_PUBLIC_DEPS is ON, force LOCAL_URL and LOCAL_REPO to empty so that
# get_from_local_if_exists() / get_from_local_repo_if_exists() use original public URLs.
# This also overrides any cached values from a previous configure.
if(BUILD_USE_PUBLIC_DEPS)
  if(DEFINED BUILD_DEPS_MIRROR_URL AND NOT "${BUILD_DEPS_MIRROR_URL}" STREQUAL "")
    message(WARNING
      "BUILD_USE_PUBLIC_DEPS=ON but BUILD_DEPS_MIRROR_URL is also set. "
      "Ignoring BUILD_DEPS_MIRROR_URL and using public URLs.")
  endif()
  set(LOCAL_URL "" CACHE STRING "local archives storage to use" FORCE)
  set(LOCAL_REPO "" CACHE STRING "local repositories storage to use" FORCE)
  message(STATUS "BUILD_USE_PUBLIC_DEPS=ON: ExternalProject will use original public URLs")
endif()

if(NOT "${LOCAL_URL}" STREQUAL "")
  message(STATUS "ExternalProject mirror: ${LOCAL_URL}")
endif()

# get_from_local_repo_if_exists/get_from_local_if_exists
# is for local storage of externals only
macro(get_from_local_repo_if_exists git_url)              # {
  # if LOCAL_REPO is set as: -DLOCAL_REPO:STRING=ssh://host/path-to-local-repo
  # then _git_url would be: {ssh|https}://host/path-to-local-repo/<user>/<repo>.git
  if("z${LOCAL_REPO}" STREQUAL "z")
    set(_git_url "${git_url}")
  else()
    # Only redirect github.com URLs; leave other hosts (e.g. gitee.com) as-is
    string(FIND ${git_url} "github.com" _gh_pos)
    if(_gh_pos EQUAL -1)
      set(_git_url "${git_url}")
    else()
      # Extract the last two path components: /<user>/<repo>.git
      string(FIND ${git_url} "/" _pos2 REVERSE)
      string(SUBSTRING ${git_url} 0 ${_pos2} _prefix)
      string(FIND ${_prefix} "/" _pos1 REVERSE)
      string(SUBSTRING ${git_url} ${_pos1} -1 _name)
      set(_git_url "${LOCAL_REPO}${_name}")
    endif()
  endif()
endmacro()                                                # }

macro(get_from_local_if_exists url)                       # {
  if("z${LOCAL_URL}" STREQUAL "z")
    set(_url "${url}")
  else()
    if(${ARGC} GREATER 1)
      # Explicit mirror filename provided (e.g. "zlib-v1.3.1.tar.gz")
      set(_url "${LOCAL_URL}/${ARGV1}")
    else()
      # Legacy behavior: extract filename from URL (last path segment)
      string(FIND ${url} "/" _pos REVERSE)
      math(EXPR _pos "${_pos} + 1")
      string(SUBSTRING ${url} ${_pos} -1 _name)
      set(_url "${LOCAL_URL}/${_name}")
    endif()
  endif()
endmacro()                                                # }

# zlib
if(TD_LINUX)
    set(ext_zlib_static libz.a)
elseif(TD_DARWIN)
    set(ext_zlib_static libz.a)
elseif(TD_WINDOWS)
    set(ext_zlib_static zlibstatic$<$<STREQUAL:${TD_CONFIG_NAME},Debug>:d>.lib)
endif()
INIT_EXT(ext_zlib
    INC_DIR          include
    LIB              lib/${ext_zlib_static}
    CHK_NAME         ZLIB
)
get_from_local_if_exists(
    "https://github.com/madler/zlib/archive/refs/tags/v1.3.1.tar.gz"
    "zlib-v1.3.1.tar.gz"
)
ExternalProject_Add(ext_zlib
    URL ${_url}
    URL_HASH SHA256=17e88863f3600672ab49182f217281b6fc4d3c762bde361935e436a95214d05c
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}        # if main project is built in Debug, ext_zlib is too
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DINSTALL_BIN_DIR:PATH=${_ins}/bin
    CMAKE_ARGS -DINSTALL_LIB_DIR:PATH=${_ins}/lib
    CMAKE_ARGS -DINSTALL_INC_DIR:PATH=${_ins}/include
    CMAKE_ARGS -DINSTALL_MAN_DIR:PATH=${_ins}/share/man
    CMAKE_ARGS -DINSTALL_PKGCONFIG_DIR:PATH=${_ins}/share/pkgconfig
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON            # linking consistent
    CMAKE_ARGS -DZLIB_BUILD_SHARED:BOOL=OFF
    CMAKE_ARGS -DZLIB_BUILD_TESTING:BOOL=OFF
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_zlib)     # this is for github workflow in cache-miss step.

# pthread
if(BUILD_PTHREAD)        # {
    if(TD_WINDOWS)
        set(ext_pthread_static pthreadVC3.lib)
        set(ext_pthread_dll pthreadVC3.dll)
    endif()
    INIT_EXT(ext_pthread
        INC_DIR          include
        LIB              lib/${ext_pthread_static}
    )
    get_from_local_if_exists(
        "https://github.com/GerHobbelt/pthread-win32/archive/3309f4d6e7538f349ae450347b02132ecb0606a7.tar.gz"
        "pthread-win32-3309f4d.tar.gz"
    )
    ExternalProject_Add(ext_pthread
        URL ${_url}
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=ON
        CMAKE_ARGS "-DCMAKE_C_FLAGS:STRING=/wd4244"
        CMAKE_ARGS "-DCMAKE_CXX_FLAGS:STRING=/wd4244"
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_pthread)     # this is for github workflow in cache-miss step.
    add_custom_target(copy_pthreadVC3 ALL
        DEPENDS ext_pthread
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${_ins}/bin/${ext_pthread_dll} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${ext_pthread_dll}
    )
endif()                     # }

# iconv
if(BUILD_WITH_ICONV)     # {
    if(TD_WINDOWS)
        set(ext_iconv_static iconv.lib)
    endif()
    INIT_EXT(ext_iconv
        INC_DIR          include
        LIB              lib/${ext_iconv_static}
    )
    get_from_local_if_exists(
        "https://github.com/win-iconv/win-iconv/archive/9f98392dfecadffd62572e73e9aba878e03496c4.tar.gz"
        "win-iconv-9f98392.tar.gz"
    )
    ExternalProject_Add(ext_iconv
        URL ${_url}
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DBUILD_SHARED:BOOL=OFF
        CMAKE_ARGS -DBUILD_STATIC:BOOL=ON
        CMAKE_ARGS -DCMAKE_C_FLAGS:STRING=/wd4267
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_iconv)     # this is for github workflow in cache-miss step.
endif()                     # }

# msvc regex
if(BUILD_MSVCREGEX)      # {
    if(TD_WINDOWS)
        set(ext_msvcregex_static regex$<$<CONFIG:Debug>:_d>.lib)
    endif()
    INIT_EXT(ext_msvcregex
        INC_DIR          include
        LIB              lib/${ext_msvcregex_static}
    )
    # Originally from https://gitee.com/l0km/libgnurx-msvc (mirrored on GitLab)
    get_from_local_if_exists(
        "https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest/libgnurx-msvc-1a6514d.tar.gz"
        "libgnurx-msvc-1a6514d.tar.gz"
    )
    set(ext_msvcregex_archive_source "${ext_msvcregex_source}/libgnurx-msvc-master")
    ExternalProject_Add(ext_msvcregex
        URL ${_url}
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E chdir "${ext_msvcregex_archive_source}" nmake /f NMakefile all test test2 test3
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${ext_msvcregex_archive_source}/regex.h" "${_ins}/include/regex.h"
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${ext_msvcregex_archive_source}/${ext_msvcregex_static}" "${_ins}/lib/${ext_msvcregex_static}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_msvcregex)     # this is for github workflow in cache-miss step.
endif()                     # }

# wcwidth
if(BUILD_WCWIDTH)        # {
    if(TD_WINDOWS)
        set(ext_wcwidth_static wcwidth.lib)
    endif()
    INIT_EXT(ext_wcwidth
        INC_DIR          include
        LIB              lib/${ext_wcwidth_static}
    )
    get_from_local_if_exists(
        "https://github.com/fumiyas/wcwidth-cjk/archive/a1b1e2c346a563f6538e46e1d29c265bdd5b1c9a.tar.gz"
        "wcwidth-cjk-a1b1e2c.tar.gz"
    )
    ExternalProject_Add(ext_wcwidth
        URL ${_url}
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${TD_SUPPORT_DIR}/in/wcwidth.cmake" "${ext_wcwidth_source}/CMakeLists.txt"
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_wcwidth)     # this is for github workflow in cache-miss step.
endif()                     # }

# wingetopt
if(BUILD_WINGETOPT)      # {
    if(TD_WINDOWS)
        set(ext_wingetopt_static wingetopt.lib)
    endif()
    INIT_EXT(ext_wingetopt
        INC_DIR          include
        LIB              lib/${ext_wingetopt_static}
    )
    get_from_local_if_exists(
        "https://github.com/alex85k/wingetopt/archive/e8531ed21b44f5a723c1dd700701b2a58ce3ea01.tar.gz"
        "wingetopt-e8531ed.tar.gz"
    )
    ExternalProject_Add(ext_wingetopt
        URL ${_url}
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_wingetopt)     # this is for github workflow in cache-miss step.
endif()                     # }

# googletest
if(BUILD_TEST)           # {
    if(TD_LINUX)
        set(ext_gtest_static libgtest.a)
        set(ext_gtest_main libgtest_main.a)
    elseif(TD_DARWIN)
        set(ext_gtest_static libgtest.a)
        set(ext_gtest_main libgtest_main.a)
    elseif(TD_WINDOWS)
        set(ext_gtest_static gtest.lib)
        set(ext_gtest_main gtest_main.lib)
    endif()
    INIT_EXT(ext_gtest
        INC_DIR          include
        LIB              lib/${ext_gtest_main}
                         lib/${ext_gtest_static}
    )
    get_from_local_if_exists(
        "https://github.com/google/googletest/archive/refs/tags/release-1.12.0.tar.gz"
        "googletest-release-1.12.0.tar.gz"
    )
    ExternalProject_Add(ext_gtest
        URL ${_url}
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -Dgtest_force_shared_crt:BOOL=ON
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_gtest)     # this is for github workflow in cache-miss step.
endif()        # }

# cppstub
if(BUILD_TEST)           # {
    if(TD_LINUX)
        set(ext_cppstub_static libcppstub.a)
        set(_platform_dir      src_linux)
    elseif(TD_DARWIN)
        set(ext_cppstub_static libcppstub.a)
        set(_platform_dir      src_darwin)
    elseif(TD_WINDOWS)
        set(ext_cppstub_static cppstub.lib)
        set(_platform_dir      src_win)
    endif()
    INIT_EXT(ext_cppstub
        INC_DIR          include
    )
    get_from_local_if_exists(
        "https://github.com/coolxv/cpp-stub/archive/3137465194014d66a8402941e80d2bccc6346f51.tar.gz"
        "cpp-stub-3137465.tar.gz"
    )
    ExternalProject_Add(ext_cppstub
        URL ${_url}
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${ext_cppstub_source}/src/stub.h ${_ins}/include/stub.h
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${ext_cppstub_source}/${_platform_dir}/addr_any.h ${_ins}/include/addr_any.h
        # TODO: seems only .h files are exported
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_cppstub)     # this is for github workflow in cache-miss step.
endif()        # }

# lz4
if(TD_LINUX)
    set(ext_lz4_static liblz4.a)
elseif(TD_DARWIN)
    set(ext_lz4_static liblz4.a)
elseif(TD_WINDOWS)
    set(ext_lz4_static lz4.lib)
endif()
INIT_EXT(ext_lz4
    INC_DIR          include
    LIB              lib/${ext_lz4_static}
    CHK_NAME         LZ4
)
get_from_local_if_exists(
    "https://github.com/lz4/lz4/archive/refs/tags/v1.10.0.tar.gz"
    "lz4-v1.10.0.tar.gz"
)
ExternalProject_Add(ext_lz4
    URL ${_url}
    URL_HASH SHA256=537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b
    PREFIX "${_base}"
    SOURCE_SUBDIR build/cmake
    CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DBUILD_STATIC_LIBS:BOOL=ON
    BUILD_COMMAND
        COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
    INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_lz4)     # this is for github workflow in cache-miss step.

# cJson
if(TD_LINUX)
    set(ext_cjson_static libcjson.a)
elseif(TD_DARWIN)
    set(ext_cjson_static libcjson.a)
elseif(TD_WINDOWS)
    set(ext_cjson_static cjson.lib)
endif()
INIT_EXT(ext_cjson
    INC_DIR          include/cjson           # TODO: tweak in this way to hack #include <cJSON.h> in source codes
    LIB              lib/${ext_cjson_static}
)
get_from_local_if_exists(
    "https://github.com/DaveGamble/cJSON/archive/12c4bf1986c288950a3d06da757109a6aa1ece38.tar.gz"
    "cJSON-12c4bf1986c2.tar.gz"
)
ExternalProject_Add(ext_cjson
    URL ${_url}
    URL_HASH SHA256=1f0e45ff5c2dca61e88bbc47b2537b64fd8bceb02b4abbdcd85a6c7135e4bd75
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DCJSON_BUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DENABLE_HIDDEN_SYMBOLS:BOOL=ON
    CMAKE_ARGS -DENABLE_PUBLIC_SYMBOLS:BOOL=OFF
    CMAKE_ARGS -DCMAKE_POLICY_VERSION_MINIMUM=3.5
    CMAKE_ARGS -DENABLE_CJSON_TEST:BOOL=OFF
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    BUILD_COMMAND
        COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
    INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_cjson)     # this is for github workflow in cache-miss step.

# xz
if(TD_LINUX)
    set(ext_xz_static liblzma.a)
elseif(TD_DARWIN)
    set(ext_xz_static liblzma.a)
elseif(TD_WINDOWS)
    set(ext_xz_static lzma.lib)
endif()
INIT_EXT(ext_xz
    INC_DIR          include
    LIB              lib/${ext_xz_static}
    # debugging github working flow
    # CHK_NAME         LZMA
)
get_from_local_if_exists(
    "https://github.com/tukaani-project/xz/archive/refs/tags/v5.8.1.tar.gz"
    "xz-v5.8.1.tar.gz"
)
ExternalProject_Add(ext_xz
    URL ${_url}
    URL_HASH SHA256=bdbc23fbf9098843357e71e49685724fda2c320c29cb1b25fd90505f14bb0b3d
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
    CMAKE_ARGS -DCREATE_LZMA_SYMLINKS:BOOL=OFF
    CMAKE_ARGS -DCREATE_XZ_SYMLINKS:BOOL=OFF
    BUILD_COMMAND
        COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
    INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_xz)     # this is for github workflow in cache-miss step.

# xxHash
# NOTE: ref from lzma2::xxhash.h: `https://github.com/Cyan4973/xxHash`
# TODO: external-symbols (eg. XXH64_createState ...) exist both in libxxhash.a and libfast-lzma2.a
#       static linking problem?
#       currently, always call DEP_ext_... in such order, for the same target:
#       DEP_ext_xxhash(...)
#       DEP_ext_lzma2(...)
if(TD_LINUX)
    set(ext_xxhash_static libxxhash.a)
elseif(TD_DARWIN)
    set(ext_xxhash_static libxxhash.a)
elseif(TD_WINDOWS)
    set(ext_xxhash_static xxhash.lib)
endif()
get_from_local_if_exists(
    "https://github.com/Cyan4973/xxHash/archive/de9d6577907d4f4f8153e96b0cb0cbdf7df649bb.tar.gz"
    "xxHash-de9d6577907d.tar.gz"
)
if(NOT TD_WINDOWS)        # {
    INIT_EXT(ext_xxhash
        INC_DIR          "usr/local/include"
        LIB              "usr/local/lib/${ext_xxhash_static}"
    )
    ExternalProject_Add(ext_xxhash
        URL ${_url}
        URL_HASH SHA256=2be1ed3a89931a932695129762174c9f51a4d7ebf38db3f6f0a9db765a30f718
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/xxhash.Makefile Makefile
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make DESTDIR=${_ins}
        INSTALL_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make DESTDIR=${_ins} install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
else()                       # }{
    INIT_EXT(ext_xxhash
        INC_DIR          "include"
        LIB              "lib/${ext_xxhash_static}"
    )
    ExternalProject_Add(ext_xxhash
        URL ${_url}
        URL_HASH SHA256=2be1ed3a89931a932695129762174c9f51a4d7ebf38db3f6f0a9db765a30f718
        PREFIX "${_base}"
        SOURCE_SUBDIR cmake_unofficial
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif()                      # }
add_dependencies(build_externals ext_xxhash)     # this is for github workflow in cache-miss step.

# lzma2
if(TD_LINUX)
    set(ext_lzma2_static libfast-lzma2.a)
    INIT_EXT(ext_lzma2
        INC_DIR          usr/local/include
        LIB              usr/local/lib/${ext_lzma2_static}
    )
    get_from_local_if_exists(
        "https://github.com/conor42/fast-lzma2/archive/ded964d203cabe1a572d2c813c55e8a94b4eda48.tar.gz"
        "fast-lzma2-ded964d203ca.tar.gz"
    )
    ExternalProject_Add(ext_lzma2
        URL ${_url}
        URL_HASH SHA256=ee71c637966a7ac429a245e2ee96a7a7ce52eb59087899f07cd1068a41c3af0e
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/lzma2.Makefile Makefile
            # NOTE: xxhash.h is now introduced by ext_xxhash
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make DESTDIR=${_ins}
        INSTALL_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make DESTDIR=${_ins} install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_lzma2)     # this is for github workflow in cache-miss step.
endif()

# libuv
if(BUILD_WITH_UV)        # {
    if(TD_LINUX)
        set(ext_libuv_static libuv.a)
    elseif(TD_DARWIN)
        set(ext_libuv_static libuv.a)
    elseif(TD_WINDOWS)
        set(ext_libuv_static libuv.lib)
    endif()
    INIT_EXT(ext_libuv
        INC_DIR          include
        LIB              lib/${ext_libuv_static}
        CHK_NAME         LIBUV
    )
    get_from_local_if_exists(
        "https://github.com/libuv/libuv/archive/refs/tags/v1.49.2.tar.gz"
        "libuv-v1.49.2.tar.gz"
    )
    ExternalProject_Add(ext_libuv
        URL ${_url}
        URL_HASH SHA256=388ffcf3370d4cf7c4b3a3205504eea06c4be5f9e80d2ab32d19f8235accc1cf
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        CMAKE_ARGS -DLIBUV_BUILD_SHARED:BOOL=OFF
        CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_libuv)     # this is for github workflow in cache-miss step.
endif()     # }

# tz
if(NOT TD_WINDOWS)       # {
    if(TD_LINUX)
        set(ext_tz_static libtz.a)
        set(_c_flags_list -fPIC -DTHREAD_SAFE=1)
    elseif(TD_DARWIN)
        set(ext_tz_static libtz.a)
        set(_c_flags_list -fPIC -DHAVE_GETTEXT=0 -DTHREAD_SAFE=1) # TODO: brew install gettext?
    endif()
    INIT_EXT(ext_tz
        INC_DIR          include
        LIB              usr/lib/${ext_tz_static}
    )
    string(JOIN " " _c_flags ${_c_flags_list})
    get_from_local_if_exists(
        "https://github.com/eggert/tz/archive/refs/tags/2025a.tar.gz"
        "tz-2025a.tar.gz"
    )
    ExternalProject_Add(ext_tz
        URL ${_url}
        URL_HASH SHA256=d0f35d0a3b5ca1bb25539b159c1338135a4f59b0d423381ecafa31d0449caea5
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/tz.Makefile Makefile
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
            # COMMAND make CFLAGS+=-fPIC CFLAGS+=-g TZDIR=${TZ_OUTPUT_PATH} clean libtz.a
            COMMAND "${CMAKE_COMMAND}" -E echo "-=${_c_flags}=-"
            COMMAND ${_EXT_ENV_PREFIX} make "CFLAGS=${_c_flags}" DESTDIR=${_ins}
        INSTALL_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make "CFLAGS=${_c_flags}" DESTDIR=${_ins} install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_tz)     # this is for github workflow in cache-miss step.
endif()    # }

# jemalloc
if(BUILD_JEMALLOC)     # {
    find_program(HAVE_AUTOCONF autoconf)
    if(HAVE_AUTOCONF STREQUAL "HAVE_AUTOCONF-NOTFOUND")
        message(FATAL_ERROR "`autoconf` not exist, you can install it by `sudo apt install autoconf` on linux, or `brew install autoconf` on MacOS")
    endif()
    if(TD_LINUX)
        set(ext_jemalloc_static jemalloc.a)
    elseif(TD_DARWIN)
        set(ext_jemalloc_static jemalloc.a)
    endif()
    INIT_EXT(ext_jemalloc
        INC_DIR          include
        LIB              lib/${ext_jemalloc_static}
    )
    get_from_local_if_exists(
        "https://github.com/jemalloc/jemalloc/archive/refs/tags/5.3.0.tar.gz"
        "jemalloc-5.3.0.tar.gz"
    )
    ExternalProject_Add(ext_jemalloc
        URL ${_url}
        URL_HASH SHA256=ef6f74fd45e95ee4ef7f9e19ebe5b075ca6b7fbe0140612b2a161abafb7ee179
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND ./autogen.sh
        CONFIGURE_COMMAND
            COMMAND ./configure -prefix=${_ins} --disable-initial-exec-tls     # NOTE: why disable-initial-exec-tls
                    CFLAGS=-Wno-missing-braces
                    CXXFLAGS=-Wno-missing-braces
        BUILD_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make
        INSTALL_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_jemalloc)     # this is for github workflow in cache-miss step.
endif()                     # }

# sqlite
if(BUILD_WITH_SQLITE)    # {
    if(TD_LINUX)
        set(ext_sqlite_static sqlite.a)
    elseif(TD_DARWIN)
        set(ext_sqlite_static sqlite.a)
    elseif(TD_WINDOWS)
        set(ext_sqlite_static sqlite.lib)
    endif()
    INIT_EXT(ext_sqlite
        INC_DIR          include
        LIB              lib/${ext_sqlite_static}
        CHK_NAME         SQLITE3
    )
    get_from_local_if_exists(
        "https://github.com/sqlite/sqlite/archive/refs/tags/version-3.36.0.tar.gz"
        "sqlite-version-3.36.0.tar.gz"
    )
    ExternalProject_Add(ext_sqlite
        URL ${_url}
        URL_HASH SHA256=a0989fc6e890ac1b1b28661490636617154da064b6bfe6c71100d23a9e7298fd
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        # TODO: seems no use at all
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_sqlite)     # this is for github workflow in cache-miss step.
endif() # }

# crashdump
if(BUILD_CRASHDUMP)      # {
    if(TD_WINDOWS)
        set(ext_crashdump_static crashdump.lib)
    endif()
    INIT_EXT(ext_crashdump
        INC_DIR          include
        LIB              lib/${ext_crashdump_static}
    )
    get_from_local_if_exists(
        "https://github.com/Arnavion/crashdump/archive/149b43c10debdf28a2c50d79dee5ff344d83bd06.tar.gz"
        "crashdump-149b43c.tar.gz"
    )
    ExternalProject_Add(ext_crashdump
        URL ${_url}
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/crashdump.cmake CMakeLists.txt
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/crasher.c.in crasher/crasher.c
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_crashdump)     # this is for github workflow in cache-miss step.
endif()   # }

# ssl
if(NOT TD_WINDOWS)       # {
    # TODO: why at this moment???
    # file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.2/)
    if(TD_LINUX)
        set(ext_ssl_static libssl.a)
        set(ext_crypto_static libcrypto.a)
    elseif(TD_DARWIN)
        set(ext_ssl_static libssl.a)
        set(ext_crypto_static libcrypto.a)
    endif()
    INIT_EXT(ext_ssl
        INC_DIR          include
        LIB              lib/${ext_ssl_static}
                         lib/${ext_crypto_static}
        # debugging github working flow
        # CHK_NAME         SSL
    )
    list(SUBLIST ext_ssl_libs 0 1 ext_ssl_lib_ssl)
    list(SUBLIST ext_ssl_libs 1 1 ext_ssl_lib_crypto)
    # URL https://github.com/openssl/openssl/releases/download/openssl-3.1.3/openssl-3.1.3.tar.gz
    # URL_HASH SHA256=f0316a2ebd89e7f2352976445458689f80302093788c466692fb2a188b2eacf6
    get_from_local_if_exists(
        "https://github.com/openssl/openssl/releases/download/openssl-3.1.3/openssl-3.1.3.tar.gz"
        "openssl-3.1.3.tar.gz"
    )
    # Docker Desktop for Mac uses VirtioFS for bind-mount volumes.  Under heavy
    # ccache corrupts certain OpenSSL .o files (cipher_aria.o becomes "data"
    # instead of ELF) when gcc-toolset-14 is used via ccache symlinks.
    # _EXT_CCACHE_EXPORT conditionally sets CCACHE_DISABLE=1.
    # MAKEFLAGS is unset to prevent the parent cmake make's flags (e.g. -s -j1)
    # from leaking into OpenSSL's own make invocation.
    ExternalProject_Add(ext_ssl
        URL ${_url}
        URL_HASH SHA256=f0316a2ebd89e7f2352976445458689f80302093788c466692fb2a188b2eacf6
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CONFIGURE_COMMAND
            COMMAND ./Configure --prefix=${_ins} no-shared --libdir=lib
        BUILD_COMMAND
            COMMAND sh -c "unset MAKEFLAGS && ${_EXT_CCACHE_EXPORT}make -j4"
        INSTALL_COMMAND
            COMMAND sh -c "unset MAKEFLAGS && make install_sw -j4"
        EXCLUDE_FROM_ALL TRUE
    )
    add_dependencies(build_externals ext_ssl)     # this is for github workflow in cache-miss step.
endif()    # }

# libcurl
if(NOT TD_WINDOWS)       # {
    if(TD_LINUX)
        set(ext_curl_static libcurl.a)
        set(_c_flags_list -fPIC -Wno-implicit-function-declaration)
    elseif(TD_DARWIN)
        set(ext_curl_static libcurl.a)
        set(_c_flags_list -Wno-implicit-function-declaration)
    endif()
else()
    set(ext_curl_static libcurl$<$<STREQUAL:${TD_CONFIG_NAME},Debug>:-d>.lib)
    set(_c_flags_list)
endif()

INIT_EXT(ext_curl
    INC_DIR          include
    LIB              lib/${ext_curl_static}
    # currently: tqStreamNotify.c uses curl_ws_send, but CURL4_OPENSSL exports curl_easy_send
    #            libcurl4-openssl-dev on ubuntu 22.04 is too old
    # CHK_NAME         CURL4_OPENSSL
)

if(${TD_WINDOWS})
    # URL https://github.com/curl/curl/releases/download/curl-8_2_1/curl-8.2.1.tar.gz
    # URL_HASH MD5=b25588a43556068be05e1624e0e74d41
    get_from_local_if_exists(
        "https://github.com/curl/curl/releases/download/curl-8_2_1/curl-8.2.1.tar.gz"
        "curl-8.2.1.tar.gz"
    )
    ExternalProject_Add(ext_curl
        URL ${_url}
        URL_HASH MD5=b25588a43556068be05e1624e0e74d41
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
        CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
        CMAKE_ARGS -DBUILD_CURL_EXE:BOOL=OFF
        CMAKE_ARGS -DENABLE_WEBSOCKETS:BOOL=ON
        CMAKE_ARGS -DCURL_USE_SCHANNEL:BOOL=ON
        CMAKE_ARGS -DCURL_USE_OPENSSL:BOOL=OFF
        CMAKE_ARGS -DCURL_ZLIB:BOOL=OFF
        CMAKE_ARGS -DCURL_DISABLE_LDAP:BOOL=ON
        CMAKE_ARGS -DCURL_DISABLE_LDAPS:BOOL=ON
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
else()
    string(JOIN " " _c_flags ${_c_flags_list})
    # URL https://github.com/curl/curl/releases/download/curl-8_2_1/curl-8.2.1.tar.gz
    # URL_HASH MD5=b25588a43556068be05e1624e0e74d41
    get_from_local_if_exists(
        "https://github.com/curl/curl/releases/download/curl-8_2_1/curl-8.2.1.tar.gz"
        "curl-8.2.1.tar.gz"
    )
    ExternalProject_Add(ext_curl
        URL ${_url}
        URL_HASH MD5=b25588a43556068be05e1624e0e74d41
        # GIT_SHALLOW TRUE
        DEPENDS ext_ssl
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CONFIGURE_COMMAND
            # COMMAND ./Configure --prefix=$ENV{HOME}/.cos-local.2 no-shared
            COMMAND ${CMAKE_COMMAND} -E env "CFLAGS=${_c_flags}" "CXXFLAGS=${_c_flags}" ./configure --prefix=${_ins} --with-ssl=${ext_ssl_install}
                    --enable-websockets --enable-shared=no --disable-ldap
                    --disable-ldaps --without-brotli --without-zstd
                    --without-libidn2 --without-nghttp2 --without-libpsl
                    --without-librtmp #--enable-debug
        BUILD_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make -j4
        INSTALL_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif()
add_dependencies(build_externals ext_curl)     # this is for github workflow in cache-miss step.

# geos
if(BUILD_GEOS)           # {
    if(TD_LINUX)
        set(ext_geos_static libgeos.a)
        set(ext_geos_c_static libgeos_c.a)
    elseif(TD_DARWIN)
        set(ext_geos_static libgeos.a)
        set(ext_geos_c_static libgeos_c.a)
    elseif(TD_WINDOWS)
        set(ext_geos_static geos.lib)
        set(ext_geos_c_static geos_c.lib)
    endif()
    INIT_EXT(ext_geos
        INC_DIR          include
        LIB              lib/${ext_geos_c_static}
                         lib/${ext_geos_static}
        CHK_NAME         GEOS
    )
    get_from_local_if_exists(
        "https://github.com/libgeos/geos/archive/refs/tags/3.12.0.tar.gz"
        "geos-3.12.0.tar.gz"
    )
    ExternalProject_Add(ext_geos
        URL ${_url}
        URL_HASH SHA256=0b4fca58fc09677e6230bc8aef527fd2d7cdf9ff55b4ef3af75a775cb8d76e89
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DCMAKE_POLICY_VERSION_MINIMUM=3.5
        CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
        CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
        CMAKE_ARGS -DBUILD_GEOSOP:BOOL=OFF
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_geos)     # this is for github workflow in cache-miss step.
endif()                     # }

# libdwarf
if(BUILD_ADDR2LINE)      # {
    if(TD_LINUX)
        set(ext_dwarf_static libdwarf.a)
    elseif(TD_DARWIN)
        set(ext_dwarf_static libdwarf.a)
    endif()
    INIT_EXT(ext_dwarf
        INC_DIR          include
        LIB              lib/${ext_dwarf_static}
    )

    set(_c_cxx_flags_list
      -I${ext_zlib_install}/include
      -L${ext_zlib_install}/lib
    )
    if (TD_DARWIN)      # {
      list(APPEND _c_cxx_flags_list
        -Wno-unused-command-line-argument
        -Wno-error=unused-but-set-variable
        -Wno-error=strict-prototypes
        -Wno-error=self-assign
        -Wno-error=null-pointer-subtraction
      )
    endif()                # }
    string(JOIN " " _c_cxx_flags ${_c_cxx_flags_list})

    get_from_local_if_exists(
        "https://github.com/davea42/libdwarf-code/archive/refs/tags/libdwarf-0.3.1.tar.gz"
        "libdwarf-code-libdwarf-0.3.1.tar.gz"
    )
    ExternalProject_Add(ext_dwarf
        URL ${_url}
        URL_HASH SHA256=0e79dc9c43cbf67fdd64591cede9da0727b17fef0efe91cbcf48a714369cf3fc
        DEPENDS ext_zlib
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS "-DCMAKE_C_FLAGS:STRING=${_c_cxx_flags}"
        CMAKE_ARGS "-DCMAKE_CXX_FLAGS:STRING=${_c_cxx_flags}"
        CMAKE_ARGS -DDO_TESTING:BOOL=OFF
        CMAKE_ARGS -DDWARF_WITH_LIBELF:BOOL=ON
        CMAKE_ARGS -DLIBDWARF_CRT:STRING=MD
        CMAKE_ARGS -DWALL:BOOL=ON
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different
                    "${ext_dwarf_source}/src/lib/libdwarf/dwarf.h"
                    "${ext_dwarf_install}/include/libdwarf/dwarf.h"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_dwarf)     # this is for github workflow in cache-miss step.
endif()   # }

# addr2line
if(BUILD_ADDR2LINE)      # {
    if(TD_LINUX)
        set(ext_addr2line_static libaddr2line.a)
    elseif(TD_DARWIN)
        set(ext_addr2line_static libaddr2line.a)
    endif()
    INIT_EXT(ext_addr2line
        INC_DIR          include
        LIB              lib/${ext_addr2line_static}
    )
    get_from_local_if_exists(
        "https://github.com/davea42/libdwarf-addr2line/archive/9d76b420f9d1261fa7feada3a209e605f54ba859.tar.gz"
        "libdwarf-addr2line-9d76b420f9d1.tar.gz"
    )
    ExternalProject_Add(ext_addr2line
        URL ${_url}
        URL_HASH SHA256=90bd652116122ebbb36e9f31b4bdf5bfb6bf9baca3edb9380e06eb4d9f19e233
        DEPENDS ext_dwarf
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DDWARF_BASE_DIR:STRING=${ext_dwarf_install}
        CMAKE_ARGS -DZLIB_BASE_DIR:STRING=${ext_zlib_install}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${TD_SUPPORT_DIR}/in/addr2line.cmake" "${ext_addr2line_source}/CMakeLists.txt"
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_addr2line)     # this is for github workflow in cache-miss step.
endif()   # }

# pcre2
if(BUILD_PCRE2)          # {
    # TODO: seems no necessary cause strict rules has been enforced by geos
    if(TD_LINUX)
        set(ext_pcre2_static libpcre2-8.a)
    elseif(TD_DARWIN)
        set(ext_pcre2_static libpcre2-8.a)
    elseif(TD_WINDOWS)
        set(ext_pcre2_static pcre2-8-static$<$<STREQUAL:${TD_CONFIG_NAME},Debug>:d>.lib)
    endif()
    INIT_EXT(ext_pcre2
        INC_DIR          include
        LIB              lib/${ext_pcre2_static}
    )
    get_from_local_if_exists(
        "https://github.com/PCRE2Project/pcre2/archive/refs/tags/pcre2-10.45.tar.gz"
        "pcre2-pcre2-10.45.tar.gz"
    )
    ExternalProject_Add(ext_pcre2
        URL ${_url}
        URL_HASH SHA256=35ce7d21f511c4a81d7079164077d25fbc41af00f19e1b547801df905c5f0fab
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DPCRE2_BUILD_TESTS:BOOL=OFF
        CMAKE_ARGS -DPCRE2_STATIC_PIC:BOOL=OFF
        CMAKE_ARGS -DPCRE2_SHOW_REPORT:BOOL=OFF
        CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        # NOTE: turns off because of dynamic linking
        CMAKE_ARGS -DPCRE2_SUPPORT_LIBZ:BOOL=OFF
        CMAKE_ARGS -DPCRE2_SUPPORT_LIBBZ2:BOOL=OFF
        CMAKE_ARGS -DPCRE2_SUPPORT_LIBREADLINE:BOOL=OFF
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_pcre2)     # this is for github workflow in cache-miss step.
endif()                     # }

include(GNUInstallDirs)
message(STATUS "Using libdir: ${CMAKE_INSTALL_LIBDIR}")
if(TD_ROCKSDB_USE_EXTERNAL)         # {
    if(TD_LINUX)
        set(ext_rocksdb_static librocksdb.a)
    elseif(TD_DARWIN)
        set(ext_rocksdb_static librocksdb.a)
    elseif(TD_WINDOWS)
        set(ext_rocksdb_static rocksdb.lib)
    endif()
    INIT_EXT(ext_rocksdb
        INC_DIR          include
        LIB              lib/${ext_rocksdb_static}
    )

    if(TD_ROCKSDB_BUILD_FROM_SOURCE)
        # BUILD_CONTRIB=ON + BUILD_ROCKSDB=ON: download and compile RocksDB
        # URL https://github.com/facebook/rocksdb/archive/refs/tags/v8.1.1.tar.gz
        # URL_HASH MD5=3b4c97ee45df9c8a5517308d31ab008b
        get_from_local_if_exists(
            "https://github.com/facebook/rocksdb/archive/refs/tags/v8.1.1.tar.gz"
            "rocksdb-v8.1.1.tar.gz"
        )
        ExternalProject_Add(ext_rocksdb
            URL ${_url}
            URL_HASH MD5=3b4c97ee45df9c8a5517308d31ab008b
            # GIT_SHALLOW TRUE
            PREFIX "${_base}"
            CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
            CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
            CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE=ON
            CMAKE_ARGS -DPORTABLE:BOOL=ON
            CMAKE_ARGS -DWITH_FALLOCATE:BOOL=OFF
            CMAKE_ARGS -DWITH_JEMALLOC:BOOL=OFF
            CMAKE_ARGS -DWITH_GFLAGS:BOOL=OFF
            CMAKE_ARGS -DWITH_LIBURING:BOOL=OFF
            CMAKE_ARGS -DFAIL_ON_WARNINGS:BOOL=OFF
            # CMAKE_ARGS -DWITH_ALL_TESTS:BOOL=OFF
            CMAKE_ARGS -DWITH_TESTS:BOOL=OFF
            CMAKE_ARGS -DWITH_BENCHMARK_TOOLS:BOOL=OFF
            CMAKE_ARGS -DWITH_TOOLS:BOOL=OFF
            CMAKE_ARGS -DROCKSDB_BUILD_SHARED:BOOL=OFF
            CMAKE_ARGS -DROCKSDB_INSTALL_ON_WINDOWS:BOOL=ON
            # "-DCMAKE_CXX_FLAGS:STRING=-Wno-maybe-uninitialized"
            BUILD_COMMAND
                COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
            INSTALL_COMMAND
                COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
        add_dependencies(build_externals ext_rocksdb)     # this is for github workflow in cache-miss step.
    else()
        # ROCKSDB_USE_DEPS=OFF + BUILD_ROCKSDB=OFF: reuse cached ExternalProject artifacts.
        # Validate that the cached library actually exists.
        #
        # INIT_EXT declares LIB as "lib/librocksdb.a" and ExternalProject forces
        # -DCMAKE_INSTALL_LIBDIR:PATH=lib, so the primary check uses "lib/".
        # For backward compatibility with caches built before this fix (where
        # CMAKE_INSTALL_LIBDIR may have resolved to "lib64" on x86_64), we also
        # check the CMAKE_INSTALL_LIBDIR path as a fallback and update
        # ext_rocksdb_libs so the linker can find the library.
        set(_rocksdb_install_prefix "${TD_EXTERNALS_BASE_DIR}/install/ext_rocksdb/${TD_CONFIG_NAME_RESOLVED}")
        set(_rocksdb_check_path "${_rocksdb_install_prefix}/lib/${ext_rocksdb_static}")
        if(NOT EXISTS "${_rocksdb_check_path}")
            # Fallback: older caches may have installed into CMAKE_INSTALL_LIBDIR (e.g. lib64)
            set(_rocksdb_found FALSE)
            if(NOT "${CMAKE_INSTALL_LIBDIR}" STREQUAL "lib")
                set(_rocksdb_check_alt "${_rocksdb_install_prefix}/${CMAKE_INSTALL_LIBDIR}/${ext_rocksdb_static}")
                if(EXISTS "${_rocksdb_check_alt}")
                    set(ext_rocksdb_libs "${_rocksdb_check_alt}")
                    set(_rocksdb_found TRUE)
                    message(STATUS "[rocksdb] Found cached library at legacy path: ${_rocksdb_check_alt}")
                endif()
            endif()
            if(NOT _rocksdb_found)
                message(FATAL_ERROR
                    "[rocksdb] Expecting cached ExternalProject artifact at:\n"
                    "  ${_rocksdb_check_path}\n"
                    "  but it does not exist. Either:\n"
                    "  - Run with -DBUILD_CONTRIB=ON -DBUILD_ROCKSDB=ON to build from source, or\n"
                    "  - Set -DROCKSDB_USE_DEPS=ON to use prebuilt deps/.")
            endif()
        endif()
    endif()
endif()                                          # }

if(TD_TAOS_TOOLS)
    if(TD_LINUX)
        set(ext_jansson_static libjansson.a)
    elseif(TD_DARWIN)
        set(ext_jansson_static libjansson.a)
    elseif(TD_WINDOWS)
        set(ext_jansson_static jansson$<$<STREQUAL:${TD_CONFIG_NAME},Debug>:_d>.lib)
    endif()
    INIT_EXT(ext_jansson
        INC_DIR          include
        LIB              lib/${ext_jansson_static}
        CHK_NAME         JANSSON
    )
    get_from_local_if_exists(
        "https://github.com/akheron/jansson/archive/61fc3d0e28e1a35410af42e329cd977095ec32d2.tar.gz"
        "jansson-61fc3d0e28e1.tar.gz"
    )
    ExternalProject_Add(ext_jansson
        URL ${_url}
        URL_HASH SHA256=a076437807defba7a7803e65b5eae78247becd415c06618133910a4f5ccc3a2a
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DJANSSON_BUILD_DOCS:BOOL=OFF
        CMAKE_ARGS -DCMAKE_POLICY_VERSION_MINIMUM=3.5
        CMAKE_ARGS -DJANSSON_EXAMPLES:BOOL=OFF
        CMAKE_ARGS -DJANSSON_WITHOUT_TESTS:BOOL=ON
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_jansson)     # this is for github workflow in cache-miss step.

    if(TD_LINUX)
        set(ext_snappy_static libsnappy.a)
    elseif(TD_DARWIN)
        set(ext_snappy_static libsnappy.a)
    elseif(TD_WINDOWS)
        set(ext_snappy_static snappy.lib)
    endif()
    INIT_EXT(ext_snappy
        INC_DIR          include
        LIB              lib/${ext_snappy_static}
        CHK_NAME         snappy
    )
    get_from_local_if_exists(
        "https://github.com/google/snappy/archive/32ded457c0b1fe78ceb8397632c416568d6714a0.tar.gz"
        "snappy-32ded457c0b1.tar.gz"
    )
    ExternalProject_Add(ext_snappy
        URL ${_url}
        URL_HASH SHA256=677d1dd8172bac1862e6c8d7bbe1fe9fb2320cfd11ee04756b1ef8b3699c6135
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        CMAKE_ARGS -DCMAKE_POLICY_VERSION_MINIMUM=3.5
        # CMAKE_ARGS -DBENCHMARK_ENABLE_GTEST_TESTS:BOOL=OFF
        # CMAKE_ARGS -DBENCHMARK_INSTALL_DOCS:BOOL=OFF
        # CMAKE_ARGS -DBENCHMARK_USE_BUNDLED_GTEST:BOOL=OFF
        # CMAKE_ARGS -DINSTALL_GTEST:BOOL=OFF
        CMAKE_ARGS -DSNAPPY_BUILD_BENCHMARKS:BOOL=OFF
        CMAKE_ARGS -DSNAPPY_BUILD_TESTS:BOOL=OFF
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_snappy)     # this is for github workflow in cache-miss step.

    if(TD_LINUX)
        set(ext_avro_static libavro.a)
        set(_c_flags_list "")
    elseif(TD_DARWIN)
        set(ext_avro_static libavro.a)
        set(_c_flags_list "")
    elseif(TD_WINDOWS)
        set(ext_avro_static avro.lib)
        set(_c_flags_list
            /wd4819
            /wd4244
            /wd4267
            /wd4068
            /wd4996
            /wd4146
            /wd4305
        )
    endif()
    string(JOIN " " _c_flags ${_c_flags_list})
    INIT_EXT(ext_avro
        INC_DIR          include
        LIB              lib/${ext_avro_static}
        CHK_NAME         AVRO
    )
    get_from_local_if_exists(
        "https://github.com/apache/avro/archive/7b106b12ae22853c977259710d92a237d76f2236.tar.gz"
        "avro-7b106b12ae22.tar.gz"
    )
    message(STATUS
      "[external] ext_avro: fetching '${_url}'"
    )
    ExternalProject_Add(ext_avro
        URL ${_url}
        URL_HASH SHA256=75c544c67cdf0846ea44b169c57d6c450eaf0b0b5cceac2db4b7afe3f2e2475a
        DEPENDS ext_zlib ext_jansson ext_snappy
        PREFIX "${_base}"
        SOURCE_SUBDIR lang/c
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DZLIB_INCLUDE_DIRS:STRING=${ext_zlib_install}/include
        CMAKE_ARGS -DZLIB_LIBRARIES:STRING=${ext_zlib_install}/lib/${ext_zlib_static}
        CMAKE_ARGS -DSNAPPY_INCLUDE_DIRS:STRING=${ext_snappy_install}/include
        CMAKE_ARGS -DSNAPPY_LIBRARIES:STRING=${ext_snappy_install}/lib/${ext_snappy_static}
        CMAKE_ARGS -DJANSSON_INCLUDE_DIRS:STRING=${ext_jansson_install}/include
        CMAKE_ARGS -DJANSSON_LIBRARY_DIRS:STRING=${ext_jansson_install}/lib/${ext_jansson_static}
        CMAKE_ARGS "-DCMAKE_C_FLAGS:STRING=${_c_flags}"
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.CMakeLists.txt.in            ${ext_avro_source}/lang/c/CMakeLists.txt
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.src.avro.msinttypes.h.in     ${ext_avro_source}/lang/c/src/avro/msinttypes.h
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.src.avro.platform.h.in       ${ext_avro_source}/lang/c/src/avro/platform.h
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.src.avroappend.c.in          ${ext_avro_source}/lang/c/src/avroappend.c
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.src.avro_private.h.in        ${ext_avro_source}/lang/c/src/avro_private.h
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.src.CMakeLists.txt.in        ${ext_avro_source}/lang/c/src/CMakeLists.txt
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.src.codec.c.in               ${ext_avro_source}/lang/c/src/codec.c
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.src.schema.c.in              ${ext_avro_source}/lang/c/src/schema.c
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.tests.CMakeLists.txt.in      ${ext_avro_source}/lang/c/tests/CMakeLists.txt
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/avro.lang.c.tests.test_avro_data.c.in    ${ext_avro_source}/lang/c/tests/test_avro_data.c
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        LOG_DOWNLOAD ON
        LOG_UPDATE ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
        LOG_INSTALL ON
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_avro)     # this is for github workflow in cache-miss step.
endif()

# libxml2
if(TD_LINUX)
    set(ext_libxml2_static libxml2.a)
elseif(TD_DARWIN)
    set(ext_libxml2_static libxml2.a)
elseif(TD_WINDOWS)
    if(TD_CONFIG_NAME_RESOLVED STREQUAL "Debug")
        set(ext_libxml2_static libxml2sd.lib)
    else()
        set(ext_libxml2_static libxml2.lib)
    endif()
    # On Windows, libxml2 is built as a static library, consumers must define LIBXML_STATIC
    macro(DEP_ext_libxml2_INC tgt)
        if(${ext_libxml2_build_contrib})
            target_include_directories(${tgt} PUBLIC "${ext_libxml2_inc_dir}")
            target_compile_definitions(${tgt} PUBLIC LIBXML_STATIC)
            if(NOT TD_EXTERNALS_USE_ONLY)
                set_target_properties(ext_libxml2_imp PROPERTIES
                    IMPORTED_LOCATION "${ext_libxml2_libs}"
                )
                add_dependencies(${tgt} ext_libxml2)
            endif()
            add_definitions(-D_ext_libxml2)
        endif()
    endmacro()
endif()

INIT_EXT(ext_libxml2
    INC_DIR          include/libxml2
    LIB              lib/${ext_libxml2_static}
)
set(_libxml2_depends "")
set(_libxml2_extra_args "")
if(TD_WINDOWS AND BUILD_WITH_ICONV)
    list(APPEND _libxml2_depends ext_iconv)
    list(APPEND _libxml2_extra_args "-DIconv_INCLUDE_DIR:STRING=${ext_iconv_inc_dir}")
    list(APPEND _libxml2_extra_args "-DIconv_LIBRARY:STRING=${ext_iconv_libs}")
elseif(TD_WINDOWS)
    list(APPEND _libxml2_extra_args "-DLIBXML2_WITH_ICONV:BOOL=OFF")
endif()
get_from_local_if_exists(
    "https://github.com/GNOME/libxml2/archive/refs/tags/v2.14.0.tar.gz"
    "libxml2-v2.14.0.tar.gz"
)
ExternalProject_Add(ext_libxml2
    URL ${_url}
    URL_HASH SHA256=5ef0c82e17b26c90ecd06f0feaeb60892bf1f9a8beef89dce20f3425bec337de
    PREFIX "${_base}"
    DEPENDS ${_libxml2_depends}
    CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DCMAKE_DEBUG_POSTFIX:STRING=
    CMAKE_ARGS -DLIBXML2_WITH_PYTHON:BOOL=OFF
    CMAKE_ARGS -DLIBXML2_WITH_TESTS:BOOL=OFF
    CMAKE_ARGS -DLIBXML2_WITH_PROGRAMS:BOOL=OFF
    CMAKE_ARGS -DLIBXML2_WITH_TESTS:BOOL=OFF
    CMAKE_ARGS ${_libxml2_extra_args}
    BUILD_COMMAND COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
    INSTALL_COMMAND COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_libxml2)     # this is for github workflow in cache-miss step.

# libs3
if(TD_LINUX)
    set(ext_libs3_static liblibs3.a)
elseif(TD_DARWIN)
    set(ext_libs3_static liblibs3.a)
elseif(TD_WINDOWS)
    set(ext_libs3_static libs3.lib)
endif()
INIT_EXT(ext_libs3
    INC_DIR          include
    LIB              lib/${ext_libs3_static}
)
string(JOIN " " _ssl_libs ${ext_ssl_libs})
set(_libs3_extra_args "")
set(_libs3_depends ext_libxml2 ext_curl ext_zlib)
if(TD_WINDOWS)
    list(APPEND _libs3_extra_args "-DCMAKE_C_FLAGS:STRING=/DWIN32_LEAN_AND_MEAN /DLIBXML_STATIC")
    list(APPEND _libs3_extra_args "-DPTHREAD_INCLUDE:STRING=${ext_pthread_inc_dir}")
    list(APPEND _libs3_extra_args "-DPTHREAD_LIBS:STRING=${ext_pthread_libs}")
    list(APPEND _libs3_depends ext_pthread)
endif()
# Source: https://github.com/taosdata/libs3 commit f727a1e (bji/libs3@98f667b + Windows/MSVC support)
get_from_local_if_exists(
    "https://github.com/taosdata/libs3/archive/f727a1e5da21ed518c323a849dda70d39ccfe647.tar.gz"
    "libs3-f727a1e5da21.tar.gz"
)
set(_libs3_ts_args "")
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24")
    list(APPEND _libs3_ts_args DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
endif()
ExternalProject_Add(ext_libs3
    URL ${_url}
    URL_HASH SHA256=008ce6c8881b84313b22025303b0076b75a2da9a94e7cb255e25ec39d01b096c
    ${_libs3_ts_args}
    DEPENDS ${_libs3_depends}
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
    CMAKE_ARGS ${_libs3_extra_args}
    CMAKE_ARGS -DCURL_INCLUDE:STRING=${ext_curl_inc_dir}
    CMAKE_ARGS -DCURL_LIBS:STRING=${ext_curl_libs}
    CMAKE_ARGS -DOPENSSL_INCLUDE:STRING=${ext_ssl_inc_dir}
    CMAKE_ARGS -DOPENSSL_LIBS:STRING=${ext_ssl_lib_ssl}
    CMAKE_ARGS -DCRYPTO_LIBS:STRING=${ext_ssl_lib_crypto}
    CMAKE_ARGS -DLIBXML2_INCLUDE:STRING=${ext_libxml2_inc_dir}
    CMAKE_ARGS -DLIBXML2_LIBS:STRING=${ext_libxml2_libs}
    CMAKE_ARGS -DZLIB_INCLUDE:STRING=${ext_zlib_inc_dir}
    CMAKE_ARGS -DZLIB_LIBS:STRING=${ext_zlib_libs}
    BUILD_COMMAND
        COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
    INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_libs3)     # this is for github workflow in cache-miss step.

if(NOT TD_WINDOWS)        # {
    # azure
    if(TD_LINUX)
        set(ext_azure_static libtd_azure_sdk.a)
    elseif(TD_DARWIN)
        set(ext_azure_static libtd_azure_sdk.a)
    elseif(TD_WINDOWS)
        set(ext_azure_static td_azure_sdk.lib)
    endif()
    INIT_EXT(ext_azure
        INC_DIR          include
        LIB              lib/${ext_azure_static}
    )
    # URL https://github.com/Azure/azure-sdk-for-cpp/archive/refs/tags/azure-storage-blobs_12.13.0-beta.1.tar.gz
    # URL_HASH SHA256=3eca486fd60e3522d0a633025ecd652a71515b1e944799b2e8ee31fd590305a9
    get_from_local_if_exists(
        "https://github.com/Azure/azure-sdk-for-cpp/archive/refs/tags/azure-storage-blobs_12.13.0-beta.1.tar.gz"
        "azure-storage-blobs_12.13.0-beta.1.tar.gz"
    )
    ExternalProject_Add(ext_azure
        URL ${_url}
        URL_HASH SHA256=3eca486fd60e3522d0a633025ecd652a71515b1e944799b2e8ee31fd590305a9
        # GIT_SHALLOW TRUE
        DEPENDS ext_libxml2 ext_curl ext_zlib
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DCURL_INCLUDE:STRING=${ext_curl_inc_dir}
        CMAKE_ARGS -DCURL_LIBS:STRING=${ext_curl_libs}
        CMAKE_ARGS -DOPENSSL_INCLUDE:STRING=${ext_ssl_inc_dir}
        CMAKE_ARGS -DOPENSSL_LIBS:STRING=${ext_ssl_lib_ssl}
        CMAKE_ARGS -DCRYPTO_LIBS:STRING=${ext_ssl_lib_crypto}
        CMAKE_ARGS -DLIBXML2_INCLUDE:STRING=${ext_libxml2_inc_dir}
        CMAKE_ARGS -DLIBXML2_LIBS:STRING=${ext_libxml2_libs}
        CMAKE_ARGS -DZLIB_INCLUDE:STRING=${ext_zlib_inc_dir}
        CMAKE_ARGS -DZLIB_LIBS:STRING=${ext_zlib_libs}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${TD_SUPPORT_DIR}/in/azure.CMakeLists.txt.in" "${ext_azure_source}/CMakeLists.txt"
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_azure)     # this is for github workflow in cache-miss step.

    # mxml
    if(TD_LINUX)
        set(ext_mxml_static libmxml.a)
    elseif(TD_DARWIN)
        set(ext_mxml_static libmxml.a)
    elseif(TD_WINDOWS)
        set(ext_mxml_static mxml.lib)
    endif()
    INIT_EXT(ext_mxml
        INC_DIR          include
        LIB              lib/${ext_mxml_static}
    )
    get_from_local_if_exists(
        "https://github.com/michaelrsweet/mxml/archive/refs/tags/v2.12.tar.gz"
        "mxml-v2.12.tar.gz"
    )
    ExternalProject_Add(ext_mxml
        URL ${_url}
        # NOTE: if you change the version, refer to the comments below!!!
        URL_HASH SHA256=4d850d15cdd4fdb9e82817eb069050d7575059a9a2729c82b23440e4445da199
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND ""
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )

    # NOTE: tweaking to prevent mxml from being rebuilt each time requested
    #       any other better approach?
    add_custom_command(
        OUTPUT
          ${ext_mxml_source}/configure
          ${ext_mxml_source}/Makefile.in
          ${ext_mxml_source}/README.md
          ${ext_mxml_source}/CHANGES.md
        DEPENDS ext_mxml
        WORKING_DIRECTORY ${ext_mxml_source}
    )

    add_custom_command(
        OUTPUT
          ${ext_mxml_source}/install/lib/${ext_mxml_static}
        DEPENDS
          ${ext_mxml_source}/configure
          ${ext_mxml_source}/Makefile.in
          ${ext_mxml_source}/README.md
          ${ext_mxml_source}/CHANGES.md
        WORKING_DIRECTORY ${ext_mxml_source}
        COMMAND pwd
        COMMAND ./configure --prefix=${ext_mxml_source}/install --enable-shared=no
        COMMAND ${_EXT_ENV_PREFIX} make DESTDIR=${ext_mxml_source}/install
        COMMAND ${_EXT_ENV_PREFIX} make DESTDIR=${ext_mxml_source}/install install
    )

    add_custom_target(ext_mxml_post
        DEPENDS
          ${ext_mxml_source}/install/lib/${ext_mxml_static}
        WORKING_DIRECTORY ${ext_mxml_source}
        COMMAND "${CMAKE_COMMAND}" -E echo ${ext_mxml_source}/install/lib/${ext_mxml_static}
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./install/include/mxml.h ${_ins}/include/mxml.h
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different ./install/lib/${ext_mxml_static} ${_ins}/lib/${ext_mxml_static}
    )
    add_dependencies(build_externals ext_mxml_post)     # this is for github workflow in cache-miss step.

    # apr
    if(TD_LINUX)
        set(ext_apr_static libapr-1.a)
    elseif(TD_DARWIN)
        set(ext_apr_static libapr-1.a)
    elseif(TD_WINDOWS)
        set(ext_apr_static apr-1.lib)
    endif()
    INIT_EXT(ext_apr
        INC_DIR          include/apr-1
        LIB              lib/${ext_apr_static}
    )
    # URL https://dlcdn.apache.org//apr/apr-1.7.4.tar.gz
    # URL_HASH SHA256=a4137dd82a185076fa50ba54232d920a17c6469c30b0876569e1c2a05ff311d9
    get_from_local_if_exists(
        "https://dlcdn.apache.org//apr/apr-1.7.6.tar.gz"
        "apr-1.7.6.tar.gz"
    )
    ExternalProject_Add(ext_apr
        URL ${_url}
        URL_HASH SHA256=6a10e7f7430510600af25fabf466e1df61aaae910bf1dc5d10c44a4433ccc81d
        # GIT_SHALLOW TRUE
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DAPR_BUILD_SHARED:BOOL=OFF
        PATCH_COMMAND ""
        CONFIGURE_COMMAND
            COMMAND ./configure --prefix=${_ins} --enable-shared=no
        BUILD_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make            # NOTE: do NOT specify DESTDIR=
        INSTALL_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make install    # NOTE: do NOT specify DESTDIR=
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_apr)

    # apr-util
    if(TD_LINUX)
        set(ext_aprutil_static libaprutil-1.a)
    elseif(TD_DARWIN)
        set(ext_aprutil_static libaprutil-1.a)
    elseif(TD_WINDOWS)
        set(ext_aprutil_static aprutil-1.lib)
    endif()
    INIT_EXT(ext_aprutil
        INC_DIR          include/apr-1
        LIB              lib/${ext_aprutil_static}
    )
    # URL https://dlcdn.apache.org//apr/apr-util-1.6.3.tar.gz
    # URL_HASH SHA256=2b74d8932703826862ca305b094eef2983c27b39d5c9414442e9976a9acf1983
    get_from_local_if_exists(
        "https://dlcdn.apache.org//apr/apr-util-1.6.3.tar.gz"
        "apr-util-1.6.3.tar.gz"
    )
    ExternalProject_Add(ext_aprutil
        URL ${_url}
        URL_HASH SHA256=2b74d8932703826862ca305b094eef2983c27b39d5c9414442e9976a9acf1983
        # GIT_SHALLOW TRUE
        DEPENDS ext_apr
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
        PATCH_COMMAND ""
        CONFIGURE_COMMAND
            COMMAND ./configure --prefix=${_ins} --enable-shared=no --with-apr=${ext_apr_install}
        BUILD_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make            # NOTE: do NOT specify DESTDIR=
        INSTALL_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make install    # NOTE: do NOT specify DESTDIR=
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_aprutil)

    # cos
    if(TD_LINUX)
        set(ext_cos_static libcos-1.a)
    elseif(TD_DARWIN)
        set(ext_cos_static libcos-1.a)
    elseif(TD_WINDOWS)
        set(ext_cos_static cos-1.lib)
        set(_c_flags_list)
    endif()
    INIT_EXT(ext_cos
        INC_DIR          include
        LIB              lib/${ext_cos_static}
    )
    get_from_local_if_exists(
        "https://github.com/tencentyun/cos-c-sdk-v5/archive/refs/tags/v5.0.16.tar.gz"
        "cos-c-sdk-v5-v5.0.16.tar.gz"
    )
    ExternalProject_Add(ext_cos
        URL ${_url}
        URL_HASH SHA256=4f83633cbf453e756981f74637155db41f450edc2723378185c6d4e1ceedf48b
        DEPENDS ext_curl ext_mxml_post ext_aprutil
        PREFIX "${_base}"
        # BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DAPR_INCLUDE_DIR:STRING=${ext_apr_inc_dir}
        CMAKE_ARGS -DAPR_UTIL_INCLUDE_DIR:STRING=${ext_aprutil_inc_dir}
        CMAKE_ARGS -DMINIXML_INCLUDE_DIR:STRING=${ext_mxml_inc_dir}
        CMAKE_ARGS -DCURL_INCLUDE_DIR:STRING=${ext_curl_inc_dir}
        CMAKE_ARGS -DMINIXML_LIBRARY:STRING=${ext_mxml_libs}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${TD_SUPPORT_DIR}/in/cos.CMakeLists.txt.in" "${ext_cos_source}/CMakeLists.txt"
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_cos)
endif()                      # }

if(TD_LINUX AND TD_ENTERPRISE)        # {
if(${BUILD_LIBSASL})      # {
    if(${TD_LINUX})
        set(ext_sasl2 libsasl2.a)
        set(_c_flags_list -fPIC)
    endif()

    INIT_EXT(ext_sasl2
        INC_DIR          include
        LIB              lib/${ext_sasl2}
    )
    get_from_local_if_exists(
        "https://github.com/cyrusimap/cyrus-sasl/archive/refs/tags/cyrus-sasl-2.1.27.tar.gz"
        "cyrus-sasl-cyrus-sasl-2.1.27.tar.gz"
    )
    ExternalProject_Add(ext_sasl2
        URL ${_url}
        URL_HASH SHA256=b564d773803dc4cff42d2bdc04c80f2b105897a724c247817d4e4a99dd6b9976
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND ./autogen.sh
            COMMAND sed -i "s/#define PROTOTYPES 0/#define PROTOTYPES 1/" include/makemd5.c saslauthd/md5global.h
        CONFIGURE_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E env
                CC=gcc
                CC_FOR_BUILD=gcc
                CFLAGS=-Wno-missing-braces
                CXXFLAGS=-Wno-missing-braces
                ./configure -prefix=${_ins} --with-pic --enable-static=yes --without-openssl --enable-shared=no --enable-plain --enable-anon --enable-scram=no --enable-login=no --enable-digest=no --with-saslauthd=no --with-authdaemond=no
        BUILD_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make
        INSTALL_COMMAND
            COMMAND ${_EXT_ENV_PREFIX} make install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_sasl2)     # this is for github workflow in cache-miss step.
endif(${BUILD_LIBSASL})   # }
endif()

if(BUILD_PYUDF)

# ── CPython SDK (headers + import libs, auto-downloaded) ─────────────────
# Downloads prebuilt Python from python-build-standalone. Eliminates the
# need to install Python on the build machine.
# BUILD_PYUDF_PYTHON_VERSION must be set (default provided in options.cmake).

# PBS release tag — internal, tightly coupled to version list above.
set(_pyudf_pbs_release "20260510")

if(NOT BUILD_PYUDF_PYTHON_VERSION)
    message(FATAL_ERROR
    "[pyudf] BUILD_PYUDF=ON but BUILD_PYUDF_PYTHON_VERSION is not set.\n"
    "  Set -DBUILD_PYUDF_PYTHON_VERSION:STRING=\"3.15.0b1\" "
        "or set -DBUILD_PYUDF=OFF to disable Python UDF plugin.")
endif()
set(_pyver "${BUILD_PYUDF_PYTHON_VERSION}")

# Platform triple for python-build-standalone
if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|ARM64|arm64")
    set(_pbs_arch "aarch64")
else()
    set(_pbs_arch "x86_64")
endif()
if(TD_WINDOWS)
    set(_pbs_triple "${_pbs_arch}-pc-windows-msvc")
elseif(APPLE)
    set(_pbs_triple "${_pbs_arch}-apple-darwin")
else()
    set(_pbs_triple "${_pbs_arch}-unknown-linux-gnu")
endif()

set(PYUDF_CPYTHON_TARGET "" CACHE INTERNAL "Single ext_cpython target for pyudf")
string(REGEX MATCH "^([0-9]+)\\.([0-9]+)" _ver_short "${_pyver}")
if(NOT _ver_short)
    message(FATAL_ERROR "[pyudf] Invalid BUILD_PYUDF_PYTHON_VERSION='${_pyver}', expected format like 3.15.0b1")
endif()
string(REPLACE "." "_" _ver_safe "${_ver_short}")
string(REPLACE "." "" _vermm "${_ver_short}")
set(_extname "ext_cpython_${_ver_safe}")

INIT_DIRS(${_extname} ${TD_EXTERNALS_BASE_DIR})

set(_url "https://github.com/astral-sh/python-build-standalone/releases/download/${_pyudf_pbs_release}/cpython-${_pyver}+${_pyudf_pbs_release}-${_pbs_triple}-install_only.tar.gz")

ExternalProject_Add(${_extname}
    URL "${_url}"
    PREFIX "${_base}"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" -E copy_directory "<SOURCE_DIR>" "${_ins}/python"
    EXCLUDE_FROM_ALL TRUE
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
add_dependencies(build_externals ${_extname})

# Store paths for pyudf/CMakeLists.txt
# The archive top-level "python/" becomes SOURCE_DIR after extraction.
# On Windows, headers are at include/ (flat), on Linux at include/python3.XX/
if(TD_WINDOWS)
    set(${_extname}_inc_dir  "${_ins}/python/include" CACHE INTERNAL "")
    # abi3: prefer the stable import lib so taospyudf.dll imports
    # python3.dll instead of a minor-locked python3XX.dll.
    # For supported SDK baselines (3.14.3 / 3.15.0b1), python3.lib is present.
    set(${_extname}_lib_path "${_ins}/python/libs/python3.lib" CACHE INTERNAL "")
else()
    set(${_extname}_inc_dir  "${_ins}/python/include/python${_ver_short}" CACHE INTERNAL "")
    set(${_extname}_lib_path "" CACHE INTERNAL "")  # Linux: no libpython linking
endif()
set(${_extname}_ver_short "${_ver_short}" CACHE INTERNAL "")
set(${_extname}_ver_safe  "${_ver_safe}"  CACHE INTERNAL "")

set(PYUDF_CPYTHON_TARGET "${_extname}" CACHE INTERNAL "Single ext_cpython target for pyudf")

message(STATUS "[pyudf] Will download CPython ${_pyver} SDK for ${_pbs_triple}")

# ── plog (header-only, for Python UDF plugin logging) ────────────────────
INIT_EXT(ext_plog
    INC_DIR          include
)
get_from_local_repo_if_exists("https://github.com/SergiusTheBest/plog.git")
ExternalProject_Add(ext_plog
    GIT_REPOSITORY ${_git_url}
    GIT_TAG 1.1.10
    GIT_SHALLOW TRUE
    PREFIX "${_base}"
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${_ins}/include"
        COMMAND "${CMAKE_COMMAND}" -E copy_directory "${ext_plog_source}/include/plog" "${_ins}/include/plog"
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_plog)
endif() # BUILD_PYUDF
