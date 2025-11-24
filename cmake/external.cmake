option(TD_EXTERNALS_USE_ONLY "external dependencies use only, otherwise download-build-install" OFF)
option(TD_ALIGN_EXTERNAL "keep externals' CMAKE_BUILD_TYPE align with the main project" ON)

# eg.: cmake -B debug -DCMAKE_BUILD_TYPE:STRING=Debug
#      TD_CONFIG_NAME will be `Debug`
#   for multi-configuration tools, such as `Visual Studio ...`
#      cmake --build build --config Release
#      TD_CONFIG_NAME will be `Release`
set(TD_CONFIG_NAME "$<IF:$<STREQUAL:z$<CONFIG>,z>,$<IF:$<STREQUAL:z${CMAKE_BUILD_TYPE},z>,Debug,${CMAKE_BUILD_TYPE}>,$<CONFIG>>")
if(NOT ${TD_ALIGN_EXTERNAL})
    if(NOT ${TD_WINDOWS})
        set(TD_CONFIG_NAME "Release")
    endif()
endif()

set(TD_EXTERNALS_BASE_DIR "${CMAKE_SOURCE_DIR}/.externals" CACHE PATH "path where external dependencies reside")
message(STATUS "TD_EXTERNALS_BASE_DIR:${TD_EXTERNALS_BASE_DIR}")

set(TD_INTERNALS_BASE_DIR "${CMAKE_SOURCE_DIR}/.internals" CACHE PATH "path where internal dependencies reside")
message(STATUS "TD_INTERNALS_BASE_DIR:${TD_INTERNALS_BASE_DIR}")

include(ExternalProject)

add_custom_target(build_externals)

macro(INIT_DIRS name base_dir)     # {
    set(_base            "${base_dir}/build/${name}")                      # where all source and build stuffs locate
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

    if(${BUILD_CONTRIB} OR NOT ${${name}_have_dev})
      set(${name}_build_contrib     TRUE)
    else()
      set(${name}_build_contrib     FALSE)
    endif()

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
        if(NOT ${TD_WINDOWS})
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
            if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
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
            if(NOT ${TD_WINDOWS})       # {
              if("z${name}" STREQUAL "zext_libuv")
                  target_link_libraries(${tgt} PUBLIC dl)
              endif()
            endif()                     # }
        else()
            foreach(v ${${name}_libs})
                target_link_libraries(${tgt} PRIVATE "${v}")
            endforeach()
            if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
                # make homebrew-installed-libs available
                target_link_directories(${tgt} PUBLIC "${BREW_PREFIX}/lib")
            endif()
        endif()
        add_definitions(-D_${name})
    endmacro()                               # }
endmacro()                         # }

set(LOCAL_REPO "" CACHE STRING "local repositories storage to use")
set(LOCAL_URL "" CACHE STRING "local archives storage to use")

# get_from_local_repo_if_exists/get_from_local_if_exists
# is for local storage of externals only
macro(get_from_local_repo_if_exists git_url)              # {
  # if LOCAL_REPO is set as: -DLOCAL_REPO:STRING=ssh://host/path-to-local-repo
  # then _git_url would be: ssh://host/path-to-local-repo/<git_url-name>.git
  if("z${LOCAL_REPO}" STREQUAL "z")
    set(_git_url "${git_url}")
  else()
    string(FIND ${git_url} "/" _pos REVERSE)
    string(SUBSTRING ${git_url} ${_pos} -1 _name)
    set(_git_url "${LOCAL_REPO}/${_name}")
  endif()
endmacro()                                                # }

macro(get_from_local_if_exists url)                       # {
  if("z${LOCAL_URL}" STREQUAL "z")
    set(_url "${url}")
  else()
    string(FIND ${url} "/" _pos REVERSE)
    string(SUBSTRING ${url} ${_pos} -1 _name)
    set(_url "${LOCAL_URL}/${_name}")
  endif()
endmacro()                                                # }

# zlib
if(${TD_LINUX})
    set(ext_zlib_static libz.a)
elseif(${TD_DARWIN})
    set(ext_zlib_static libz.a)
elseif(${TD_WINDOWS})
    set(ext_zlib_static zlibstatic$<$<STREQUAL:${TD_CONFIG_NAME},Debug>:d>.lib)
endif()
INIT_EXT(ext_zlib
    INC_DIR          include
    LIB              lib/${ext_zlib_static}
    CHK_NAME         ZLIB
)
# GIT_REPOSITORY https://github.com/taosdata-contrib/zlib.git
# GIT_TAG        v1.3.1
get_from_local_repo_if_exists("https://github.com/madler/zlib.git")
ExternalProject_Add(ext_zlib
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v1.3.1 
    GIT_SHALLOW TRUE
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}        # if main project is built in Debug, ext_zlib is too
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}                # let default INSTALL step use
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON            # linking consistent
    CMAKE_ARGS -DZLIB_BUILD_SHARED:BOOL=OFF
    CMAKE_ARGS -DZLIB_BUILD_TESTING:BOOL=OFF
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_zlib)     # this is for github workflow in cache-miss step.

# pthread
if(${BUILD_PTHREAD})        # {
    if(${TD_WINDOWS})
        set(ext_pthread_static pthreadVC3.lib)
        set(ext_pthread_dll pthreadVC3.dll)
    endif()
    INIT_EXT(ext_pthread
        INC_DIR          include
        LIB              lib/${ext_pthread_static}
    )
    # GIT_REPOSITORY https://github.com/GerHobbelt/pthread-win32
    # GIT_TAG v3.0.3.1
    get_from_local_repo_if_exists("https://github.com/GerHobbelt/pthread-win32")
    ExternalProject_Add(ext_pthread
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 3309f4d6e7538f349ae450347b02132ecb0606a7
        GIT_SHALLOW FALSE
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
if(${BUILD_WITH_ICONV})     # {
    if(${TD_WINDOWS})
        set(ext_iconv_static iconv.lib)
    endif()
    INIT_EXT(ext_iconv
        INC_DIR          include
        LIB              lib/${ext_iconv_static}
    )
    # GIT_REPOSITORY https://github.com/win-iconv/win-iconv.git
    # GIT_TAG v0.0.8
    get_from_local_repo_if_exists("https://github.com/win-iconv/win-iconv.git")
    ExternalProject_Add(ext_iconv
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 9f98392dfecadffd62572e73e9aba878e03496c4
        GIT_SHALLOW FALSE
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
if(${BUILD_MSVCREGEX})      # {
    if(${TD_WINDOWS})
        set(ext_msvcregex_static regex$<$<CONFIG:Debug>:_d>.lib)
    endif()
    INIT_EXT(ext_msvcregex
        INC_DIR          include
        LIB              lib/${ext_msvcregex_static}
    )
    # GIT_REPOSITORY https://gitee.com/l0km/libgnurx-msvc.git
    # GIT_TAG master
    get_from_local_repo_if_exists("https://gitee.com/l0km/libgnurx-msvc.git")
    ExternalProject_Add(ext_msvcregex
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 1a6514dd59bac8173ad4a55f63727d36269043cd
        GIT_SHALLOW FALSE
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND nmake /f NMakefile all test test2 test3
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${ext_msvcregex_source}/regex.h" "${_ins}/include/regex.h"
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${ext_msvcregex_source}/${ext_msvcregex_static}" "${_ins}/lib/${ext_msvcregex_static}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_msvcregex)     # this is for github workflow in cache-miss step.
endif()                     # }

# wcwidth
if(${BUILD_WCWIDTH})        # {
    if(${TD_WINDOWS})
        set(ext_wcwidth_static wcwidth.lib)
    endif()
    INIT_EXT(ext_wcwidth
        INC_DIR          include
        LIB              lib/${ext_wcwidth_static}
    )
    # GIT_REPOSITORY https://github.com/fumiyas/wcwidth-cjk.git
    # GIT_TAG master
    get_from_local_repo_if_exists("https://github.com/fumiyas/wcwidth-cjk.git")
    ExternalProject_Add(ext_wcwidth
        GIT_REPOSITORY ${_git_url}
        GIT_TAG a1b1e2c346a563f6538e46e1d29c265bdd5b1c9a
        GIT_SHALLOW FALSE
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
if(${BUILD_WINGETOPT})      # {
    if(${TD_WINDOWS})
        set(ext_wingetopt_static wingetopt.lib)
    endif()
    INIT_EXT(ext_wingetopt
        INC_DIR          include
        LIB              lib/${ext_wingetopt_static}
    )
    # GIT_REPOSITORY https://github.com/alex85k/wingetopt.git
    # GIT_TAG master
    get_from_local_repo_if_exists("https://github.com/alex85k/wingetopt.git")
    ExternalProject_Add(ext_wingetopt
        GIT_REPOSITORY ${_git_url}
        GIT_TAG e8531ed21b44f5a723c1dd700701b2a58ce3ea01
        GIT_SHALLOW FALSE
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
if(${BUILD_TEST})           # {
    if(${TD_LINUX})
        set(ext_gtest_static libgtest.a)
        set(ext_gtest_main libgtest_main.a)
    elseif(${TD_DARWIN})
        set(ext_gtest_static libgtest.a)
        set(ext_gtest_main libgtest_main.a)
    elseif(${TD_WINDOWS})
        set(ext_gtest_static gtest.lib)
        set(ext_gtest_main gtest_main.lib)
    endif()
    INIT_EXT(ext_gtest
        INC_DIR          include
        LIB              lib/${ext_gtest_main}
                         lib/${ext_gtest_static}
    )
    # GIT_REPOSITORY https://github.com/taosdata-contrib/googletest.git
    # GIT_TAG release-1.11.0
    get_from_local_repo_if_exists("https://github.com/google/googletest.git")
    ExternalProject_Add(ext_gtest
        GIT_REPOSITORY ${_git_url}
        GIT_TAG release-1.12.0
        GIT_SHALLOW TRUE
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -Dgtest_force_shared_crt:BOOL=ON
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_gtest)     # this is for github workflow in cache-miss step.
endif(${BUILD_TEST})        # }

# cppstub
if(${BUILD_TEST})           # {
    if(${TD_LINUX})
        set(ext_cppstub_static libcppstub.a)
        set(_platform_dir      src_linux)
    elseif(${TD_DARWIN})
        set(ext_cppstub_static libcppstub.a)
        set(_platform_dir      src_darwin)
    elseif(${TD_WINDOWS})
        set(ext_cppstub_static cppstub.lib)
        set(_platform_dir      src_win)
    endif()
    INIT_EXT(ext_cppstub
        INC_DIR          include
    )
    # GIT_REPOSITORY https://github.com/coolxv/cpp-stub.git
    # GIT_TAG 3137465194014d66a8402941e80d2bccc6346f51
    # GIT_SUBMODULES "src"
    get_from_local_repo_if_exists("https://github.com/coolxv/cpp-stub.git")
    ExternalProject_Add(ext_cppstub
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 3137465194014d66a8402941e80d2bccc6346f51
        GIT_SHALLOW FALSE
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
endif(${BUILD_TEST})        # }

# lz4
if(${TD_LINUX})
    set(ext_lz4_static liblz4.a)
elseif(${TD_DARWIN})
    set(ext_lz4_static liblz4.a)
elseif(${TD_WINDOWS})
    set(ext_lz4_static lz4.lib)
endif()
INIT_EXT(ext_lz4
    INC_DIR          include
    LIB              lib/${ext_lz4_static}
    CHK_NAME         LZ4
)
# GIT_REPOSITORY https://github.com/taosdata-contrib/lz4.git
# GIT_TAG v1.9.3
get_from_local_repo_if_exists("https://github.com/lz4/lz4.git")
ExternalProject_Add(ext_lz4
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v1.10.0
    GIT_SHALLOW TRUE
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
if(${TD_LINUX})
    set(ext_cjson_static libcjson.a)
elseif(${TD_DARWIN})
    set(ext_cjson_static libcjson.a)
elseif(${TD_WINDOWS})
    set(ext_cjson_static cjson.lib)
endif()
INIT_EXT(ext_cjson
    INC_DIR          include/cjson           # TODO: tweak in this way to hack #include <cJSON.h> in source codes
    LIB              lib/${ext_cjson_static}
)
# GIT_REPOSITORY https://github.com/taosdata-contrib/cJSON.git
# GIT_TAG v1.7.15
get_from_local_repo_if_exists("https://github.com/DaveGamble/cJSON.git")
ExternalProject_Add(ext_cjson
    GIT_REPOSITORY ${_git_url}
    GIT_TAG 12c4bf1986c288950a3d06da757109a6aa1ece38
    GIT_SHALLOW FALSE
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
if(${TD_LINUX})
    set(ext_xz_static liblzma.a)
elseif(${TD_DARWIN})
    set(ext_xz_static liblzma.a)
elseif(${TD_WINDOWS})
    set(ext_xz_static lzma.lib)
endif()
INIT_EXT(ext_xz
    INC_DIR          include
    LIB              lib/${ext_xz_static}
    # debugging github working flow
    # CHK_NAME         LZMA
)
# GIT_REPOSITORY https://github.com/xz-mirror/xz.git
# GIT_TAG v5.4.4
get_from_local_repo_if_exists("https://github.com/tukaani-project/xz.git")
ExternalProject_Add(ext_xz
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v5.8.1
    GIT_SHALLOW TRUE
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
if(${TD_LINUX})
    set(ext_xxhash_static libxxhash.a)
elseif(${TD_DARWIN})
    set(ext_xxhash_static libxxhash.a)
elseif(${TD_WINDOWS})
    set(ext_xxhash_static xxhash.lib)
endif()
get_from_local_repo_if_exists("https://github.com/Cyan4973/xxHash.git")
if(NOT ${TD_WINDOWS})        # {
    INIT_EXT(ext_xxhash
        INC_DIR          "usr/local/include"
        LIB              "usr/local/lib/${ext_xxhash_static}"
    )
    ExternalProject_Add(ext_xxhash
        GIT_REPOSITORY ${_git_url}
        GIT_TAG de9d6577907d4f4f8153e96b0cb0cbdf7df649bb
        GIT_SHALLOW FALSE
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/xxhash.Makefile Makefile
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND make DESTDIR=${_ins}
        INSTALL_COMMAND
            COMMAND make DESTDIR=${_ins} install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
else()                       # }{
    INIT_EXT(ext_xxhash
        INC_DIR          "include"
        LIB              "lib/${ext_xxhash_static}"
    )
    ExternalProject_Add(ext_xxhash
        GIT_REPOSITORY ${_git_url}
        GIT_TAG de9d6577907d4f4f8153e96b0cb0cbdf7df649bb
        GIT_SHALLOW FALSE
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
if(${TD_LINUX})
    set(ext_lzma2_static libfast-lzma2.a)
    INIT_EXT(ext_lzma2
        INC_DIR          usr/local/include
        LIB              usr/local/lib/${ext_lzma2_static}
    )
    # GIT_REPOSITORY https://github.com/conor42/fast-lzma2.git
    get_from_local_repo_if_exists("https://github.com/conor42/fast-lzma2.git")
    ExternalProject_Add(ext_lzma2
        GIT_REPOSITORY ${_git_url}
        GIT_TAG ded964d203cabe1a572d2c813c55e8a94b4eda48
        GIT_SHALLOW FALSE
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/lzma2.Makefile Makefile
            # NOTE: xxhash.h is now introduced by ext_xxhash
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND make DESTDIR=${_ins}
        INSTALL_COMMAND
            COMMAND make DESTDIR=${_ins} install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_lzma2)     # this is for github workflow in cache-miss step.
endif()

# libuv
if(${BUILD_WITH_UV})        # {
    if(${TD_LINUX})
        set(ext_libuv_static libuv.a)
    elseif(${TD_DARWIN})
        set(ext_libuv_static libuv.a)
    elseif(${TD_WINDOWS})
        set(ext_libuv_static libuv.lib)
    endif()
    INIT_EXT(ext_libuv
        INC_DIR          include
        LIB              lib/${ext_libuv_static}
        CHK_NAME         LIBUV
    )
    # GIT_REPOSITORY https://github.com/libuv/libuv.git
    # GIT_TAG v1.49.2
    get_from_local_repo_if_exists("https://github.com/libuv/libuv.git")
    ExternalProject_Add(ext_libuv
        GIT_REPOSITORY ${_git_url}
        GIT_TAG v1.49.2
        GIT_SHALLOW TRUE
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
endif(${BUILD_WITH_UV})     # }

# tz
if(NOT ${TD_WINDOWS})       # {
    if(${TD_LINUX})
        set(ext_tz_static libtz.a)
        set(_c_flags_list -fPIC)
    elseif(${TD_DARWIN})
        set(ext_tz_static libtz.a)
        set(_c_flags_list -fPIC -DHAVE_GETTEXT=0) # TODO: brew install gettext?
    endif()
    INIT_EXT(ext_tz
        INC_DIR          include
        LIB              usr/lib/${ext_tz_static}
    )
    string(JOIN " " _c_flags ${_c_flags_list})
    # GIT_REPOSITORY https://github.com/eggert/tz.git
    # GIT_TAG main
    get_from_local_repo_if_exists("https://github.com/eggert/tz.git")
    ExternalProject_Add(ext_tz
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 2025a
        GIT_SHALLOW TRUE
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
            COMMAND make "CFLAGS=${_c_flags}" DESTDIR=${_ins}
        INSTALL_COMMAND
            COMMAND make "CFLAGS=${_c_flags}" DESTDIR=${_ins} install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_tz)     # this is for github workflow in cache-miss step.
endif(NOT ${TD_WINDOWS})    # }

# jemalloc
if(${JEMALLOC_ENABLED})     # {
    find_program(HAVE_AUTOCONF autoconf)
    if(${HAVE_AUTOCONF} STREQUAL "HAVE_AUTOCONF-NOTFOUND")
        message(FATAL_ERROR "`autoconf` not exist, you can install it by `sudo apt install autoconf` on linux, or `brew install autoconf` on MacOS")
    endif()
    if(${TD_LINUX})
        set(ext_jemalloc_static jemalloc.a)
    elseif(${TD_DARWIN})
        set(ext_jemalloc_static jemalloc.a)
    endif()
    INIT_EXT(ext_jemalloc
        INC_DIR          include
        LIB              lib/${ext_jemalloc_static}
    )
    # GIT_REPOSITORY https://github.com/jemalloc/jemalloc.git
    # GIT_TAG 5.3.0
    get_from_local_repo_if_exists("https://github.com/jemalloc/jemalloc.git")
    ExternalProject_Add(ext_jemalloc
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 5.3.0
        GIT_SHALLOW TRUE
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
            COMMAND make
        INSTALL_COMMAND
            COMMAND make install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_jemalloc)     # this is for github workflow in cache-miss step.
endif()                     # }

# sqlite
if(${BUILD_WITH_SQLITE})    # {
    if(${TD_LINUX})
        set(ext_sqlite_static sqlite.a)
    elseif(${TD_DARWIN})
        set(ext_sqlite_static sqlite.a)
    elseif(${TD_WINDOWS})
        set(ext_sqlite_static sqlite.lib)
    endif()
    INIT_EXT(ext_sqlite
        INC_DIR          include
        LIB              lib/${ext_sqlite_static}
        CHK_NAME         SQLITE3
    )
    # GIT_REPOSITORY https://github.com/sqlite/sqlite.git
    # GIT_TAG version-3.36.0
    get_from_local_repo_if_exists("https://github.com/sqlite/sqlite.git")
    ExternalProject_Add(ext_sqlite
        GIT_REPOSITORY ${_git_url}
        GIT_TAG version-3.36.0
        GIT_SHALLOW TRUE
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
endif(${BUILD_WITH_SQLITE}) # }

# crashdump
if(${BUILD_CRASHDUMP})      # {
    if(${TD_WINDOWS})
        set(ext_crashdump_static crashdump.lib)
    endif()
    INIT_EXT(ext_crashdump
        INC_DIR          include
        LIB              lib/${ext_crashdump_static}
    )
    # GIT_REPOSITORY https://github.com/Arnavion/crashdump.git
    # GIT_TAG master
    get_from_local_repo_if_exists("https://github.com/Arnavion/crashdump.git")
    ExternalProject_Add(ext_crashdump
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 149b43c10debdf28a2c50d79dee5ff344d83bd06
        GIT_SHALLOW FALSE
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
endif(${BUILD_CRASHDUMP})   # }

# ssl
if(NOT ${TD_WINDOWS})       # {
    # TODO: why at this moment???
    # file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.2/)
    if(${TD_LINUX})
        set(ext_ssl_static libssl.a)
        set(ext_crypto_static libcrypto.a)
    elseif(${TD_DARWIN})
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
    get_from_local_if_exists("https://github.com/openssl/openssl/releases/download/openssl-3.1.3/openssl-3.1.3.tar.gz")
    ExternalProject_Add(ext_ssl
        URL ${_url}
        URL_HASH SHA256=f0316a2ebd89e7f2352976445458689f80302093788c466692fb2a188b2eacf6
        # GIT_SHALLOW TRUE
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CONFIGURE_COMMAND
            # COMMAND ./Configure --prefix=$ENV{HOME}/.cos-local.2 no-shared
            COMMAND ./Configure --prefix=${_ins} no-shared --libdir=lib
        BUILD_COMMAND
            COMMAND make -j4
        INSTALL_COMMAND
            COMMAND make install_sw -j4
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_ssl)     # this is for github workflow in cache-miss step.
endif(NOT ${TD_WINDOWS})    # }

# libcurl
if(NOT ${TD_WINDOWS})       # {
    if(${TD_LINUX})
        set(ext_curl_static libcurl.a)
    elseif(${TD_DARWIN})
        set(ext_curl_static libcurl.a)
    endif()
    INIT_EXT(ext_curl
        INC_DIR          include
        LIB              lib/${ext_curl_static}
        # currently: tqStreamNotify.c uses curl_ws_send, but CURL4_OPENSSL exports curl_easy_send
        #            libcurl4-openssl-dev on ubuntu 22.04 is too old
        # CHK_NAME         CURL4_OPENSSL
    )
    # URL https://github.com/curl/curl/releases/download/curl-8_2_1/curl-8.2.1.tar.gz
    # URL_HASH MD5=b25588a43556068be05e1624e0e74d41
    get_from_local_if_exists("https://github.com/curl/curl/releases/download/curl-8_2_1/curl-8.2.1.tar.gz")
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
            COMMAND ./configure --prefix=${_ins} --with-ssl=${ext_ssl_install}
                    --enable-websockets --enable-shared=no --disable-ldap
                    --disable-ldaps --without-brotli --without-zstd
                    --without-libidn2 --without-nghttp2 --without-libpsl
                    --without-librtmp #--enable-debug
        BUILD_COMMAND
            COMMAND make -j4
        INSTALL_COMMAND
            COMMAND make install
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_curl)     # this is for github workflow in cache-miss step.
endif(NOT ${TD_WINDOWS})    # }

# geos
if(${BUILD_GEOS})           # {
    if(${TD_LINUX})
        set(ext_geos_static libgeos.a)
        set(ext_geos_c_static libgeos_c.a)
    elseif(${TD_DARWIN})
        set(ext_geos_static libgeos.a)
        set(ext_geos_c_static libgeos_c.a)
    elseif(${TD_WINDOWS})
        set(ext_geos_static geos.lib)
        set(ext_geos_c_static geos_c.lib)
    endif()
    INIT_EXT(ext_geos
        INC_DIR          include
        LIB              lib/${ext_geos_c_static}
                         lib/${ext_geos_static}
        CHK_NAME         GEOS
    )
    # GIT_REPOSITORY https://github.com/libgeos/geos.git
    # GIT_TAG 3.12.0
    get_from_local_repo_if_exists("https://github.com/libgeos/geos.git")
    ExternalProject_Add(ext_geos
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 3.12.0
        GIT_SHALLOW TRUE
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
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
if(${BUILD_ADDR2LINE})      # {
    if(${TD_LINUX})
        set(ext_dwarf_static libdwarf.a)
    elseif(${TD_DARWIN})
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
    if (${TD_DARWIN})      # {
      list(APPEND _c_cxx_flags_list
        -Wno-unused-command-line-argument
        -Wno-error=unused-but-set-variable
        -Wno-error=strict-prototypes
        -Wno-error=self-assign
        -Wno-error=null-pointer-subtraction
      )
    endif()                # }
    string(JOIN " " _c_cxx_flags ${_c_cxx_flags_list})

    # GIT_REPOSITORY https://github.com/davea42/libdwarf-code.git
    # GIT_TAG libdwarf-0.3.1
    get_from_local_repo_if_exists("https://github.com/davea42/libdwarf-code.git")
    ExternalProject_Add(ext_dwarf
        GIT_REPOSITORY ${_git_url}
        GIT_TAG libdwarf-0.3.1
        GIT_SHALLOW TRUE
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
endif(${BUILD_ADDR2LINE})   # }

# addr2line
if(${BUILD_ADDR2LINE})      # {
    if(${TD_LINUX})
        set(ext_addr2line_static libaddr2line.a)
    elseif(${TD_DARWIN})
        set(ext_addr2line_static libaddr2line.a)
    endif()
    INIT_EXT(ext_addr2line
        INC_DIR          include
        LIB              lib/${ext_addr2line_static}
    )
    # GIT_REPOSITORY https://github.com/davea42/libdwarf-addr2line.git
    # GIT_TAG main
    get_from_local_repo_if_exists("https://github.com/davea42/libdwarf-addr2line.git")
    ExternalProject_Add(ext_addr2line
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 9d76b420f9d1261fa7feada3a209e605f54ba859
        GIT_SHALLOW FALSE
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
endif(${BUILD_ADDR2LINE})   # }

# pcre2
if(${BUILD_PCRE2})          # {
    # TODO: seems no necessary cause strict rules has been enforced by geos
    if(${TD_LINUX})
        set(ext_pcre2_static libpcre2-8.a)
    elseif(${TD_DARWIN})
        set(ext_pcre2_static libpcre2-8.a)
    elseif(${TD_WINDOWS})
        set(ext_pcre2_static pcre2-8-static$<$<STREQUAL:${TD_CONFIG_NAME},Debug>:d>.lib)
    endif()
    INIT_EXT(ext_pcre2
        INC_DIR          include
        LIB              lib/${ext_pcre2_static}
    )
    # GIT_REPOSITORY https://github.com/PCRE2Project/pcre2.git
    # GIT_TAG pcre2-10.43
    get_from_local_repo_if_exists("https://github.com/PCRE2Project/pcre2.git")
    ExternalProject_Add(ext_pcre2
        GIT_REPOSITORY ${_git_url}
        # GIT_TAG db3b532aa0cc9bbaf804927b1f15566cadb4917a
        GIT_TAG pcre2-10.45
        GIT_SHALLOW TRUE
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

if (${BUILD_CONTRIB} OR NOT ${TD_LINUX})         # {
    if(${TD_LINUX})
        set(ext_rocksdb_static librocksdb.a)
    elseif(${TD_DARWIN})
        set(ext_rocksdb_static librocksdb.a)
    elseif(${TD_WINDOWS})
        set(ext_rocksdb_static rocksdb.lib)
    endif()
    INIT_EXT(ext_rocksdb
        INC_DIR          include
        LIB              lib/${ext_rocksdb_static}
    )
    # URL https://github.com/facebook/rocksdb/archive/refs/tags/v8.1.1.tar.gz
    # URL_HASH MD5=3b4c97ee45df9c8a5517308d31ab008b
    get_from_local_if_exists("https://github.com/facebook/rocksdb/archive/refs/tags/v8.1.1.tar.gz")
    ExternalProject_Add(ext_rocksdb
        URL ${_url}
        URL_HASH MD5=3b4c97ee45df9c8a5517308d31ab008b
        # GIT_SHALLOW TRUE
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
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
endif()                                          # }

if(TD_TAOS_TOOLS)
    if(${TD_LINUX})
        set(ext_jansson_static libjansson.a)
    elseif(${TD_DARWIN})
        set(ext_jansson_static libjansson.a)
    elseif(${TD_WINDOWS})
        set(ext_jansson_static jansson$<$<STREQUAL:${TD_CONFIG_NAME},Debug>:_d>.lib)
    endif()
    INIT_EXT(ext_jansson
        INC_DIR          include
        LIB              lib/${ext_jansson_static}
        CHK_NAME         JANSSON
    )
    get_from_local_repo_if_exists("https://github.com/akheron/jansson.git")
    ExternalProject_Add(ext_jansson
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 61fc3d0e28e1a35410af42e329cd977095ec32d2
        GIT_SHALLOW FALSE
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

    if(${TD_LINUX})
        set(ext_snappy_static libsnappy.a)
    elseif(${TD_DARWIN})
        set(ext_snappy_static libsnappy.a)
    elseif(${TD_WINDOWS})
        set(ext_snappy_static snappy.lib)
    endif()
    INIT_EXT(ext_snappy
        INC_DIR          include
        LIB              lib/${ext_snappy_static}
        CHK_NAME         snappy
    )
    get_from_local_repo_if_exists("https://github.com/google/snappy.git")
    ExternalProject_Add(ext_snappy
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 32ded457c0b1fe78ceb8397632c416568d6714a0
        GIT_SHALLOW FALSE
        GIT_SUBMODULES ""
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

    if(${TD_LINUX})
        set(ext_avro_static libavro.a)
        set(_c_flags_list "")
    elseif(${TD_DARWIN})
        set(ext_avro_static libavro.a)
        set(_c_flags_list "")
    elseif(${TD_WINDOWS})
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
    get_from_local_repo_if_exists("https://github.com/apache/avro.git")
    ExternalProject_Add(ext_avro
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 7b106b12ae22853c977259710d92a237d76f2236
        GIT_SHALLOW FALSE
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
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_avro)     # this is for github workflow in cache-miss step.
endif()


if(NOT ${TD_WINDOWS})        # {
    # libxml2
    if(${TD_LINUX})
        set(ext_libxml2_static libxml2.a)
    elseif(${TD_DARWIN})
        set(ext_libxml2_static libxml2.a)
    elseif(${TD_WINDOWS})
        set(ext_libxml2_static libxml2.lib)
    endif()
    INIT_EXT(ext_libxml2
        INC_DIR          include/libxml2
        LIB              lib/${ext_libxml2_static}
    )
    # URL https://github.com/GNOME/libxml2/archive/refs/tags/v2.10.4.tar.gz
    # URL_HASH SHA256=6f6fb27f91bb65f9d7196e3c616901b3e18a7dea31ccc2ae857940b125faa780
    get_from_local_repo_if_exists("https://github.com/GNOME/libxml2.git")
    ExternalProject_Add(ext_libxml2
        GIT_REPOSITORY ${_git_url}
        GIT_TAG v2.14.0
        GIT_SHALLOW TRUE
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_LIBDIR:PATH=lib
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
        CMAKE_ARGS -DLIBXML2_WITH_PYTHON:BOOL=OFF
        CMAKE_ARGS -DLIBXML2_WITH_TESTS:BOOL=OFF
        CMAKE_ARGS -DLIBXML2_WITH_PROGRAMS:BOOL=OFF
        CMAKE_ARGS -DLIBXML2_WITH_TESTS:BOOL=OFF

        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_libxml2)     # this is for github workflow in cache-miss step.

    # libs3
    if(${TD_LINUX})
        set(ext_libs3_static liblibs3.a)
    elseif(${TD_DARWIN})
        set(ext_libs3_static liblibs3.a)
    elseif(${TD_WINDOWS})
        set(ext_libs3_static libs3.lib)
    endif()
    INIT_EXT(ext_libs3
        INC_DIR          include
        LIB              lib/${ext_libs3_static}
    )
    string(JOIN " " _ssl_libs ${ext_ssl_libs})
    # GIT_REPOSITORY https://github.com/bji/libs3
    get_from_local_repo_if_exists("https://github.com/bji/libs3")
    ExternalProject_Add(ext_libs3
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 98f667b248a7288c1941582897343171cfdf441c
        GIT_SHALLOW FALSE
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
          COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/in/libs3.CMakeLists.txt.in ${ext_libs3_source}/CMakeLists.txt
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_libs3)     # this is for github workflow in cache-miss step.

    # azure
    if(${TD_LINUX})
        set(ext_azure_static libtd_azure_sdk.a)
    elseif(${TD_DARWIN})
        set(ext_azure_static libtd_azure_sdk.a)
    elseif(${TD_WINDOWS})
        set(ext_azure_static td_azure_sdk.lib)
    endif()
    INIT_EXT(ext_azure
        INC_DIR          include
        LIB              lib/${ext_azure_static}
    )
    # URL https://github.com/Azure/azure-sdk-for-cpp/archive/refs/tags/azure-storage-blobs_12.13.0-beta.1.tar.gz
    # URL_HASH SHA256=3eca486fd60e3522d0a633025ecd652a71515b1e944799b2e8ee31fd590305a9
    get_from_local_if_exists("https://github.com/Azure/azure-sdk-for-cpp/archive/refs/tags/azure-storage-blobs_12.13.0-beta.1.tar.gz")
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
    if(${TD_LINUX})
        set(ext_mxml_static libmxml.a)
    elseif(${TD_DARWIN})
        set(ext_mxml_static libmxml.a)
    elseif(${TD_WINDOWS})
        set(ext_mxml_static mxml.lib)
    endif()
    INIT_EXT(ext_mxml
        INC_DIR          include
        LIB              lib/${ext_mxml_static}
    )
    # GIT_REPOSITORY https://github.com/michaelrsweet/mxml.git
    # GIT_TAG v2.12
    get_from_local_repo_if_exists("https://github.com/michaelrsweet/mxml.git")
    ExternalProject_Add(ext_mxml
        GIT_REPOSITORY ${_git_url}
        # NOTE: if you change GIT_TAG here, refer to the comments below!!!
        GIT_TAG v2.12
        GIT_SHALLOW TRUE
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
        COMMAND make DESTDIR=${ext_mxml_source}/install
        COMMAND make DESTDIR=${ext_mxml_source}/install install
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
    if(${TD_LINUX})
        set(ext_apr_static libapr-1.a)
    elseif(${TD_DARWIN})
        set(ext_apr_static libapr-1.a)
    elseif(${TD_WINDOWS})
        set(ext_apr_static apr-1.lib)
    endif()
    INIT_EXT(ext_apr
        INC_DIR          include/apr-1
        LIB              lib/${ext_apr_static}
    )
    # URL https://dlcdn.apache.org//apr/apr-1.7.4.tar.gz
    # URL_HASH SHA256=a4137dd82a185076fa50ba54232d920a17c6469c30b0876569e1c2a05ff311d9
    get_from_local_if_exists("https://dlcdn.apache.org//apr/apr-1.7.6.tar.gz")
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
            COMMAND make            # NOTE: do NOT specify DESTDIR=
        INSTALL_COMMAND
            COMMAND make install    # NOTE: do NOT specify DESTDIR=
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_apr)

    # apr-util
    if(${TD_LINUX})
        set(ext_aprutil_static libaprutil-1.a)
    elseif(${TD_DARWIN})
        set(ext_aprutil_static libaprutil-1.a)
    elseif(${TD_WINDOWS})
        set(ext_aprutil_static aprutil-1.lib)
    endif()
    INIT_EXT(ext_aprutil
        INC_DIR          include/apr-1
        LIB              lib/${ext_aprutil_static}
    )
    # URL https://dlcdn.apache.org//apr/apr-util-1.6.3.tar.gz
    # URL_HASH SHA256=2b74d8932703826862ca305b094eef2983c27b39d5c9414442e9976a9acf1983
    get_from_local_if_exists("https://dlcdn.apache.org//apr/apr-util-1.6.3.tar.gz")
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
            COMMAND make            # NOTE: do NOT specify DESTDIR=
        INSTALL_COMMAND
            COMMAND make install    # NOTE: do NOT specify DESTDIR=
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_aprutil)

    # cos
    if(${TD_LINUX})
        set(ext_cos_static libcos-1.a)
    elseif(${TD_DARWIN})
        set(ext_cos_static libcos-1.a)
    elseif(${TD_WINDOWS})
        set(ext_cos_static cos-1.lib)
        set(_c_flags_list)
    endif()
    INIT_EXT(ext_cos
        INC_DIR          include
        LIB              lib/${ext_cos_static}
    )
    # GIT_REPOSITORY https://github.com/tencentyun/cos-c-sdk-v5.git
    # GIT_TAG v5.0.16
    get_from_local_repo_if_exists("https://github.com/tencentyun/cos-c-sdk-v5.git")
    ExternalProject_Add(ext_cos
        GIT_REPOSITORY ${_git_url}
        GIT_TAG v5.0.16
        GIT_SHALLOW TRUE
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

if(TD_WEBSOCKET)
    message("${Green} use libtaos-ws${ColourReset}")
    if(${TD_LINUX})
        set(ext_taosws_dll libtaosws.so)
        set(ext_taosws_lib_from libtaosws.a)
        set(ext_taosws_lib_to libtaosws.a)
        set(ext_taosws_link ${ext_taosws_dll})
    elseif(${TD_DARWIN})
        set(ext_taosws_dll libtaosws.dylib)
        set(ext_taosws_lib_from libtaosws.a)
        set(ext_taosws_lib_to libtaosws.a)
        set(ext_taosws_link ${ext_taosws_dll})
    elseif(${TD_WINDOWS})
        set(ext_taosws_dll taosws.dll)
        set(ext_taosws_lib_from taosws.dll.lib)
        set(ext_taosws_lib_to taosws.lib)
        set(ext_taosws_link ${ext_taosws_lib_to})
    endif()
    INIT_EXT(ext_taosws
        INC_DIR include
    )
    get_from_local_repo_if_exists("https://github.com/taosdata/taos-connector-rust.git")
    ExternalProject_Add(ext_taosws
        GIT_REPOSITORY ${_git_url}
        GIT_TAG ${TAOSWS_GIT_TAG_NAME}
        GIT_SHALLOW ${TAOSWS_GIT_TAG_SHALLOW}
        BUILD_IN_SOURCE TRUE
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CONFIGURE_COMMAND
        COMMAND "${CMAKE_COMMAND}" -E echo "taosws-rs no need cmake to config"
        BUILD_COMMAND
        COMMAND "${CMAKE_COMMAND}" -E env "TD_VERSION=${TD_VER_NUMBER}" cargo build --release --locked -p taos-ws-sys --features rustls
        INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/${ext_taosws_dll} ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/${ext_taosws_dll}
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/${ext_taosws_lib_from} ${CMAKE_ARCHIVE_OUTPUT_DIRECTORY}/${ext_taosws_lib_to}
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different target/release/taosws.h ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/../include/taosws.h
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_taosws)
endif()
