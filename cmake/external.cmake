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

include(ExternalProject)

add_custom_target(build_externals)

# eg.: INIT_EXT(ext_zlib)
# initialization all variables to be used by external project and those relied on
macro(INIT_EXT name)               # {
    set(_base            "${TD_EXTERNALS_BASE_DIR}/build/${name}")                      # where all source and build stuffs locate
    set(_ins             "${TD_EXTERNALS_BASE_DIR}/install/${name}/${TD_CONFIG_NAME}")  # where all installed stuffs locate
    set(${name}_base     "${_base}")
    set(${name}_source   "${_base}/src/${name}")
    set(${name}_build    "${_base}/src/${name}-build")
    set(${name}_install  "${_ins}")
    set(${name}_inc_dir  "")
    set(${name}_libs     "")
    set(${name}_byproducts "")
    set(_subclause       "")
    foreach(v ${ARGN})               # {
        if(    "${v}" STREQUAL                   "INC_DIR")
            set(_subclause                       "INC_DIR")        # target_include_directories
        elseif("${v}" STREQUAL                   "LIB_DIR")
            set(_subclause                       "LIB_DIR")        # target_link_directories
        elseif("${v}" STREQUAL                   "LIB")
            set(_subclause                       "LIB")            # target_link_libraries with full-path-lib
        elseif("${v}" STREQUAL                   "BYPRODUCTS")
            set(_subclause                       "BYPRODUCTS")
        elseif("${_subclause}x" STREQUAL "x")
            message(FATAL_ERROR     "expecting keywords either INC_DIR or LIB_DIR or LIB or BYPRODUCTS")
        else()
            if(    "${_subclause}" STREQUAL      "INC_DIR")
                list(APPEND ${name}_inc_dir      "${_ins}/${v}")
            elseif("${_subclause}" STREQUAL      "LIB_DIR")
                list(APPEND ${name}_libs         "${_ins}/${v}")
            elseif("${_subclause}" STREQUAL      "LIB")
                list(APPEND ${name}_libs         "${_ins}/${v}")
            elseif("${_subclause}" STREQUAL      "BYPRODUCTS")
                list(APPEND ${name}_byproducts   "${_ins}/${v}")
            else()
                message(FATAL_ERROR     "internal error")
            endif()
        endif()
    endforeach()                     # }
    if(NOT TD_EXTERNALS_USE_ONLY)
        add_library(${name}_imp STATIC IMPORTED)
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
        foreach(v ${${name}_inc_dir})
            target_include_directories(${tgt} PUBLIC "${v}")
        endforeach()
        if(NOT TD_EXTERNALS_USE_ONLY)     # {
            foreach(v ${${name}_libs})
                set_target_properties(${name}_imp PROPERTIES
                    IMPORTED_LOCATION "${v}"
                )
            endforeach()
            foreach(v ${${name}_byproducts})
                set_target_properties(${name}_imp PROPERTIES
                    IMPORTED_LOCATION "${v}"
                )
            endforeach()
            add_dependencies(${tgt} ${name})
        endif()                           # }
        add_definitions(-D_${name})
        if("z${name}" STREQUAL "zext_gtest")
            target_compile_features(${tgt} PUBLIC cxx_std_11)
            find_package(Threads REQUIRED)
            target_link_libraries(${tgt} PRIVATE Threads::Threads)
        endif()
    endmacro()                               # }
    macro(DEP_${name}_LIB tgt)               # {
        if(NOT TD_EXTERNALS_USE_ONLY)     # {
            foreach(v ${${name}_libs})
                set_target_properties(${name}_imp PROPERTIES
                    IMPORTED_LOCATION "${v}"
                )
            endforeach()
            foreach(v ${${name}_byproducts})
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
        add_definitions(-D_${name})
    endmacro()                               # }
endmacro()                         # }

# get_from_local_repo_if_exists/get_from_local_if_exists
# is for local storage of externals only
macro(get_from_local_repo_if_exists git_url)              # {
  # if LOCAL_REPO is set as: -DLOCAL_REPO:STRING=ssh://host/path-to-local-repo
  # then _git_url would be: ssh://host/path-to-local-repo/<git_url-name>.git
  if(NOT DEFINED LOCAL_REPO)
    set(_git_url "${git_url}")
  else()
    string(FIND ${git_url} "/" _pos REVERSE)
    string(SUBSTRING ${git_url} ${_pos} -1 _name)
    set(_git_url "${LOCAL_REPO}/${_name}")
  endif()
endmacro()                                                # }

macro(get_from_local_if_exists url)                       # {
  if(NOT DEFINED LOCAL_URL)
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
    set(ext_zlib_static zs$<$<STREQUAL:${TD_CONFIG_NAME},Debug>:d>.lib)
endif()
INIT_EXT(ext_zlib
    INC_DIR          include
    LIB              lib/${ext_zlib_static}
)
# freemine: original from taosdata-contrib
# GIT_REPOSITORY https://github.com/taosdata-contrib/zlib.git
# GIT_TAG        v1.2.11
get_from_local_repo_if_exists("https://github.com/madler/zlib.git")
ExternalProject_Add(ext_zlib
    GIT_REPOSITORY ${_git_url}
    GIT_TAG 5a82f71ed1dfc0bec044d9702463dbdf84ea3b71
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}        # if main project is built in Debug, ext_zlib is too
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}                # let default INSTALL step use
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON            # linking consistent
    CMAKE_ARGS -DZLIB_BUILD_SHARED:BOOL=OFF
    CMAKE_ARGS -DZLIB_BUILD_TESTING:BOOL=ON
    GIT_SHALLOW TRUE
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
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=ON         # freemine: building dll or not
        CMAKE_ARGS "-DCMAKE_C_FLAGS:STRING=/wd4244"
        CMAKE_ARGS "-DCMAKE_CXX_FLAGS:STRING=/wd4244"
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${_ins}/bin/${ext_pthread_dll} ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${ext_pthread_dll}
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_pthread)     # this is for github workflow in cache-miss step.
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
        GIT_SHALLOW TRUE
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
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND nmake /f NMakefile all test test2 test3
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${ext_msvcregex_source}/regex.h" "${_ins}/include/regex.h"
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${ext_msvcregex_source}/${ext_msvcregex_static}" "${_ins}/lib/${ext_msvcregex_static}"
        GIT_SHALLOW TRUE
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
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${TD_SUPPORT_DIR}/wcwidth.cmake" "${ext_wcwidth_source}/CMakeLists.txt"
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        GIT_SHALLOW TRUE
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
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        GIT_SHALLOW TRUE
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
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -Dgtest_force_shared_crt:BOOL=ON
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_gtest)     # this is for github workflow in cache-miss step.
    include_directories(${_ins}/include)            # freemine: a better and specific way?
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
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${ext_cppstub_source}/src/stub.h ${_ins}/include/stub.h
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${ext_cppstub_source}/${_platform_dir}/addr_any.h ${_ins}/include/addr_any.h
        # freemine: TODO: seems only .h files are exported
        GIT_SHALLOW TRUE
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
)
# GIT_REPOSITORY https://github.com/taosdata-contrib/lz4.git
# GIT_TAG v1.9.3
get_from_local_repo_if_exists("https://github.com/lz4/lz4.git")
ExternalProject_Add(ext_lz4
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v1.10.0
    PREFIX "${_base}"
    SOURCE_SUBDIR build/cmake
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DBUILD_STATIC_LIBS:BOOL=ON
    BUILD_COMMAND
        COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
    INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
    GIT_SHALLOW TRUE
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
    INC_DIR          include/cjson           # freemine: tweak in this way to hack #include <cJSON.h> in source codes
    LIB              lib/${ext_cjson_static}
)
# GIT_REPOSITORY https://github.com/taosdata-contrib/cJSON.git
# GIT_TAG v1.7.15
get_from_local_repo_if_exists("https://github.com/DaveGamble/cJSON.git")
ExternalProject_Add(ext_cjson
    GIT_REPOSITORY ${_git_url}
    GIT_TAG 12c4bf1986c288950a3d06da757109a6aa1ece38
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DCJSON_BUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DENABLE_HIDDEN_SYMBOLS:BOOL=ON
    CMAKE_ARGS -DENABLE_PUBLIC_SYMBOLS:BOOL=OFF
    BUILD_COMMAND
        COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
    INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
    GIT_SHALLOW TRUE
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
    set(ext_xz_static liblzma.lib)
endif()
INIT_EXT(ext_xz
    INC_DIR          include
    LIB              lib/${ext_xz_static}
)
# GIT_REPOSITORY https://github.com/xz-mirror/xz.git
# GIT_TAG v5.4.4
get_from_local_repo_if_exists("https://github.com/xz-mirror/xz.git")
ExternalProject_Add(ext_xz
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v5.4.4
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    BUILD_COMMAND
        COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
    INSTALL_COMMAND
        COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_xz)     # this is for github workflow in cache-miss step.

# xxHash
# freemine: ref from lzma2::xxhash.h: `https://github.com/Cyan4973/xxHash`
# freemine: TODO: external-symbols (eg. XXH64_createState ...) exist both in libxxhash.a and libfast-lzma2.a
#                 static linking problem?
#                 currently, always call DEP_ext_... in such order, for the same target:
#                 DEP_ext_xxhash(...)
#                 DEP_ext_lzma2(...)
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
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/xxhash.Makefile Makefile
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND make DESTDIR=${_ins}
        INSTALL_COMMAND
            COMMAND make DESTDIR=${_ins} install
        GIT_SHALLOW TRUE
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
        PREFIX "${_base}"
        SOURCE_SUBDIR cmake_unofficial
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        GIT_SHALLOW TRUE
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
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/lzma2.Makefile Makefile
            # freemine: xxhash.h is now introduced by ext_xxhash
        CONFIGURE_COMMAND ""
        BUILD_COMMAND
            COMMAND make DESTDIR=${_ins}
        INSTALL_COMMAND
            COMMAND make DESTDIR=${_ins} install
        GIT_SHALLOW TRUE
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
    )
    # GIT_REPOSITORY https://github.com/libuv/libuv.git
    # GIT_TAG v1.49.2
    get_from_local_repo_if_exists("https://github.com/libuv/libuv.git")
    ExternalProject_Add(ext_libuv
        GIT_REPOSITORY ${_git_url}
        GIT_TAG v1.50.0
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        CMAKE_ARGS -DLIBUV_BUILD_SHARED:BOOL=OFF
        CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        GIT_SHALLOW TRUE
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
        set(_c_flags_list -fPIC -DHAVE_GETTEXT=0) # freemine: TODO: brew install gettext?
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
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/tz.Makefile Makefile
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
            # COMMAND make CFLAGS+=-fPIC CFLAGS+=-g TZDIR=${TZ_OUTPUT_PATH} clean libtz.a
            COMMAND "${CMAKE_COMMAND}" -E echo "-=${_c_flags}=-"
            COMMAND make "CFLAGS=${_c_flags}" DESTDIR=${_ins}
        INSTALL_COMMAND
            COMMAND make "CFLAGS=${_c_flags}" DESTDIR=${_ins} install
        GIT_SHALLOW TRUE
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
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND ./autogen.sh
        CONFIGURE_COMMAND
            COMMAND ./configure -prefix=${_ins} --disable-initial-exec-tls     # freemine: why disable-initial-exec-tls
                    CFLAGS=-Wno-missing-braces
                    CXXFLAGS=-Wno-missing-braces
        BUILD_COMMAND
            COMMAND make
        INSTALL_COMMAND
            COMMAND make install
        # freemine: TODO: always refreshed!!!
        GIT_SHALLOW TRUE
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
    )
    # GIT_REPOSITORY https://github.com/sqlite/sqlite.git
    # GIT_TAG version-3.36.0
    get_from_local_repo_if_exists("https://github.com/sqlite/sqlite.git")
    ExternalProject_Add(ext_sqlite
        GIT_REPOSITORY ${_git_url}
        GIT_TAG version-3.36.0
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        # freemine: TODO: seems no use at all
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
        GIT_PROGRESS TRUE
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
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/crashdump.cmake CMakeLists.txt
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_SUPPORT_DIR}/crasher.c.in crasher/crasher.c
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_crashdump)     # this is for github workflow in cache-miss step.
endif(${BUILD_CRASHDUMP})   # }

# ssl
if(NOT ${TD_WINDOWS})       # {
    # freemine: why at this moment???
    # file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.2/)
    if(${TD_LINUX})
        set(ext_ssl_static libssl.a)
        set(ext_crypto_static libcrypto.a)
    elseif(${TD_DARWIN})
        set(ext_ssl_static libssl.a)
        set(ext_crypto_static libcrypto.a)
    endif()
    if(${TD_LINUX})
      set(_lib lib64)
    else()
      set(_lib lib)
    endif()
    INIT_EXT(ext_ssl
        INC_DIR          include
        LIB              ${_lib}/${ext_ssl_static}
                         ${_lib}/${ext_crypto_static}
    )
    # URL https://github.com/openssl/openssl/releases/download/openssl-3.1.3/openssl-3.1.3.tar.gz
    # URL_HASH SHA256=f0316a2ebd89e7f2352976445458689f80302093788c466692fb2a188b2eacf6
    get_from_local_if_exists("https://github.com/openssl/openssl/releases/download/openssl-3.1.3/openssl-3.1.3.tar.gz")
    ExternalProject_Add(ext_ssl
        URL ${_url}
        URL_HASH SHA256=f0316a2ebd89e7f2352976445458689f80302093788c466692fb2a188b2eacf6
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CONFIGURE_COMMAND
            # COMMAND ./Configure --prefix=$ENV{HOME}/.cos-local.2 no-shared
            COMMAND ./Configure --prefix=${_ins} no-shared
        BUILD_COMMAND
            COMMAND make -j4
        INSTALL_COMMAND
            COMMAND make install_sw -j4
        GIT_SHALLOW TRUE
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
    )
    # URL https://github.com/curl/curl/releases/download/curl-8_2_1/curl-8.2.1.tar.gz
    # URL_HASH MD5=b25588a43556068be05e1624e0e74d41
    get_from_local_if_exists("https://github.com/curl/curl/releases/download/curl-8_2_1/curl-8.2.1.tar.gz")
    ExternalProject_Add(ext_curl
        URL ${_url}
        URL_HASH MD5=b25588a43556068be05e1624e0e74d41
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
        GIT_SHALLOW TRUE
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
    )
    # GIT_REPOSITORY https://github.com/libgeos/geos.git
    # GIT_TAG 3.12.0
    get_from_local_repo_if_exists("https://github.com/libgeos/geos.git")
    ExternalProject_Add(ext_geos
        GIT_REPOSITORY ${_git_url}
        GIT_TAG c1a3d838ced34c29f2d4ba9982dbde31f79b2a05
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
        CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        GIT_SHALLOW TRUE
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
        GIT_SHALLOW TRUE
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
        DEPENDS ext_dwarf
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DDWARF_BASE_DIR:STRING=${ext_dwarf_install}
        CMAKE_ARGS -DZLIB_BASE_DIR:STRING=${ext_zlib_install}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${TD_SUPPORT_DIR}/addr2line.cmake" "${ext_addr2line_source}/CMakeLists.txt"
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_addr2line)     # this is for github workflow in cache-miss step.
endif(${BUILD_ADDR2LINE})   # }

# pcre2
if(${BUILD_PCRE2})          # {
    # freemine: seems no necessary cause strict rules has been enforced by geos
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
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${TD_CONFIG_NAME}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DPCRE2_BUILD_TESTS:BOOL=OFF
        CMAKE_ARGS -DPCRE2_STATIC_PIC:BOOL=OFF
        CMAKE_ARGS -DPCRE2_SHOW_REPORT:BOOL=OFF
        # freemine: turns off because of dynamic linking
        CMAKE_ARGS -DPCRE2_SUPPORT_LIBZ:BOOL=OFF
        CMAKE_ARGS -DPCRE2_SUPPORT_LIBBZ2:BOOL=OFF
        CMAKE_ARGS -DPCRE2_SUPPORT_LIBREADLINE:BOOL=OFF
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config "${TD_CONFIG_NAME}"
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config "${TD_CONFIG_NAME}" --prefix "${_ins}"
        GIT_SHALLOW TRUE
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
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_rocksdb)     # this is for github workflow in cache-miss step.
endif()                                          # }
