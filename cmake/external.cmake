option(DEPEND_DIRECTLY "depend externals directly, otherwise externals will not be built each time to save building time"    ON)

include(ExternalProject)

add_custom_target(build_externals)

# eg.: cmake -B debug -DCMAKE_BUILD_TYPE:STRING=Debug
#      TD_CONFIG_NAME will be `Debug`
#   for multi-configuration tools, such as `Visual Studio ...`
#      cmake --build build --config Release
#      TD_CONFIG_NAME will be `Release`
set(TD_CONFIG_NAME "$<IF:$<STREQUAL:z$<CONFIG>,z>,$<IF:$<STREQUAL:z${CMAKE_BUILD_TYPE},z>,Debug,${CMAKE_BUILD_TYPE}>,$<CONFIG>>")

# eg.: INIT_EXT(ext_zlib)
# initialization all variables to be used by external project and those relied on
macro(INIT_EXT name)
    set(_base            "${CMAKE_SOURCE_DIR}/.externals/build/${name}")                      # where all source and build stuffs locate
    set(_ins             "${CMAKE_SOURCE_DIR}/.externals/install/${name}/${TD_CONFIG_NAME}")  # where all installed stuffs locate
    set(${name}_base     "${_base}")
    set(${name}_source   "${_base}/src/${name}")
    set(${name}_build    "${_base}/src/${name}-build")
    set(${name}_install  "${_ins}")
    set(${name}_inc_dir  "")
    set(${name}_libs     "")
    set(${name}_byproducts "")
    set(_subclause       "")
    foreach(v ${ARGN})
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
    endforeach()
    if(DEPEND_DIRECTLY)
        add_library(${name}_imp STATIC IMPORTED)
    endif()
    # eg.: DEP_ext_zlib(tgt)
    #      make tgt depend on ext_zlib, and call target_include_directories/target_link_libraries accordingly
    #      NOTE: currently, full path to the target's artifact is used, such as libz.a
    macro(DEP_${name} tgt)
        cmake_language(CALL DEP_${name}_INC ${tgt})
        cmake_language(CALL DEP_${name}_LIB ${tgt})
    endmacro()
    macro(DEP_${name}_INC tgt)
        foreach(v ${${name}_inc_dir})
            target_include_directories(${tgt} PUBLIC "${v}")
        endforeach()
        foreach(v ${${name}_libs})
            if(DEPEND_DIRECTLY)
                set_target_properties(${name}_imp PROPERTIES
                    IMPORTED_LOCATION "${v}"
                )
            endif()
        endforeach()
        foreach(v ${${name}_byproducts})
            if(DEPEND_DIRECTLY)
                set_target_properties(${name}_imp PROPERTIES
                    IMPORTED_LOCATION "${v}"
                )
            endif()
        endforeach()
        if(DEPEND_DIRECTLY)
            add_dependencies(${tgt} ${name})
        endif()
        add_definitions(-D_${name})
    endmacro()
    macro(DEP_${name}_LIB tgt)
        foreach(v ${${name}_libs})
            if(DEPEND_DIRECTLY)
                set_target_properties(${name}_imp PROPERTIES
                    IMPORTED_LOCATION "${v}"
                )
            endif()
            target_link_libraries(${tgt} PRIVATE "${v}")
        endforeach()
        foreach(v ${${name}_byproducts})
            if(DEPEND_DIRECTLY)
                set_target_properties(${name}_imp PROPERTIES
                    IMPORTED_LOCATION "${v}"
                )
            endif()
        endforeach()
        if(DEPEND_DIRECTLY)
            add_dependencies(${tgt} ${name})
        endif()
        add_definitions(-D_${name})
    endmacro()
endmacro()

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
    set(ext_zlib_static zs$<$<CONFIG:Debug>:d>.lib)
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
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}        # if main project is built in Debug, ext_zlib is too
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
if(${BUILD_PTHREAD})
    if(${TD_WINDOWS})
        set(ext_pthread_static pthreadVC3.lib)
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
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=ON         # freemine: building dll or not
        CMAKE_ARGS "-DCMAKE_C_FLAGS:STRING=/wd4244"
        CMAKE_ARGS "-DCMAKE_CXX_FLAGS:STRING=/wd4244"
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_pthread)     # this is for github workflow in cache-miss step.
endif()

# iconv
if(${BUILD_WITH_ICONV})
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
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DBUILD_SHARED:BOOL=OFF
        CMAKE_ARGS -DBUILD_STATIC:BOOL=ON
        CMAKE_ARGS -DCMAKE_C_FLAGS:STRING=/wd4267
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_iconv)     # this is for github workflow in cache-miss step.
endif()

# msvc regex
if(${BUILD_MSVCREGEX})
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
endif()

# wcwidth
if(${BUILD_WCWIDTH})
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
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${TD_CONTRIB_DIR}/wcwidth.cmake" "${ext_wcwidth_source}/CMakeLists.txt"
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_wcwidth)     # this is for github workflow in cache-miss step.
endif()

# wingetopt
if(${BUILD_WINGETOPT})
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
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_wingetopt)     # this is for github workflow in cache-miss step.
endif()

# googletest
if(${BUILD_TEST}) # freemine: add BUILD_GTEST
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
        LIB              lib/${ext_gtest_static}
                         lib/${ext_gtest_main}
    )
    # GIT_REPOSITORY https://github.com/taosdata-contrib/googletest.git
    # GIT_TAG release-1.11.0
    get_from_local_repo_if_exists("https://github.com/google/googletest.git")
    ExternalProject_Add(ext_gtest
        GIT_REPOSITORY ${_git_url}
        GIT_TAG release-1.12.0
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_dependencies(build_externals ext_gtest)     # this is for github workflow in cache-miss step.
endif(${BUILD_TEST})

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
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DBUILD_STATIC_LIBS:BOOL=ON
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_dependencies(build_externals ext_lz4)     # this is for github workflow in cache-miss step.
