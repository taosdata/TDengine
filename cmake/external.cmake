include(ExternalProject)

option(DEPEND_DIRECTLY "depend externals directly, otherwise externals will not be built each time to save building time"    ON)

macro(INIT_EXT name)
    set(_base            "${CMAKE_SOURCE_DIR}/.externals/${name}")
    set(_ins             "${CMAKE_SOURCE_DIR}/.externals/${name}/$<CONFIG>/install")
    set(${name}_base     "${_base}")
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

macro(get_from_local_repo_if_exists git_url)              # {
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

# cjson
if(${TD_LINUX})
    set(ext_cjson_static libcjson.a)
elseif(${TD_DARWIN})
    set(ext_cjson_static libcjson.a)
elseif(${TD_WINDOWS})
    set(ext_cjson_static cjson.lib)
endif()
INIT_EXT(ext_cjson
    INC_DIR         include/cjson
    LIB             lib/${ext_cjson_static}
)
get_from_local_repo_if_exists("https://github.com/taosdata-contrib/cJSON.git")
ExternalProject_Add(ext_cjson
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v1.7.15
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DCJSON_BUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DCJSON_OVERRIDE_BUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DENABLE_CJSON_TEST:BOOL=OFF
    CMAKE_ARGS -DENABLE_TARGET_EXPORT:BOOL=OFF
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    CMAKE_ARGS -DENABLE_PUBLIC_SYMBOLS:BOOL=OFF
    CMAKE_ARGS -DENABLE_HIDDEN_SYMBOLS:BOOL=ON
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)

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
get_from_local_repo_if_exists("https://github.com/taosdata-contrib/lz4.git")
ExternalProject_Add(ext_lz4
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v1.9.3
    SOURCE_SUBDIR build/cmake
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DBUILD_STATIC_LIBS:BOOL=ON
    CMAKE_ARGS -DLZ4_BUILD_CLI:BOOL=OFF
    CMAKE_ARGS -DLZ4_BUILD_LEGACY_LZ4C:BOOL=OFF
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)

# zlib
if(${TD_LINUX})
    set(ext_zlib_static libz.a)
elseif(${TD_DARWIN})
    set(ext_zlib_static libz.a)
elseif(${TD_WINDOWS})
    set(ext_zlib_static zlibstatic$<$<CONFIG:Debug>:D>.lib)
endif()
INIT_EXT(ext_zlib
    INC_DIR          include
    LIB              lib/${ext_zlib_static}
)
get_from_local_repo_if_exists("https://github.com/taosdata-contrib/zlib.git")
ExternalProject_Add(ext_zlib
    GIT_REPOSITORY ${_git_url}
    GIT_TAG v1.2.11
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    PATCH_COMMAND
        COMMAND git restore -- CMakeLists.txt
        COMMAND git apply ${TD_CONTRIB_DIR}/zlib.diff
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)

# rocksdb
if(${BUILD_WITH_ROCKSDB})              # {
    if(${BUILD_CONTRIB})               # {
        if(${TD_LINUX})
            set(ext_rocksdb_static librocksdb.a)
        elseif(${TD_DARWIN})
            set(ext_rocksdb_static librocksdb.a)
        elseif(${TD_WINDOWS})
            set(ext_rocksdb_static rocksdb.lib)
        endif()
        INIT_EXT(ext_rocksdb
            INC_DIR            include
            LIB                lib/${ext_rocksdb_static}
        )
        get_from_local_if_exists("https://github.com/facebook/rocksdb/archive/refs/tags/v8.1.1.tar.gz")
        ExternalProject_Add(ext_rocksdb
            URL ${_url}
            URL_HASH MD5=3b4c97ee45df9c8a5517308d31ab008b
            BUILD_ALWAYS TRUE
            DOWNLOAD_EXTRACT_TIMESTAMP TRUE
            PREFIX "${_base}"
            CONFIGURE_COMMAND
                COMMAND "${CMAKE_COMMAND}" -E copy ${TD_CONTRIB_DIR}/rocksdb.cmake CMakeLists.txt
                COMMAND "${CMAKE_COMMAND}" -B . -S ../ext_rocksdb -DCMAKE_BUILD_TYPE:STRING=$<CONFIG>
                        -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
                        -DWITH_FALLOCATE:BOOL=OFF
                        -DWITH_JEMALLOC:BOOL=OFF
                        -DWITH_GFLAGS:BOOL=OFF
                        -DWITH_LIBURING:BOOL=OFF
                        -DWITH_TESTS:BOOL=OFF
                        -DWITH_BENCHMARK_TOOLS:BOOL=OFF
                        -DWITH_TOOLS:BOOL=OFF
                        -DWITH_LIBURING:BOOL=OFF
                        -DROCKSDB_BUILD_SHARED:BOOL=OFF
                        -DWITH_BENCHMARK_TOOLS:BOOL=OFF
                        -DWITH_CORE_TOOLS:BOOL=OFF
                        -DWITH_TOOLS:BOOL=OFF
                        -DWITH_TRACE_TOOLS:BOOL=OFF
                        -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
                        -DROCKSDB_INSTALL_ON_WINDOWS:BOOL=ON
            BUILD_COMMAND
                COMMAND "${CMAKE_COMMAND}" --build . --config $<CONFIG>
            INSTALL_COMMAND
                COMMAND "${CMAKE_COMMAND}" --install . --config $<CONFIG> --prefix ${_ins}
            GIT_SHALLOW TRUE
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
    endif()                            # }
endif()                                # }

# libuv
if(${BUILD_WITH_UV})           # {
    if(${TD_LINUX})
        set(ext_libuv_static libuv.a)
    elseif(${TD_DARWIN})
        set(ext_libuv_static libuv.a)
    elseif(${TD_WINDOWS})
        set(ext_libuv_static libuv.lib)
    endif()
    INIT_EXT(ext_libuv
        INC_DIR            include
        LIB                lib/${ext_libuv_static}
    )
    get_from_local_repo_if_exists("https://github.com/libuv/libuv.git")
    ExternalProject_Add(ext_libuv
        GIT_REPOSITORY ${_git_url}
        GIT_TAG v1.49.2
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
        CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
        CMAKE_ARGS -DLIBUV_BUILD_BENCH:BOOL=OFF
        CMAKE_ARGS -DLIBUV_BUILD_SHARED:BOOL=OFF
        CMAKE_ARGS -DLIBUV_BUILD_TESTS:BOOL=OFF
        CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif()                        # }

# geos
if(${BUILD_GEOS})              # {
    if(${TD_LINUX})
        set(ext_geos_c_static libgeos_c.a)
    elseif(${TD_DARWIN})
        set(ext_geos_c_static libgeos_c.a)
    elseif(${TD_WINDOWS})
        set(ext_geos_c_static geos_c.lib)
    endif()
    if(${TD_LINUX})
        set(ext_geos_static libgeos.a)
    elseif(${TD_DARWIN})
        set(ext_geos_static libgeos.a)
    elseif(${TD_WINDOWS})
        set(ext_geos_static geos.lib)
    endif()
    INIT_EXT(ext_geos
        INC_DIR           include
        LIB               lib/${ext_geos_c_static}
                          lib/${ext_geos_static}
    )
    get_from_local_repo_if_exists("https://github.com/libgeos/geos.git")
    ExternalProject_Add(ext_geos
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 3.12.0
        PREFIX "${_base}"
        CONFIGURE_COMMAND
            COMMAND "${CMAKE_COMMAND}" -B . -S ../ext_geos -DCMAKE_BUILD_TYPE:STRING=$<CONFIG>
                    -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
                    -DBUILD_GEOSOP:BOOL=OFF
                    -DBUILD_SHARED_LIBS:BOOL=OFF
                    -DBUILD_TESTING:BOOL=OFF
                    -DGEOS_BUILD_DEVELOPER:BOOL=OFF
                    -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        BUILD_COMMAND
            COMMAND "${CMAKE_COMMAND}" --build . --config $<CONFIG>
        INSTALL_COMMAND
            COMMAND "${CMAKE_COMMAND}" --install . --config $<CONFIG>
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif()                        # }

# pcre2
if(${TD_LINUX})
    set(ext_pcre2_static      libpcre2-8.a)
    set(ext_pcre2posix_static libpcre2-posix.a)
elseif(${TD_DARWIN})
    set(ext_pcre2_static      libpcre2-8.a)
    set(ext_pcre2posix_static libpcre2-posix.a)
elseif(${TD_WINDOWS})
    set(ext_pcre2_static      pcre2-8-static$<$<CONFIG:Debug>:D>.lib)
    set(ext_pcre2posix_static pcre2-posix-static$<$<CONFIG:Debug>:D>.lib)
endif()
if(${BUILD_PCRE2})           # {
    INIT_EXT(ext_pcre2
        INC_DIR                include
        LIB                    lib/${ext_pcre2posix_static}
                               lib/${ext_pcre2_static}
    )
    get_from_local_repo_if_exists("https://github.com/PCRE2Project/pcre2.git")
    ExternalProject_Add(ext_pcre2
        GIT_REPOSITORY ${_git_url}
        GIT_TAG pcre2-10.43
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
        CMAKE_ARGS -DPCRE2_BUILD_TESTS:BOOL=OFF
        CMAKE_ARGS -DPCRE2_STATIC_PIC:BOOL=ON
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif()                      # }

# lzma2
if(NOT ${TD_WINDOWS})         # {
    if(NOT ${TD_DARWIN})    # {
        set(ext_lzma2_static libfast-lzma2.a)
        INIT_EXT(ext_lzma2
            INC_DIR            usr/local/include
            LIB                usr/local/lib/${ext_lzma2_static}
        )
        get_from_local_repo_if_exists("https://github.com/conor42/fast-lzma2.git")
        ExternalProject_Add(ext_lzma2
            GIT_REPOSITORY ${_git_url}
            GIT_TAG ded964d203cabe1a572d2c813c55e8a94b4eda48
            PREFIX "${_base}"
            BUILD_IN_SOURCE TRUE
            PATCH_COMMAND
                COMMAND git restore -- Makefile
                COMMAND git apply ${CMAKE_SOURCE_DIR}/contrib/lzma2.diff
            CONFIGURE_COMMAND ""
            BUILD_COMMAND
                COMMAND make DESTDIR=${_ins}
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different fast-lzma2.h ${_ins}/usr/local/include/fast-lzma2.h
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different fl2_errors.h ${_ins}/usr/local/include/fl2_errors.h
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different xxhash.h ${_ins}/usr/local/include/xxhash.h
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different libfast-lzma2.a ${_ins}/usr/local/lib/libfast-lzma2.a
            INSTALL_COMMAND ""
            GIT_SHALLOW TRUE
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
    else()                  # }{
        set(ext_lzma2_static libfast-lzma2.a)
        INIT_EXT(ext_lzma2
            INC_DIR            include
            LIB                lib/${ext_lzma2_static}
        )
        get_from_local_repo_if_exists("https://github.com/conor42/fast-lzma2.git")
        ExternalProject_Add(ext_lzma2
            GIT_REPOSITORY ${_git_url}
            GIT_TAG ded964d203cabe1a572d2c813c55e8a94b4eda48
            PREFIX "${_base}"
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
            PATCH_COMMAND
                COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${CMAKE_SOURCE_DIR}/contrib/lzma2_darwin.CMakeLists.txt.cmake" ${_base}/src/ext_lzma2/CMakeLists.txt
            GIT_SHALLOW TRUE
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
    endif()                 # }
else()                        # }{
    if(${TD_WINDOWS})
        set(ext_lzma2_static fast-lzma2.lib)
    endif()
    INIT_EXT(ext_lzma2
        INC_DIR            include
        LIB                lib/${ext_lzma2_static}
    )
    get_from_local_repo_if_exists("https://github.com/conor42/fast-lzma2.git")
    ExternalProject_Add(ext_lzma2
        GIT_REPOSITORY ${_git_url}
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
        PATCH_COMMAND COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_CONTRIB_DIR}/lzma2.cmake ${_base}/src/ext_lzma2/CMakeLists.txt
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif()                       # }

# tz
if(NOT ${TD_WINDOWS})              # {
    if(${TD_LINUX})
        set(ext_tz_static libtz.a)
    elseif(${TD_DARWIN})
        set(ext_tz_static libtz.a)
    elseif(${TD_WINDOWS})
        set(ext_tz_static tz.lib)
    endif()
    INIT_EXT(ext_tz
        INC_DIR           usr/include
        LIB               usr/lib/${ext_tz_static}
    )
    get_from_local_repo_if_exists("https://github.com/eggert/tz.git")
    ExternalProject_Add(ext_tz
        GIT_REPOSITORY ${_git_url}
        GIT_TAG main       # freemine: or fixed?
        PREFIX "${_base}"
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    set(_cflags "-fPIC")
    if(${TD_DARWIN})         # {
      set(_cflags "${_cflags} -DHAVE_GETTEXT=0")
    endif()                  # }
    add_custom_command(
        OUTPUT ${_ins}/usr/lib/${ext_tz_static}
        WORKING_DIRECTORY ${_base}/src/ext_tz
        COMMAND make DESTDIR=${_ins} CFLAGS=${_cflags}
        COMMAND make DESTDIR=${_ins} CFLAGS=${_cflags} install
        VERBATIM
    )
    add_custom_target(
        ext_tz_builder
        DEPENDS ext_tz
               ${_ins}/usr/lib/${ext_tz_static}
    )
endif()                            # }

# googletest or gtest in short
if(${BUILD_TEST})              # {
    if(${TD_LINUX})
        set(ext_gtest_static libgtest.a)
    elseif(${TD_DARWIN})
        set(ext_gtest_static libgtest.a)
    elseif(${TD_WINDOWS})
        set(ext_gtest_static gtest$<$<CONFIG:Debug>:D>.lib)
    endif()
    if(${TD_LINUX})
        set(ext_gtest_main_static libgtest_main.a)
    elseif(${TD_DARWIN})
        set(ext_gtest_main_static libgtest_main.a)
    elseif(${TD_WINDOWS})
        set(ext_gtest_main_static gtest_main$<$<CONFIG:Debug>:D>.lib)
    endif()
    INIT_EXT(ext_gtest
        INC_DIR                   include
        LIB                       lib/${ext_gtest_main_static}
                                  lib/${ext_gtest_static}
    )
    get_from_local_repo_if_exists("https://github.com/taosdata-contrib/googletest.git")
    ExternalProject_Add(ext_gtest
        GIT_REPOSITORY ${_git_url}
        GIT_TAG release-1.11.0
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
        CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=OFF
        CMAKE_ARGS -Dgtest_force_shared_crt:BOOL=ON
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )

    INIT_EXT(ext_stub)
    get_from_local_repo_if_exists("https://github.com/coolxv/cpp-stub.git")
    ExternalProject_Add(ext_stub
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 3137465194014d66a8402941e80d2bccc6346f51
        # GIT_SUBMODULES "src"
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    if(${TD_LINUX})          # {
        set(ext_stub_addr_any ${_base}/src/ext_stub/src_linux)
    elseif(${TD_DARWIN})     # }{
        set(ext_stub_addr_any ${_base}/src/ext_stub/src_darwin)
    elseif(${TD_WINDOWS})    # }{
        set(ext_stub_addr_any ${_base}/src/ext_stub/src_win)
    endif()                  # }
    set(ext_stub_stub ${_base}/src/ext_stub/src)
endif()                        # }

# pthread
if(${BUILD_PTHREAD})     # {
    if(${TD_WINDOWS})        # {
        if(${TD_WINDOWS})
            set(ext_pthread_static     pthreadVC3$<$<CONFIG:Debug>:D>.lib)
            set(ext_pthread_dllname    pthreadVC3$<$<CONFIG:Debug>:D>.dll)
        endif()
        INIT_EXT(ext_pthread
            INC_DIR               x86_64/$<CONFIG>/include
            LIB                   x86_64/$<CONFIG>/lib/${ext_pthread_static}
        )
        get_from_local_repo_if_exists("https://github.com/GerHobbelt/pthread-win32")
        ExternalProject_Add(ext_pthread
                GIT_REPOSITORY ${_git_url}
                GIT_TAG v3.0.3.1
                PREFIX "${_base}"
                CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
                CONFIGURE_COMMAND
                    COMMAND "${CMAKE_COMMAND}" -B . -S ../ext_pthread -DCMAKE_BUILD_TYPE:STRING=$<CONFIG> -DCMAKE_INSTALL_PREFIX:STRING=${_ins}
                BUILD_COMMAND
                    COMMAND "${CMAKE_COMMAND}" --build . --config $<CONFIG>
                INSTALL_COMMAND
                    COMMAND "${CMAKE_COMMAND}" --install . --config $<CONFIG>
                    COMMAND "${CMAKE_COMMAND}" -E copy_if_different
                        ${_ins}/x86_64/$<CONFIG>/bin/${ext_pthread_dllname}
                        ${EXECUTABLE_OUTPUT_PATH}/$<CONFIG>/${ext_pthread_dllname}
                GIT_SHALLOW TRUE
                BUILD_ALWAYS TRUE
                EXCLUDE_FROM_ALL TRUE
                VERBATIM
        )
    endif()                  # }
endif()                  # }

# iconv
if(${BUILD_WITH_ICONV})  # {
    if(${TD_WINDOWS})        # {
        if(${TD_WINDOWS})
            set(ext_iconv_static iconv.lib)
        endif()
        INIT_EXT(ext_iconv
            INC_DIR               include
            LIB                   lib/${ext_iconv_static}
        )
        get_from_local_repo_if_exists("https://github.com/win-iconv/win-iconv.git")
        ExternalProject_Add(ext_iconv
                GIT_REPOSITORY ${_git_url}
                GIT_TAG v0.0.8
                PREFIX "${_base}"
                CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
                CMAKE_ARGS -DBUILD_SHARED:BOOL=OFF
                CMAKE_ARGS -DBUILD_STATIC:BOOL=ON
                GIT_SHALLOW TRUE
                EXCLUDE_FROM_ALL TRUE
                VERBATIM
        )
    endif()                  # }
endif()                  # }

# msvc regex
if(${BUILD_MSVCREGEX})       # {
    if(${TD_WINDOWS})        # {
        if(${TD_WINDOWS})
            set(ext_msvcregex_static regex.lib)
        endif()
        INIT_EXT(ext_msvcregex
            INC_DIR               include
            LIB                   lib/${ext_msvcregex_static}
        )
        get_from_local_repo_if_exists("https://gitee.com/l0km/libgnurx-msvc.git")
        ExternalProject_Add(ext_msvcregex
                GIT_REPOSITORY ${_git_url}
                GIT_TAG master
                PREFIX "${_base}"
                CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
                PATCH_COMMAND COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_CONTRIB_DIR}/msvcregex.cmake ${_base}/src/ext_msvcregex/CMakeLists.txt
                GIT_SHALLOW TRUE
                EXCLUDE_FROM_ALL TRUE
                VERBATIM
        )
    endif()                  # }
endif()                      # }

# wcwidth
if(${BUILD_WCWIDTH})         # {
    if(${TD_WINDOWS})        # {
        if(${TD_WINDOWS})
            set(ext_wcwidth_static wcwidth.lib)
        endif()
        INIT_EXT(ext_wcwidth
            LIB                   lib/${ext_wcwidth_static}
            BYPRODUCTS            include/wcwidth.h
        )
        get_from_local_repo_if_exists("https://github.com/fumiyas/wcwidth-cjk.git")
        ExternalProject_Add(ext_wcwidth
                GIT_REPOSITORY ${_git_url}
                GIT_TAG master
                PREFIX "${_base}"
                CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
                PATCH_COMMAND COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_CONTRIB_DIR}/wcwidth.cmake ${_base}/src/ext_wcwidth/CMakeLists.txt
                GIT_SHALLOW TRUE
                EXCLUDE_FROM_ALL TRUE
                VERBATIM
        )
    endif()                  # }
endif()                      # }

# crashdump
if(${BUILD_CRASHDUMP})       # {
    if(${TD_WINDOWS})
        set(ext_crashdump_static crashdump.lib)
    endif()
    INIT_EXT(ext_crashdump
        INC_DIR         include/crashdump
        LIB             lib/${ext_crashdump_static}
    )
    get_from_local_repo_if_exists("https://github.com/Arnavion/crashdump.git")
    ExternalProject_Add(ext_crashdump
        GIT_REPOSITORY ${_git_url}
        GIT_TAG 149b43c10debdf28a2c50d79dee5ff344d83bd06
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
        PATCH_COMMAND
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_CONTRIB_DIR}/crashdump.cmake CMakeLists.txt
            COMMAND "${CMAKE_COMMAND}" -E copy_if_different ${TD_CONTRIB_DIR}/crasher.c.in crasher/crasher.c
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif()                      # }

# wingetopt
if(${BUILD_WINGETOPT})
    if(${TD_WINDOWS})
        set(ext_wingetopt_static wingetopt.lib)
    endif()
    INIT_EXT(ext_wingetopt
        INC_DIR         include
        LIB             lib/${ext_wingetopt_static}
    )
    get_from_local_repo_if_exists("https://github.com/alex85k/wingetopt.git")
    ExternalProject_Add(ext_wingetopt
        GIT_REPOSITORY ${_git_url}
        GIT_TAG master
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif(${BUILD_WINGETOPT})

add_executable(main main.c)
if(${TD_DARWIN})          # {
  target_include_directories(main PRIVATE /usr/local/include)
  target_link_directories(main PRIVATE /usr/local/lib)
endif()                   # }
DEP_ext_cjson(main)
DEP_ext_lz4(main)
DEP_ext_zlib(main)
if(${BUILD_WITH_ROCKSDB})              # {
    if(${BUILD_CONTRIB})               # {
        DEP_ext_rocksdb(main)
        if(${TD_WINDOWS})
            target_link_libraries(main PUBLIC Rpcrt4 Shlwapi)
        else()                         # }{
            target_link_libraries(main PUBLIC m stdc++)
        endif()
    endif()                            # }
endif()                                # }
if(${BUILD_WITH_UV})           # {
    DEP_ext_libuv(main)
    if(${TD_WINDOWS})
        target_link_libraries(main PUBLIC ws2_32 Iphlpapi Userenv Dbghelp)
    endif()
endif()                        # }
if(${BUILD_GEOS})              # {
    DEP_ext_geos(main)
endif()                        # }
DEP_ext_lzma2(main)
if(${BUILD_TEST})              # {
    DEP_ext_gtest(main)
endif()                        # }
if(${BUILD_PTHREAD})     # {
    if(${TD_WINDOWS})        # {
        DEP_ext_pthread(main)
    endif()                  # }
endif()                  # }
if(${BUILD_WITH_ICONV})  # {
    if(${TD_WINDOWS})        # {
        DEP_ext_iconv(main)
    endif()                  # }
endif()                  # }
DEP_ext_pcre2(main)
if(${TD_WINDOWS})        # {
    add_definitions(-DPCRE2_STATIC) # freemine: a better approach?
endif()                  # }
if(${BUILD_MSVCREGEX})       # {
    if(${TD_WINDOWS})        # {
        DEP_ext_msvcregex(main)
    endif()                  # }
endif()                      # }
if(${BUILD_WCWIDTH})         # {
    if(${TD_WINDOWS})        # {
        DEP_ext_wcwidth(main)
    endif()                  # }
endif()                      # }

message(STATUS        "     BUILD_TEST:${BUILD_TEST}")
message(STATUS        "  BUILD_CONTRIB:${BUILD_CONTRIB}")
message(STATUS        "DEPEND_DIRECTLY:${DEPEND_DIRECTLY}")
