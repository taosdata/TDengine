include(ExternalProject)

macro(INIT_EXT name)
    set(_base            "${CMAKE_SOURCE_DIR}/.externals/${name}")
    set(_ins             "${CMAKE_SOURCE_DIR}/.externals/${name}/install")
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
    add_library(${name}_imp STATIC IMPORTED)
    macro(DEP_${name} tgt)
        foreach(v ${${name}_inc_dir})
            target_include_directories(${tgt} PUBLIC "${v}")
        endforeach()
        foreach(v ${${name}_libs})
            set_target_properties(${name}_imp PROPERTIES
                IMPORTED_LOCATION "${v}"
            )
            target_link_libraries(${tgt} PUBLIC "${v}")
        endforeach()
        foreach(v ${${name}_byproducts})
            set_target_properties(${name}_imp PROPERTIES
                IMPORTED_LOCATION "${v}"
            )
        endforeach()
        add_dependencies(${tgt} ${name})
    endmacro()
endmacro()

# cjson
INIT_EXT(ext_cjson
    INC_DIR         include/cjson
    LIB             lib/libcjson.a
)
ExternalProject_Add(ext_cjson
    GIT_REPOSITORY https://github.com/taosdata-contrib/cJSON.git
    GIT_TAG v1.7.15
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DCJSON_BUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DCJSON_OVERRIDE_BUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel
    CMAKE_ARGS -DENABLE_CJSON_TEST:BOOL=OFF
    CMAKE_ARGS -DENABLE_TARGET_EXPORT:BOOL=OFF
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)

# lz4
INIT_EXT(ext_lz4
    INC_DIR          include
    LIB              lib/liblz4.a
)
ExternalProject_Add(ext_lz4
    GIT_REPOSITORY https://github.com/taosdata-contrib/lz4.git
    GIT_TAG v1.9.3
    SOURCE_SUBDIR build/cmake
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
    CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
    CMAKE_ARGS -DBUILD_STATIC_LIBS:BOOL=ON
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel
    CMAKE_ARGS -DLZ4_BUILD_CLI:BOOL=OFF
    CMAKE_ARGS -DLZ4_BUILD_LEGACY_LZ4C:BOOL=OFF
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)

# zlib
INIT_EXT(ext_zlib
    INC_DIR          include
    LIB              lib/libz.a
)
ExternalProject_Add(ext_zlib
    GIT_REPOSITORY https://github.com/taosdata-contrib/zlib.git
    GIT_TAG v1.2.11
    PREFIX "${_base}"
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
    CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel
    CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
    # CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
    # CMAKE_ARGS -DLIBUV_BUILD_BENCH:BOOL=OFF
    # CMAKE_ARGS -DLIBUV_BUILD_SHARED:BOOL=OFF
    # CMAKE_ARGS -DLIBUV_BUILD_TESTS:BOOL=OFF
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)

# rocksdb
if(${BUILD_WITH_ROCKSDB})              # {
    if(${BUILD_CONTRIB})               # {
        INIT_EXT(ext_rocksdb
            INC_DIR            include
            LIB                lib/librocksdb.a
        )
        ExternalProject_Add(ext_rocksdb
            URL https://github.com/facebook/rocksdb/archive/refs/tags/v8.1.1.tar.gz
            URL_HASH MD5=3b4c97ee45df9c8a5517308d31ab008b
            PREFIX "${_base}"
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
            CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel
            CMAKE_ARGS -DWITH_FALLOCATE:BOOL=OFF
            CMAKE_ARGS -DWITH_JEMALLOC:BOOL=OFF
            CMAKE_ARGS -DWITH_GFLAGS:BOOL=OFF
            # CMAKE_ARGS -DPORTABLE:BOOL=ON         # freemine: to check later
            CMAKE_ARGS -DWITH_LIBURING:BOOL=OFF
            # CMAKE_ARGS -DFAIL_ON_WARNINGS:BOOL=ON
            CMAKE_ARGS -DWITH_TESTS:BOOL=OFF
            CMAKE_ARGS -DWITH_BENCHMARK_TOOLS:BOOL=OFF
            CMAKE_ARGS -DWITH_TOOLS:BOOL=OFF
            CMAKE_ARGS -DWITH_LIBURING:BOOL=OFF
            CMAKE_ARGS -DROCKSDB_BUILD_SHARED:BOOL=OFF
            # CMAKE_ARGS -DWITH_ALL_TESTS:BOOL=OFF
            CMAKE_ARGS -DWITH_BENCHMARK_TOOLS:BOOL=OFF
            CMAKE_ARGS -DWITH_CORE_TOOLS:BOOL=OFF
            # CMAKE_ARGS -DWITH_RUNTIME_DEBUG:BOOL=OFF
            CMAKE_ARGS -DWITH_TOOLS:BOOL=OFF
            CMAKE_ARGS -DWITH_TRACE_TOOLS:BOOL=OFF
            # CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
            # // force building with AVX, even when PORTABLE=ON
            # FORCE_AVX:BOOL=OFF
            # // force building with AVX2, even when PORTABLE=ON
            # FORCE_AVX2:BOOL=OFF
            # // force building with SSE4.2, even when PORTABLE=ON
            # FORCE_SSE42:BOOL=OFF
            GIT_SHALLOW TRUE
            EXCLUDE_FROM_ALL TRUE
            VERBATIM
        )
    endif()                            # }{
endif()                                # }

# libuv
if(${BUILD_WITH_UV})           # {
    INIT_EXT(ext_libuv
        INC_DIR            include
        LIB                lib/libuv.a
    )
    ExternalProject_Add(ext_libuv
        GIT_REPOSITORY https://github.com/libuv/libuv.git
        GIT_TAG v1.49.2
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
        CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel
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
if(${BUILD_GEOS})
    INIT_EXT(ext_geos
        INC_DIR           include
        LIB               lib/libgeos_c.a
                          lib/libgeos.a
    )
    ExternalProject_Add(ext_geos
        GIT_REPOSITORY https://github.com/libgeos/geos.git
        GIT_TAG 3.12.0
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
        CMAKE_ARGS -DBUILD_GEOSOP:BOOL=OFF
        CMAKE_ARGS -DBUILD_SHARED_LIBS:BOOL=OFF
        CMAKE_ARGS -DBUILD_TESTING:BOOL=OFF
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel
        CMAKE_ARGS -DGEOS_BUILD_DEVELOPER:BOOL=OFF
        CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif(${BUILD_GEOS})


# pcre2
if(${BUILD_PCRE2})
    INIT_EXT(ext_pcre2
        INC_DIR                include
        LIB                    lib/libpcre2-8.a
    )
    ExternalProject_Add(ext_pcre2
        GIT_REPOSITORY https://github.com/PCRE2Project/pcre2.git
        GIT_TAG pcre2-10.43
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel
        CMAKE_ARGS -DPCRE2_BUILD_TESTS:BOOL=OFF
        # CMAKE_ARGS -DPCRE2_SUPPORT_LIBBZ2:BOOL=ON
        # CMAKE_ARGS -DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON
        # BUILD_COMMAND "${CMAKE_COMMAND}" -E false
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
endif(${BUILD_PCRE2})


# lzma2
INIT_EXT(ext_lzma2
    INC_DIR            usr/local/include
    LIB                usr/local/lib/libfast-lzma2.a
)
ExternalProject_Add(ext_lzma2
    GIT_REPOSITORY https://github.com/conor42/fast-lzma2.git
    PREFIX "${_base}"
    BUILD_IN_SOURCE TRUE
    PATCH_COMMAND
        COMMAND git restore -- Makefile
        COMMAND git apply ${CMAKE_SOURCE_DIR}/contrib/lzma2.diff
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    GIT_SHALLOW TRUE
    EXCLUDE_FROM_ALL TRUE
    VERBATIM
)
add_custom_command(
    OUTPUT ${_ins}/usr/local/include/fast-lzma2.h
           ${_ins}/usr/local/include/fl2_errors.h
           ${_ins}/usr/local/include/xxhash.h
           ${_ins}/usr/local/lib/libfast-lzma2.a
    WORKING_DIRECTORY ${_base}/src/ext_lzma2
    COMMAND make DESTDIR=${_ins}
    COMMAND make DESTDIR=${_ins} install
    COMMAND "${CMAKE_COMMAND}" -E copy_if_different xxhash.h ${_ins}/usr/local/include/
    VERBATIM
)
add_custom_target(
    ext_lzma2_builder
    DEPENDS ext_lzma2
           ${_ins}/usr/local/include/fast-lzma2.h
           ${_ins}/usr/local/include/fl2_errors.h
           ${_ins}/usr/local/include/xxhash.h
           ${_ins}/usr/local/lib/libfast-lzma2.a
)

# tz
if(NOT ${TD_WINDOWS})
    INIT_EXT(ext_tz
        INC_DIR           usr/include
        LIB               usr/lib/libtz.a
    )
    ExternalProject_Add(ext_tz
        GIT_REPOSITORY https://github.com/eggert/tz.git
        GIT_TAG main       # freemine: or fixed?
        PREFIX "${_base}"
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )
    add_custom_command(
        OUTPUT ${_ins}/usr/lib/libtz.a
        WORKING_DIRECTORY ${_base}/src/ext_tz
        COMMAND make DESTDIR=${_ins} CFLAGS=-fPIC
        COMMAND make DESTDIR=${_ins} install
        VERBATIM
    )
    add_custom_target(
        ext_tz_builder
        DEPENDS ext_tz
               ${_ins}/usr/lib/libtz.a
    )
endif(NOT ${TD_WINDOWS})

# googletest or gtest in short
if(${BUILD_TEST})
    INIT_EXT(ext_gtest
        INC_DIR                   include
        LIB                       lib/libgtest.a
                                  lib/libgtest_main.a
    )
    ExternalProject_Add(ext_gtest
        GIT_REPOSITORY https://github.com/taosdata-contrib/googletest.git
        GIT_TAG release-1.11.0
        PREFIX "${_base}"
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
        CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel
        GIT_SHALLOW TRUE
        EXCLUDE_FROM_ALL TRUE
        VERBATIM
    )

    INIT_EXT(ext_stub)
    ExternalProject_Add(ext_stub
        GIT_REPOSITORY https://github.com/coolxv/cpp-stub.git
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
endif()

