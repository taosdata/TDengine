# ================================================================================================
# Download
# ================================================================================================

if(${FALLBACK})          # {
    function(cat IN_FILE OUT_FILE)
        file(READ ${IN_FILE} CONTENTS)
        file(APPEND ${OUT_FILE} "${CONTENTS}")
    endfunction(cat IN_FILE OUT_FILE)
else()                   # }{
    include(ExternalProject)
endif()                  # }

if(${BUILD_WITH_S3})
    file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.2/)

elseif(${BUILD_WITH_COS})
    if(NOT ${FALLBACK})            # {
      message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                        # }
    set(CONTRIB_TMP_FILE3 "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in3")
    configure_file("${TD_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})

    if(${BUILD_WITH_COS})
        file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.1/)
        cat("${TD_SUPPORT_DIR}/mxml_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})
        cat("${TD_SUPPORT_DIR}/apr_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})
        cat("${TD_SUPPORT_DIR}/curl_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})
    endif(${BUILD_WITH_COS})

    configure_file(${CONTRIB_TMP_FILE3} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
    execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
    execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")

    set(CONTRIB_TMP_FILE2 "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in2")
    configure_file("${TD_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE2})

    if(${BUILD_WITH_COS})
        cat("${TD_SUPPORT_DIR}/apr-util_CMakeLists.txt.in" ${CONTRIB_TMP_FILE2})
    endif(${BUILD_WITH_COS})

    configure_file(${CONTRIB_TMP_FILE2} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
    execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
    execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
endif()

if(${FALLBACK})           # {
    set(CONTRIB_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")
    configure_file("${TD_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()                   # }

# taos-tools
if(${BUILD_TOOLS})
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/taostools_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif()

# taosws-rs
if(${WEBSOCKET})
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/taosws_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif()

# taosadapter
if(${BUILD_HTTP})
    MESSAGE("BUILD_HTTP is on")
else()
    MESSAGE("BUILD_HTTP is off, use taosAdapter")
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/taosadapter_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif()

# pthread
if(${BUILD_PTHREAD})
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/pthread_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif()

# iconv
if(${BUILD_WITH_ICONV})
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/iconv_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif()

# jemalloc
if(${JEMALLOC_ENABLED})
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/jemalloc_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif()

# msvc regex
if(${BUILD_MSVCREGEX})
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/msvcregex_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif()

# wcwidth
if(${BUILD_WCWIDTH})
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/wcwidth_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif()

# wingetopt
if(${BUILD_WINGETOPT})
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/wingetopt_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif()

# googletest
if(${BUILD_TEST})
    if(${FALLBACK})         # {
        cat("${TD_SUPPORT_DIR}/gtest_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        cat("${TD_SUPPORT_DIR}/stub_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                 # }
endif(${BUILD_TEST})

# lz4
if(${FALLBACK})             # {
    cat("${TD_SUPPORT_DIR}/lz4_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()                     # }

# zlib
if(${FALLBACK})             # {
    cat("${TD_SUPPORT_DIR}/zlib_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()                     # }

# cJson
if(${FALLBACK})             # {
    cat("${TD_SUPPORT_DIR}/cjson_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()                     # }

if(NOT ${TD_WINDOWS})
    if(${FALLBACK})             # {
        cat("${TD_SUPPORT_DIR}/tz_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                     # }
endif(NOT ${TD_WINDOWS})

# xz
# cat("${TD_SUPPORT_DIR}/xz_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# lzma2
if(${FALLBACK})             # {
    cat("${TD_SUPPORT_DIR}/lzma_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()                     # }

if(${BUILD_CONTRIB})
    if(${BUILD_WITH_ROCKSDB})
        if(${FALLBACK})             # {
            cat("${TD_SUPPORT_DIR}/rocksdb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        endif()                     # }
        add_definitions(-DUSE_ROCKSDB)
        # freemine: git grep -wn USE_ROCKSDB, and nothing found
    endif()
else()
    if(NOT ${TD_LINUX})
        if(${BUILD_WITH_ROCKSDB})
            if(${FALLBACK})             # {
                cat("${TD_SUPPORT_DIR}/rocksdb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
            endif()                     # }
            add_definitions(-DUSE_ROCKSDB)
        endif(${BUILD_WITH_ROCKSDB})
    else()
        if(${BUILD_WITH_ROCKSDB})
            add_definitions(-DUSE_ROCKSDB)
        endif(${BUILD_WITH_ROCKSDB})
    endif()
endif()

# cat("${TD_SUPPORT_DIR}/zstd_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# libuv
if(${BUILD_WITH_UV})
    if(${FALLBACK})             # {
        cat("${TD_SUPPORT_DIR}/libuv_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                     # }
endif(${BUILD_WITH_UV})

# sqlite
if(${BUILD_WITH_SQLITE})
    if(${FALLBACK})             # {
        cat("${TD_SUPPORT_DIR}/sqlite_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                     # }
endif(${BUILD_WITH_SQLITE})

# s3
if(${BUILD_WITH_S3})
    if(${FALLBACK})             # {
        cat("${TD_SUPPORT_DIR}/ssl_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        cat("${TD_SUPPORT_DIR}/xml2_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        cat("${TD_SUPPORT_DIR}/curl_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        cat("${TD_SUPPORT_DIR}/libs3_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        cat("${TD_SUPPORT_DIR}/azure_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    else()                      # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                     # }
    add_definitions(-DUSE_S3)

# cos
elseif(${BUILD_WITH_COS})
    if(${FALLBACK})             # {
        # cat("${TD_SUPPORT_DIR}/mxml_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        # cat("${TD_SUPPORT_DIR}/apr_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        # cat("${TD_SUPPORT_DIR}/apr-util_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        # cat("${TD_SUPPORT_DIR}/curl_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        cat("${TD_SUPPORT_DIR}/cos_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    else()                      # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                     # }
    add_definitions(-DUSE_COS)
endif()

# crashdump
if(${BUILD_CRASHDUMP})
    if(${FALLBACK})             # {
        cat("${TD_SUPPORT_DIR}/crashdump_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    else()                      # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                     # }
endif(${BUILD_CRASHDUMP})

# addr2line
if(${BUILD_ADDR2LINE})
    if(NOT ${TD_WINDOWS})
        if(${FALLBACK})             # {
            cat("${TD_SUPPORT_DIR}/libdwarf_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
            cat("${TD_SUPPORT_DIR}/addr2line_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        else()                      # }{
            message(FATAL_ERROR      "freemine: not refactored yet")
        endif()                     # }
    endif(NOT ${TD_WINDOWS})
endif(${BUILD_ADDR2LINE})

# geos
if(${BUILD_GEOS})
    if(${FALLBACK})             # {
        cat("${TD_SUPPORT_DIR}/geos_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                     # }
endif()

#
if(${BUILD_PCRE2})
    if(${FALLBACK})             # {
        cat("${TD_SUPPORT_DIR}/pcre2_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif()                     # }
endif()

find_program(C_COMPILER_LEMON NAMES gcc)
if(C_COMPILER_LEMON)
    message(STATUS "LEMON C compiler: ${C_COMPILER_LEMON}")
else()
    set(C_COMPILER_LEMON ${CMAKE_C_COMPILER})
    message(STATUS "LEMON C compiler: ${C_COMPILER_LEMON}")
endif()

# lemon
if(${FALLBACK})             # {
    cat("${TD_SUPPORT_DIR}/lemon_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
else()                      # }{
    set(_lemon_bin_dir "${CMAKE_CURRENT_BINARY_DIR}")
    set(_lemon_src_dir "${CMAKE_CURRENT_SOURCE_DIR}/lemon")

    set(_lemon           "${_lemon_bin_dir}/lemon")
    set(_lemon_c         "${_lemon_src_dir}/lemon.c")

    add_custom_command(
        OUTPUT ${_lemon}
        COMMAND ${C_COMPILER_LEMON} -o ${_lemon} ${_lemon_c}
        MAIN_DEPENDENCY ${_lemon_c}
        COMMENT "building lemon on the fly"
        VERBATIM
    )

    add_custom_target(
        lemon
        DEPENDS ${_lemon}
    )

    set(_lempar_c  "${_lemon_src_dir}/lempar.c")
    set(_out_dir   "${CMAKE_CURRENT_BINARY_DIR}")
    set(_sql_c     "${_out_dir}/sql.c")
    set(_sql_h     "${_out_dir}/ttokenauto.h")
    set(_sql_y     "${CMAKE_SOURCE_DIR}/source/libs/parser/inc/sql.y")

    set(taos_sql_dir  "${_out_dir}"     PARENT_SCOPE)
    set(taos_sql_h    "${_sql_h}"       PARENT_SCOPE)
    set(taos_sql_c    "${_sql_c}"       PARENT_SCOPE)

    add_custom_command(
        OUTPUT ${_sql_c} ${_sql_h}
        WORKING_DIRECTORY ${_lemon_bin_dir}
        COMMAND ./lemon -d${_out_dir} -T${_lempar_c} ${_sql_y} &&
                "${CMAKE_COMMAND}" -E echo "lemon complete successfully" ||
                "${CMAKE_COMMAND}" -E echo "lemon complete with conflicts"
        COMMAND "${CMAKE_COMMAND}" -E rename "${_out_dir}/sql.h" ${_sql_h}
        DEPENDS ${_sql_y} ${_lempar_c}
        COMMENT "Generating sql.c using lemon"
        VERBATIM
    )

    add_custom_target(lemon_sql
        DEPENDS lemon ${_sql_c} ${_sql_h}
    )
endif()                     # }

# download dependencies
if(${FALLBACK})             # {
    configure_file(${CONTRIB_TMP_FILE} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
    execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
    execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
endif()                     # }

# ================================================================================================
# Build
# ================================================================================================
# googletest
if(${BUILD_TEST})
    if(${FALLBACK})          # {
        set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
        add_subdirectory(googletest EXCLUDE_FROM_ALL)
        target_include_directories(
            gtest
            PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src>
        )

        if(${TD_WINDOWS})
            target_include_directories(
                gtest
                PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_win>
            )
        endif(${TD_WINDOWS})

        if(${TD_LINUX})
            target_include_directories(
                gtest
                PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_linux>
            )
        endif(${TD_LINUX})

        if(${TD_DARWIN})
            target_include_directories(
                gtest
                PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_darwin>
            )
        endif(${TD_DARWIN})
    else()                   # }{
        init_external(ext_gtest)
        ExternalProject_Add(ext_gtest
            GIT_REPOSITORY https://github.com/taosdata-contrib/googletest.git
            GIT_TAG release-1.11.0
            PREFIX "${_base}"
            CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${_ins}
            CMAKE_ARGS -DCMAKE_BUILD_TYPE:STRING=MinSizeRel
            GIT_SHALLOW true
            EXCLUDE_FROM_ALL TRUE
        )
        # include_directories(${_ins}/include)
        init_external(ext_stub)
        ExternalProject_Add(ext_stub
            GIT_REPOSITORY https://github.com/coolxv/cpp-stub.git
            GIT_TAG 3137465194014d66a8402941e80d2bccc6346f51
            # GIT_SUBMODULES "src"
            PREFIX "${_base}"
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            INSTALL_COMMAND ""
            EXCLUDE_FROM_ALL TRUE
        )
        if(${TD_LINUX})          # {
            set(ext_stub_addr_any ${_base}/src/ext_stub/src_linux PARENT_SCOPE)
        elseif(${TD_DARWIN})     # }{
            set(ext_stub_addr_any ${_base}/src/ext_stub/src_darwin PARENT_SCOPE)
        elseif(${TD_WINDOWS})    # }{
            set(ext_stub_addr_any ${_base}/src/ext_stub/src_win PARENT_SCOPE)
        endif()                  # }
        set(ext_stub_stub ${_base}/src/ext_stub/src PARENT_SCOPE)
    endif()                  # }
endif(${BUILD_TEST})

# cJson
# see https://stackoverflow.com/questions/37582508/silence-cmp0048-warnings-in-vendored-projects
if(${FALLBACK})          # {
    set(CMAKE_PROJECT_INCLUDE_BEFORE "${TD_SUPPORT_DIR}/EnableCMP0048.txt.in")
    option(ENABLE_CJSON_TEST "Enable building cJSON test" OFF)
    option(CJSON_OVERRIDE_BUILD_SHARED_LIBS "Override BUILD_SHARED_LIBS with CJSON_BUILD_SHARED_LIBS" ON)
    option(CJSON_BUILD_SHARED_LIBS "Overrides BUILD_SHARED_LIBS if CJSON_OVERRIDE_BUILD_SHARED_LIBS is enabled" OFF)
    add_subdirectory(cJson EXCLUDE_FROM_ALL)
    target_include_directories(
        cjson

        # see https://stackoverflow.com/questions/25676277/cmake-target-include-directories-prints-an-error-when-i-try-to-add-the-source
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cJson>
    )
    unset(CMAKE_PROJECT_INCLUDE_BEFORE)
else()                   # }{
    init_external(ext_cjson)
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
        GIT_SHALLOW true
        EXCLUDE_FROM_ALL TRUE
    )
endif()                  # }

# xml2
# if(${BUILD_WITH_S3})
# add_subdirectory(xml2 EXCLUDE_FROM_ALL)
# endif()

# lz4
if(${FALLBACK})          # {
    add_subdirectory(lz4/build/cmake EXCLUDE_FROM_ALL)
    target_include_directories(
        lz4_static
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lib
    )
else()                   # }{
    init_external(ext_lz4)
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
        GIT_SHALLOW true
        EXCLUDE_FROM_ALL TRUE
    )
endif()                  # }

# lzma2
if(${FALLBACK})          # {
else()                   # }{
    init_external(ext_lzma2)
    ExternalProject_Add(ext_lzma2
        GIT_REPOSITORY https://github.com/conor42/fast-lzma2.git
        PREFIX "${_base}"
        BUILD_IN_SOURCE TRUE
        PATCH_COMMAND
            COMMAND git restore -- Makefile
            COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/lzma2.diff
        CONFIGURE_COMMAND ""
        BUILD_COMMAND ""
        INSTALL_COMMAND ""
        GIT_SHALLOW true
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
    )
    add_custom_target(
        ext_lzma2_builder
        DEPENDS ext_lzma2
               ${_ins}/usr/local/include/fast-lzma2.h
               ${_ins}/usr/local/include/fl2_errors.h
               ${_ins}/usr/local/include/xxhash.h
               ${_ins}/usr/local/lib/libfast-lzma2.a
    )
endif()                  # }

# zlib
if(${FALLBACK})          # {
    set(CMAKE_PROJECT_INCLUDE_BEFORE "${TD_SUPPORT_DIR}/EnableCMP0048.txt.in")
    # freemine: https://cmake.org/cmake/help/latest/variable/CMAKE_PROJECT_INCLUDE_BEFORE.html
    #           Added in version 3.15
    if(${TD_DARWIN})
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=deprecated-non-prototype")
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=deprecated-non-prototype")
    endif(${TD_DARWIN})

    add_subdirectory(zlib EXCLUDE_FROM_ALL)

    if(${TD_DARWIN})
        target_compile_options(zlibstatic PRIVATE -Wno-error=deprecated-non-prototype)
    endif()
    target_include_directories(
        zlibstatic
        PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
    )

    if(${TD_DARWIN})
        target_compile_options(zlib PRIVATE -Wno-error=deprecated-non-prototype)
    endif()
    target_include_directories(
        zlib
        PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
    )
    unset(CMAKE_PROJECT_INCLUDE_BEFORE)
else()                   # }{
    init_external(ext_zlib)
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
        GIT_SHALLOW true
        EXCLUDE_FROM_ALL TRUE
    )
endif()                  # }

# add_subdirectory(xz EXCLUDE_FROM_ALL)
# target_include_directories(
# xz
# PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/xz
# PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/xz
# )

# leveldb
if(${BUILD_WITH_LEVELDB})
    if(${FALLBACK})          # {
        option(LEVELDB_BUILD_TESTS "" OFF)
        add_subdirectory(leveldb EXCLUDE_FROM_ALL)
        target_include_directories(
            leveldb
            PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/leveldb/include>
        )
    else()                   # }{
    endif()                  # }
endif(${BUILD_WITH_LEVELDB})

if(${FALLBACK})          # {
    if(${BUILD_WITH_UV})
        if(${TD_LINUX})
            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS_REL}")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL}")

            if("${CMAKE_BUILD_TYPE}" STREQUAL "")
                SET(CMAKE_BUILD_TYPE Release)
            endif()
        endif(${TD_LINUX})
    endif(${BUILD_WITH_UV})
else()                   # }{
endif()                  # }

# rocksdb
# To support rocksdb build on ubuntu: sudo apt-get install libgflags-dev
if(${FALLBACK})          # {
    if(${BUILD_WITH_ROCKSDB})
        if(${BUILD_CONTRIB})
            if(${TD_LINUX})
                SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL} -Wno-error=maybe-uninitialized -Wno-error=unused-but-set-variable -Wno-error=unused-variable -Wno-error=unused-function -Wno-errno=unused-private-field -Wno-error=unused-result")

                if("${CMAKE_BUILD_TYPE}" STREQUAL "")
                    SET(CMAKE_BUILD_TYPE Release)
                endif()
            endif(${TD_LINUX})

            MESSAGE(STATUS "ROCKSDB CXXXX STATUS CONFIG: " ${CMAKE_CXX_FLAGS})
            MESSAGE(STATUS "ROCKSDB C STATUS CONFIG: " ${CMAKE_C_FLAGS})

            if(${TD_DARWIN})
                SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=maybe-uninitialized")
            endif(${TD_DARWIN})

            if(${TD_DARWIN_ARM64})
                set(HAS_ARMV8_CRC true)
            endif(${TD_DARWIN_ARM64})

            if(${TD_WINDOWS})
                SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4244 /wd4819 /std:c++17")
                SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4244 /wd4819")
                option(WITH_JNI "" OFF)

                if(CMAKE_C_FLAGS MATCHES "/MT" OR CMAKE_C_FLAGS MATCHES "/MTd")
                    message("Rocksdb build runtime lib use /MT or /MTd")
                    option(WITH_MD_LIBRARY "build with MD" OFF)
                endif()

                set(SYSTEM_LIBS ${SYSTEM_LIBS} shlwapi.lib rpcrt4.lib)
            endif(${TD_WINDOWS})

            if(${TD_DARWIN})
                option(HAVE_THREAD_LOCAL "" OFF)
                option(WITH_IOSTATS_CONTEXT "" OFF)
                option(WITH_PERF_CONTEXT "" OFF)
            endif(${TD_DARWIN})

            option(WITH_FALLOCATE "" OFF)
            option(WITH_JEMALLOC "" OFF)
            option(WITH_GFLAGS "" OFF)
            option(PORTABLE "" ON)
            option(WITH_LIBURING "" OFF)
            option(FAIL_ON_WARNINGS OFF)

            option(WITH_TESTS "" OFF)
            option(WITH_BENCHMARK_TOOLS "" OFF)
            option(WITH_TOOLS "" OFF)
            option(WITH_LIBURING "" OFF)

            option(ROCKSDB_BUILD_SHARED "Build shared versions of the RocksDB libraries" OFF)
            add_subdirectory(rocksdb EXCLUDE_FROM_ALL)
            target_include_directories(
                rocksdb
                PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include>
            )
        else()
            if(NOT ${TD_LINUX})
                MESSAGE(STATUS "ROCKSDB CXX STATUS CONFIG: " ${CMAKE_CXX_FLAGS})
                MESSAGE(STATUS "ROCKSDB C STATUS CONFIG: " ${CMAKE_C_FLAGS})

                if(${TD_DARWIN})
                    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=maybe-uninitialized")
                    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=maybe-uninitialized")
                endif(${TD_DARWIN})

                if(${TD_DARWIN_ARM64})
                    set(HAS_ARMV8_CRC true)
                endif(${TD_DARWIN_ARM64})

                if(${TD_WINDOWS})
                    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4244 /wd4819 /std:c++17")
                    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /wd4244 /wd4819")
                    option(WITH_JNI "" OFF)

                    if(CMAKE_C_FLAGS MATCHES "/MT" OR CMAKE_C_FLAGS MATCHES "/MTd")
                        message("Rocksdb build runtime lib use /MT or /MTd")
                        option(WITH_MD_LIBRARY "build with MD" OFF)
                    endif()

                    set(SYSTEM_LIBS ${SYSTEM_LIBS} shlwapi.lib rpcrt4.lib)
                endif(${TD_WINDOWS})

                if(${TD_DARWIN})
                    option(HAVE_THREAD_LOCAL "" OFF)
                    option(WITH_IOSTATS_CONTEXT "" OFF)
                    option(WITH_PERF_CONTEXT "" OFF)
                endif(${TD_DARWIN})

                option(WITH_FALLOCATE "" OFF)
                option(WITH_JEMALLOC "" OFF)
                option(WITH_GFLAGS "" OFF)
                option(PORTABLE "" ON)
                option(WITH_LIBURING "" OFF)
                option(FAIL_ON_WARNINGS OFF)

                option(WITH_TESTS "" OFF)
                option(WITH_BENCHMARK_TOOLS "" OFF)
                option(WITH_TOOLS "" OFF)
                option(WITH_LIBURING "" OFF)

                option(ROCKSDB_BUILD_SHARED "Build shared versions of the RocksDB libraries" OFF)
                add_subdirectory(rocksdb EXCLUDE_FROM_ALL)
                target_include_directories(
                    rocksdb
                    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/rocksdb/include>
                )
            endif()
        endif()
    endif()
else()                   # }{
    if(${BUILD_WITH_ROCKSDB})              # {
        if(${BUILD_CONTRIB})               # {
            init_external(ext_rocksdb)
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
                GIT_SHALLOW true
                EXCLUDE_FROM_ALL TRUE
            )
        endif()                            # }{
    endif()                                # }
endif()                  # }

if(${BUILD_WITH_S3})
    if(${FALLBACK})          # {
        INCLUDE_DIRECTORIES($ENV{HOME}/.cos-local.2/include)
        MESSAGE("build with s3: ${BUILD_WITH_S3}")
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }

# cos
elseif(${BUILD_WITH_COS})
    if(${FALLBACK})          # {
        if(${TD_LINUX})
            set(CMAKE_PREFIX_PATH $ENV{HOME}/.cos-local.1)

            # ADD_DEFINITIONS(-DMINIXML_LIBRARY=${CMAKE_BINARY_DIR}/build/lib/libxml.a)
            option(ENABLE_TEST "Enable the tests" OFF)
            INCLUDE_DIRECTORIES($ENV{HOME}/.cos-local.1/include)

            # MESSAGE("$ENV{HOME}/.cos-local.1/include")
            set(CMAKE_BUILD_TYPE Release)
            set(ORIG_CMAKE_PROJECT_NAME ${CMAKE_PROJECT_NAME})
            set(CMAKE_PROJECT_NAME cos_c_sdk)

            add_subdirectory(cos-c-sdk-v5 EXCLUDE_FROM_ALL)
            target_include_directories(
                cos_c_sdk
                PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cos-c-sdk-v5/cos_c_sdk>
            )

            set(CMAKE_PROJECT_NAME ${ORIG_CMAKE_PROJECT_NAME})
        else()
        endif(${TD_LINUX})
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif()

# pthread
if(${BUILD_PTHREAD})
    if(${FALLBACK})          # {
        if("${CMAKE_BUILD_TYPE}" STREQUAL "")
            SET(CMAKE_BUILD_TYPE Release)
        endif()

        add_definitions(-DPTW32_STATIC_LIB)
        add_subdirectory(pthread EXCLUDE_FROM_ALL)
        set_target_properties(libpthreadVC3 PROPERTIES OUTPUT_NAME pthread)
        add_library(pthread INTERFACE)
        target_link_libraries(pthread INTERFACE libpthreadVC3)
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif()

# jemalloc
if(${JEMALLOC_ENABLED})
    if(${FALLBACK})          # {
        include(ExternalProject)
        ExternalProject_Add(jemalloc
            PREFIX "jemalloc"
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc
            BUILD_IN_SOURCE 1
            CONFIGURE_COMMAND ./autogen.sh COMMAND ./configure --prefix=${CMAKE_BINARY_DIR}/build/ --disable-initial-exec-tls
            BUILD_COMMAND ${MAKE}
        )
        INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif()

# crashdump
if(${BUILD_CRASHDUMP})
    if(${FALLBACK})          # {
        add_executable(dumper "crashdump/dumper/dumper.c")
        target_link_libraries(dumper User32.lib dbghelp.lib)
        file(READ "crashdump/crasher/crasher.c" CRASHDUMP_CONTENT)
        string(REPLACE "main(" "main_crashdump(" CRASHDUMP_CONTENT "${CRASHDUMP_CONTENT}")
        file(WRITE "crashdump/crasher/crasher.c" "${CRASHDUMP_CONTENT}")
        add_library(crashdump STATIC "crashdump/crasher/crasher.c")
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif()

# iconv
if(${BUILD_WITH_ICONV})
    if(${FALLBACK})          # {
        add_library(iconv STATIC iconv/win_iconv.c)
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif(${BUILD_WITH_ICONV})

# wingetopt
if(${BUILD_WINGETOPT})
    if(${FALLBACK})          # {
        add_subdirectory(wingetopt EXCLUDE_FROM_ALL)
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif(${BUILD_WINGETOPT})

# msvcregex
if(${BUILD_MSVCREGEX})
    if(${FALLBACK})          # {
        add_library(msvcregex STATIC "")
        target_sources(msvcregex
            PRIVATE "msvcregex/regex.c"
        )
        target_include_directories(msvcregex
            PRIVATE "msvcregex"
        )
        target_link_libraries(msvcregex
            INTERFACE Shell32
        )
        SET_TARGET_PROPERTIES(msvcregex PROPERTIES OUTPUT_NAME msvcregex)
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif(${BUILD_MSVCREGEX})

# wcwidth
if(${BUILD_WCWIDTH})
    if(${FALLBACK})          # {
        add_library(wcwidth STATIC "")
        target_sources(wcwidth
            PRIVATE "wcwidth/wcwidth.c"
        )
        target_include_directories(wcwidth
            PRIVATE "wcwidth"
        )
        SET_TARGET_PROPERTIES(wcwidth PROPERTIES OUTPUT_NAME wcwidth)
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif(${BUILD_WCWIDTH})

# LIBUV
if(${BUILD_WITH_UV})
    if(${FALLBACK})          # {
        if(TD_WINDOWS)
            # There is no GetHostNameW function on win7.
            file(READ "libuv/src/win/util.c" LIBUV_WIN_UTIL_CONTENT)
            string(REPLACE "if (GetHostNameW(buf, UV_MAXHOSTNAMESIZE" "DWORD  nSize = UV_MAXHOSTNAMESIZE;\n  if (GetComputerNameW(buf, &nSize" LIBUV_WIN_UTIL_CONTENT "${LIBUV_WIN_UTIL_CONTENT}")
            file(WRITE "libuv/src/win/util.c" "${LIBUV_WIN_UTIL_CONTENT}")
        endif()

        add_subdirectory(libuv EXCLUDE_FROM_ALL)
    else()                   # }{
        init_external(ext_libuv)
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
            GIT_SHALLOW true
            EXCLUDE_FROM_ALL TRUE
        )
    endif()                  # }
endif(${BUILD_WITH_UV})

# SQLite
# see https://stackoverflow.com/questions/8774593/cmake-link-to-external-library#comment58570736_10550334
if(${BUILD_WITH_SQLITE})
    if(${FALLBACK})          # {
        add_library(sqlite STATIC IMPORTED GLOBAL)
        set_target_properties(sqlite PROPERTIES
            IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/sqlite/.libs/libsqlite3.a"
            INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/sqlite"
        )
        target_link_libraries(sqlite
            INTERFACE m
            INTERFACE pthread
        )

        if(NOT TD_WINDOWS)
            target_link_libraries(sqlite
                INTERFACE dl
            )
        endif(NOT TD_WINDOWS)
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif(${BUILD_WITH_SQLITE})

# addr2line
if(${BUILD_ADDR2LINE})
    if(${FALLBACK})          # {
        if(NOT ${TD_WINDOWS})
            check_include_file("sys/types.h" HAVE_SYS_TYPES_H)
            check_include_file("sys/stat.h" HAVE_SYS_STAT_H)
            check_include_file("inttypes.h" HAVE_INTTYPES_H)
            check_include_file("stddef.h" HAVE_STDDEF_H)
            check_include_file("stdlib.h" HAVE_STDLIB_H)
            check_include_file("string.h" HAVE_STRING_H)
            check_include_file("memory.h" HAVE_MEMORY_H)
            check_include_file("strings.h" HAVE_STRINGS_H)
            check_include_file("stdint.h" HAVE_STDINT_H)
            check_include_file("unistd.h" HAVE_UNISTD_H)
            check_include_file("sgidefs.h" HAVE_SGIDEFS_H)
            check_include_file("stdafx.h" HAVE_STDAFX_H)
            check_include_file("elf.h" HAVE_ELF_H)
            check_include_file("libelf.h" HAVE_LIBELF_H)
            check_include_file("libelf/libelf.h" HAVE_LIBELF_LIBELF_H)
            check_include_file("alloca.h" HAVE_ALLOCA_H)
            check_include_file("elfaccess.h" HAVE_ELFACCESS_H)
            check_include_file("sys/elf_386.h" HAVE_SYS_ELF_386_H)
            check_include_file("sys/elf_amd64.h" HAVE_SYS_ELF_AMD64_H)
            check_include_file("sys/elf_sparc.h" HAVE_SYS_ELF_SPARC_H)
            check_include_file("sys/ia64/elf.h" HAVE_SYS_IA64_ELF_H)
            set(VERSION 0.3.1)
            set(PACKAGE_VERSION "\"${VERSION}\"")
            configure_file(libdwarf/cmake/config.h.cmake config.h)
            file(GLOB_RECURSE LIBDWARF_SOURCES "libdwarf/src/lib/libdwarf/*.c")
            add_library(libdwarf STATIC ${LIBDWARF_SOURCES})
            set_target_properties(libdwarf PROPERTIES OUTPUT_NAME "libdwarf")

            if(HAVE_LIBELF_H OR HAVE_LIBELF_LIBELF_H)
                target_link_libraries(libdwarf PUBLIC libelf)
            endif()

            target_include_directories(libdwarf SYSTEM PUBLIC "libdwarf/src/lib/libdwarf" ${CMAKE_CURRENT_BINARY_DIR})
            file(READ "addr2line/addr2line.c" ADDR2LINE_CONTENT)
            string(REPLACE "static int" "int" ADDR2LINE_CONTENT "${ADDR2LINE_CONTENT}")
            string(REPLACE "static void" "void" ADDR2LINE_CONTENT "${ADDR2LINE_CONTENT}")
            string(REPLACE "main(" "main_addr2line(" ADDR2LINE_CONTENT "${ADDR2LINE_CONTENT}")
            file(WRITE "addr2line/addr2line.c" "${ADDR2LINE_CONTENT}")
            add_library(addr2line STATIC "addr2line/addr2line.c")
            target_link_libraries(addr2line PUBLIC libdwarf dl z)
            target_include_directories(addr2line PUBLIC "libdwarf/src/lib/libdwarf")
        endif(NOT ${TD_WINDOWS})
    else()                   # }{
        message(FATAL_ERROR      "freemine: not refactored yet")
    endif()                  # }
endif(${BUILD_ADDR2LINE})

# geos
if(${BUILD_GEOS})
    if(${FALLBACK})          # {
        if(${TD_LINUX})
            set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS_REL}")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL}")

            if("${CMAKE_BUILD_TYPE}" STREQUAL "")
                SET(CMAKE_BUILD_TYPE Release)
            endif()
        endif(${TD_LINUX})

        option(BUILD_SHARED_LIBS "Build GEOS with shared libraries" OFF)
        add_subdirectory(geos EXCLUDE_FROM_ALL)

        if(${TD_WINDOWS})
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
            SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
        else()
            unset(CMAKE_CXX_STANDARD CACHE) # undo libgeos's setting of global CMAKE_CXX_STANDARD
        endif(${TD_WINDOWS})

        target_include_directories(
            geos_c
            PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/geos/include>
        )
    else()                   # }{
        init_external(ext_geos)
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
            GIT_SHALLOW true
            EXCLUDE_FROM_ALL TRUE
        )
    endif()                  # }
endif(${BUILD_GEOS})

if(${BUILD_PCRE2})
    if(${FALLBACK})          # {
        add_subdirectory(pcre2 EXCLUDE_FROM_ALL)
    else()                   # }{
        init_external(ext_pcre2)
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
            GIT_SHALLOW true
            EXCLUDE_FROM_ALL TRUE
        )
    endif()                  # }
endif(${BUILD_PCRE2})

if(${TD_LINUX} AND ${BUILD_WITH_S3})
    if(${FALLBACK})          # {
        add_subdirectory(azure-cmake EXCLUDE_FROM_ALL)
    endif()                  # }
endif()

IF(TD_LINUX)
    SET(TZ_OUTPUT_PATH /usr/share/zoneinfo)
ELSEIF(TD_DARWIN)
    SET(TZ_OUTPUT_PATH /var/db/timezone/zoneinfo)
ENDIF()


if(NOT ${TD_WINDOWS})
    if(${FALLBACK})          # {
        MESSAGE(STATUS "timezone file path: " ${TZ_OUTPUT_PATH})

        execute_process(
                COMMAND make TZDIR=${TZ_OUTPUT_PATH}/ clean tzdir.h
                WORKING_DIRECTORY "${TD_CONTRIB_DIR}/tz"
        )

        set(TZ_SRC_DIR "${TD_SOURCE_DIR}/source/os/src/timezone")
        file(REMOVE_RECURSE ${TZ_SRC_DIR})
        file(MAKE_DIRECTORY ${TZ_SRC_DIR})
        file(COPY ${TD_CONTRIB_DIR}/tz/private.h ${TD_CONTRIB_DIR}/tz/tzdir.h ${TD_CONTRIB_DIR}/tz/tzfile.h
                ${TD_CONTRIB_DIR}/tz/localtime.c ${TD_CONTRIB_DIR}/tz/strftime.c
                DESTINATION ${TZ_SRC_DIR})
    else()                   # }{
        init_external(ext_tz)
        ExternalProject_Add(ext_tz
            GIT_REPOSITORY https://github.com/eggert/tz.git
            GIT_TAG main       # freemine: or fixed?
            PREFIX "${_base}"
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            INSTALL_COMMAND ""
            GIT_SHALLOW true
            EXCLUDE_FROM_ALL TRUE
        )
        add_custom_command(
            OUTPUT ${_ins}/usr/lib/libtz.a
            WORKING_DIRECTORY ${_base}/src/ext_tz
            COMMAND make DESTDIR=${_ins} CFLAGS=-fPIC
            COMMAND make DESTDIR=${_ins} install
        )
        add_custom_target(
            ext_tz_builder
            DEPENDS ext_tz
                   ${_ins}/usr/lib/libtz.a
        )
    endif()                  # }
endif(NOT ${TD_WINDOWS})

#if(NOT ${TD_WINDOWS})
#    execute_process(
#            COMMAND make CFLAGS+=-fPIC CFLAGS+=-g TZDIR=${TZ_OUTPUT_PATH} clean libtz.a
#            WORKING_DIRECTORY "${TD_CONTRIB_DIR}/tz"
#    )
#endif(NOT ${TD_WINDOWS})
# ================================================================================================
# Build test
# ================================================================================================
MESSAGE("build with dependency tests: ${BUILD_DEPENDENCY_TESTS}")

if(${BUILD_DEPENDENCY_TESTS})
    add_subdirectory(test EXCLUDE_FROM_ALL)
endif(${BUILD_DEPENDENCY_TESTS})
