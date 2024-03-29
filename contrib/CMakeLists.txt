# ================================================================================================
# Download
# ================================================================================================
function(cat IN_FILE OUT_FILE)
    file(READ ${IN_FILE} CONTENTS)
    file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction(cat IN_FILE OUT_FILE)

if(${BUILD_WITH_S3})
    file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.2/)

elseif(${BUILD_WITH_COS})

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


set(CONTRIB_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")
configure_file("${TD_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# taos-tools
if(${BUILD_TOOLS})
    cat("${TD_SUPPORT_DIR}/taostools_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# taosws-rs
if(${WEBSOCKET})
    cat("${TD_SUPPORT_DIR}/taosws_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# taosadapter
if(${BUILD_HTTP})
    MESSAGE("BUILD_HTTP is on")
else ()
    MESSAGE("BUILD_HTTP is off, use taosAdapter")
    cat("${TD_SUPPORT_DIR}/taosadapter_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# pthread
if(${BUILD_PTHREAD})
    cat("${TD_SUPPORT_DIR}/pthread_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# iconv
if(${BUILD_WITH_ICONV})
    cat("${TD_SUPPORT_DIR}/iconv_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# jemalloc
if(${JEMALLOC_ENABLED})
    cat("${TD_SUPPORT_DIR}/jemalloc_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# msvc regex
if(${BUILD_MSVCREGEX})
    cat("${TD_SUPPORT_DIR}/msvcregex_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# wcwidth
if(${BUILD_WCWIDTH})
    cat("${TD_SUPPORT_DIR}/wcwidth_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# wingetopt
if(${BUILD_WINGETOPT})
    cat("${TD_SUPPORT_DIR}/wingetopt_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# googletest
if(${BUILD_TEST})
    cat("${TD_SUPPORT_DIR}/gtest_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/stub_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_TEST})

# lz4
cat("${TD_SUPPORT_DIR}/lz4_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# zlib
cat("${TD_SUPPORT_DIR}/zlib_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# cJson
cat("${TD_SUPPORT_DIR}/cjson_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

if (${BUILD_CONTRIB})
    if(${BUILD_WITH_ROCKSDB})
        cat("${TD_SUPPORT_DIR}/rocksdb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        add_definitions(-DUSE_ROCKSDB)
    endif()
else() 
    if (NOT ${TD_LINUX})
        if(${BUILD_WITH_ROCKSDB})
            cat("${TD_SUPPORT_DIR}/rocksdb_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
            add_definitions(-DUSE_ROCKSDB)
        endif(${BUILD_WITH_ROCKSDB})
    else()
        if(${BUILD_WITH_ROCKSDB})
            add_definitions(-DUSE_ROCKSDB)
        endif(${BUILD_WITH_ROCKSDB})
    endif()
endif()

#libuv
if(${BUILD_WITH_UV})
    cat("${TD_SUPPORT_DIR}/libuv_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_UV})

# sqlite
if(${BUILD_WITH_SQLITE})
    cat("${TD_SUPPORT_DIR}/sqlite_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_WITH_SQLITE})

# s3
if(${BUILD_WITH_S3})
    cat("${TD_SUPPORT_DIR}/ssl_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/xml2_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/curl_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/libs3_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    add_definitions(-DUSE_S3)

# cos
elseif(${BUILD_WITH_COS})
    #cat("${TD_SUPPORT_DIR}/mxml_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    #cat("${TD_SUPPORT_DIR}/apr_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    #cat("${TD_SUPPORT_DIR}/apr-util_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    #cat("${TD_SUPPORT_DIR}/curl_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/cos_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    add_definitions(-DUSE_COS)

endif()

# crashdump
if(${BUILD_CRASHDUMP})
    cat("${TD_SUPPORT_DIR}/crashdump_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif(${BUILD_CRASHDUMP})

# addr2line
if(${BUILD_ADDR2LINE})
    if(NOT ${TD_WINDOWS})
        cat("${TD_SUPPORT_DIR}/libdwarf_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
        cat("${TD_SUPPORT_DIR}/addr2line_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    endif(NOT ${TD_WINDOWS})
endif(${BUILD_ADDR2LINE})

# geos
if(${BUILD_GEOS})
    cat("${TD_SUPPORT_DIR}/geos_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
endif()

# download dependencies
configure_file(${CONTRIB_TMP_FILE} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
        WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download")

# ================================================================================================
# Build
# ================================================================================================
# googletest
if(${BUILD_TEST})
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

endif(${BUILD_TEST})

# cJson
# see https://stackoverflow.com/questions/37582508/silence-cmp0048-warnings-in-vendored-projects
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

# xml2
#if(${BUILD_WITH_S3})
#  add_subdirectory(xml2 EXCLUDE_FROM_ALL)
#endif()

# lz4
add_subdirectory(lz4/build/cmake EXCLUDE_FROM_ALL)
target_include_directories(
    lz4_static
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lib
)

# zlib
set(CMAKE_PROJECT_INCLUDE_BEFORE "${TD_SUPPORT_DIR}/EnableCMP0048.txt.in")
add_subdirectory(zlib EXCLUDE_FROM_ALL)
target_include_directories(
    zlibstatic
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
)
target_include_directories(
    zlib
    PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/zlib
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/zlib
)
unset(CMAKE_PROJECT_INCLUDE_BEFORE)

# leveldb
if(${BUILD_WITH_LEVELDB})
    option(LEVELDB_BUILD_TESTS "" OFF)
    add_subdirectory(leveldb EXCLUDE_FROM_ALL)
    target_include_directories(
        leveldb
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/leveldb/include>
    )
endif(${BUILD_WITH_LEVELDB})

# rocksdb
# To support rocksdb build on ubuntu: sudo apt-get install libgflags-dev
if (${BUILD_WITH_UV})
    if(${TD_LINUX})
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS_REL}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL}")
        IF ("${CMAKE_BUILD_TYPE}" STREQUAL "")
            SET(CMAKE_BUILD_TYPE Release)
        endif()
    endif(${TD_LINUX})
endif (${BUILD_WITH_UV})

if (${BUILD_WITH_ROCKSDB})
    if (${BUILD_CONTRIB})
        if(${TD_LINUX})
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL} -Wno-error=maybe-uninitialized -Wno-error=unused-but-set-variable -Wno-error=unused-variable -Wno-error=unused-function -Wno-errno=unused-private-field -Wno-error=unused-result")
            if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
                SET(CMAKE_BUILD_TYPE Release)
            endif()
        endif(${TD_LINUX})
        MESSAGE(STATUS "ROCKSDB CXXXX STATUS CONFIG: " ${CMAKE_CXX_FLAGS})
        MESSAGE(STATUS "ROCKSDB C STATUS CONFIG: " ${CMAKE_C_FLAGS})

        if(${TD_DARWIN})
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=maybe-uninitialized")
        endif(${TD_DARWIN})

        if (${TD_DARWIN_ARM64})
            set(HAS_ARMV8_CRC true)
        endif(${TD_DARWIN_ARM64})

        if (${TD_WINDOWS})
            SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4244 /wd4819")
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
        if (NOT ${TD_LINUX})
            MESSAGE(STATUS "ROCKSDB CXX STATUS CONFIG: " ${CMAKE_CXX_FLAGS})
            MESSAGE(STATUS "ROCKSDB C STATUS CONFIG: " ${CMAKE_C_FLAGS})
            if(${TD_DARWIN})
                SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=maybe-uninitialized")
                SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-error=maybe-uninitialized")
            endif(${TD_DARWIN})

            if (${TD_DARWIN_ARM64})
                set(HAS_ARMV8_CRC true)
            endif(${TD_DARWIN_ARM64})

            if (${TD_WINDOWS})
                SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd4244 /wd4819")
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

if(${BUILD_WITH_S3})
    INCLUDE_DIRECTORIES($ENV{HOME}/.cos-local.2/include)
    MESSAGE("build with s3: ${BUILD_WITH_S3}")

# cos
elseif(${BUILD_WITH_COS})
  if(${TD_LINUX})
    set(CMAKE_PREFIX_PATH $ENV{HOME}/.cos-local.1)
    #ADD_DEFINITIONS(-DMINIXML_LIBRARY=${CMAKE_BINARY_DIR}/build/lib/libxml.a)
    option(ENABLE_TEST "Enable the tests" OFF)
    INCLUDE_DIRECTORIES($ENV{HOME}/.cos-local.1/include)
    #MESSAGE("$ENV{HOME}/.cos-local.1/include")

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

endif()

# pthread
if(${BUILD_PTHREAD})
    if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    SET(CMAKE_BUILD_TYPE Release)
    endif()
    add_definitions(-DPTW32_STATIC_LIB)
    add_subdirectory(pthread EXCLUDE_FROM_ALL)
    set_target_properties(libpthreadVC3 PROPERTIES OUTPUT_NAME pthread)
    add_library(pthread INTERFACE)
    target_link_libraries(pthread INTERFACE libpthreadVC3)
endif()

# jemalloc
if(${JEMALLOC_ENABLED})
    include(ExternalProject)
    ExternalProject_Add(jemalloc
        PREFIX        "jemalloc"
        SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc
        BUILD_IN_SOURCE     1
        CONFIGURE_COMMAND  ./autogen.sh COMMAND ./configure --prefix=${CMAKE_BINARY_DIR}/build/ --disable-initial-exec-tls
        BUILD_COMMAND       ${MAKE}
    )
    INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
endif()

# crashdump
if(${BUILD_CRASHDUMP})
    add_executable(dumper "crashdump/dumper/dumper.c")
    target_link_libraries(dumper User32.lib dbghelp.lib)
    file(READ "crashdump/crasher/crasher.c" CRASHDUMP_CONTENT)
    string(REPLACE "main(" "main_crashdump(" CRASHDUMP_CONTENT "${CRASHDUMP_CONTENT}")
    file(WRITE "crashdump/crasher/crasher.c" "${CRASHDUMP_CONTENT}")
    add_library(crashdump STATIC "crashdump/crasher/crasher.c")
endif()

# iconv
if(${BUILD_WITH_ICONV})
    add_library(iconv STATIC iconv/win_iconv.c)
endif(${BUILD_WITH_ICONV})

# wingetopt
if(${BUILD_WINGETOPT})
    add_subdirectory(wingetopt EXCLUDE_FROM_ALL)
endif(${BUILD_WINGETOPT})

# msvcregex
if(${BUILD_MSVCREGEX})
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
endif(${BUILD_MSVCREGEX})

# wcwidth
if(${BUILD_WCWIDTH})
    add_library(wcwidth STATIC "")
    target_sources(wcwidth
        PRIVATE "wcwidth/wcwidth.c"
    )
    target_include_directories(wcwidth
        PRIVATE "wcwidth"
    )
    SET_TARGET_PROPERTIES(wcwidth PROPERTIES OUTPUT_NAME wcwidth)
endif(${BUILD_WCWIDTH})

# LIBUV
if(${BUILD_WITH_UV})
    if (TD_WINDOWS)
        # There is no GetHostNameW function on win7.
        file(READ "libuv/src/win/util.c" LIBUV_WIN_UTIL_CONTENT)
        string(REPLACE "if (GetHostNameW(buf, UV_MAXHOSTNAMESIZE" "DWORD  nSize = UV_MAXHOSTNAMESIZE;\n  if (GetComputerNameW(buf, &nSize" LIBUV_WIN_UTIL_CONTENT "${LIBUV_WIN_UTIL_CONTENT}")
        file(WRITE "libuv/src/win/util.c" "${LIBUV_WIN_UTIL_CONTENT}")
    endif ()
    add_subdirectory(libuv EXCLUDE_FROM_ALL)
endif(${BUILD_WITH_UV})

# SQLite
# see https://stackoverflow.com/questions/8774593/cmake-link-to-external-library#comment58570736_10550334
if(${BUILD_WITH_SQLITE})
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
endif(${BUILD_WITH_SQLITE})

# addr2line
if(${BUILD_ADDR2LINE})
    if(NOT ${TD_WINDOWS})
        check_include_file( "sys/types.h"     HAVE_SYS_TYPES_H)
        check_include_file( "sys/stat.h"      HAVE_SYS_STAT_H )
        check_include_file( "inttypes.h"      HAVE_INTTYPES_H   )
        check_include_file( "stddef.h"        HAVE_STDDEF_H   )
        check_include_file( "stdlib.h"        HAVE_STDLIB_H   )
        check_include_file( "string.h"        HAVE_STRING_H   )
        check_include_file( "memory.h"        HAVE_MEMORY_H   )
        check_include_file( "strings.h"       HAVE_STRINGS_H  )
        check_include_file( "stdint.h"        HAVE_STDINT_H   )
        check_include_file( "unistd.h"        HAVE_UNISTD_H   )
        check_include_file( "sgidefs.h"       HAVE_SGIDEFS_H  )
        check_include_file( "stdafx.h"        HAVE_STDAFX_H   )
        check_include_file( "elf.h"           HAVE_ELF_H      )
        check_include_file( "libelf.h"        HAVE_LIBELF_H   )
        check_include_file( "libelf/libelf.h" HAVE_LIBELF_LIBELF_H)
        check_include_file( "alloca.h"        HAVE_ALLOCA_H   )
        check_include_file( "elfaccess.h"     HAVE_ELFACCESS_H)
        check_include_file( "sys/elf_386.h"   HAVE_SYS_ELF_386_H  )
        check_include_file( "sys/elf_amd64.h" HAVE_SYS_ELF_AMD64_H)
        check_include_file( "sys/elf_sparc.h" HAVE_SYS_ELF_SPARC_H)
        check_include_file( "sys/ia64/elf.h"  HAVE_SYS_IA64_ELF_H )
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
        target_include_directories(addr2line PUBLIC "libdwarf/src/lib/libdwarf" )
    endif(NOT ${TD_WINDOWS})
endif(${BUILD_ADDR2LINE})

# geos
if(${BUILD_GEOS})
    if(${TD_LINUX})
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS_REL}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL}")
        if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
            SET(CMAKE_BUILD_TYPE Release)
        endif()
    endif(${TD_LINUX})
    option(BUILD_SHARED_LIBS "Build GEOS with shared libraries" OFF)
    add_subdirectory(geos EXCLUDE_FROM_ALL)
    if (${TD_WINDOWS})
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
        SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
    else ()
        unset(CMAKE_CXX_STANDARD CACHE) # undo libgeos's setting of global CMAKE_CXX_STANDARD
    endif(${TD_WINDOWS})
    target_include_directories(
        geos_c
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/geos/include>
    )
endif(${BUILD_GEOS})

# ================================================================================================
# Build test
# ================================================================================================
if(${BUILD_DEPENDENCY_TESTS})
    add_subdirectory(test EXCLUDE_FROM_ALL)
endif(${BUILD_DEPENDENCY_TESTS})
