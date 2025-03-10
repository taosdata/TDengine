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
    message(FATAL_ERROR "freemine: not implemented yet")
    set(CONTRIB_TMP_FILE3 "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in3")
    configure_file("${TD_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})

    if(${BUILD_WITH_COS})
        file(MAKE_DIRECTORY $ENV{HOME}/.cos-local.1/)
        cat("${TD_SUPPORT_DIR}/mxml_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})
        cat("${TD_SUPPORT_DIR}/apr_CMakeLists.txt.in" ${CONTRIB_TMP_FILE3})
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

# cat("${TD_SUPPORT_DIR}/zstd_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})

# s3
if(${BUILD_WITH_S3})
    message(FATAL_ERROR "freemine: not implemented yet")
    cat("${TD_SUPPORT_DIR}/xml2_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/libs3_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/azure_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    add_definitions(-DUSE_S3)

# cos
elseif(${BUILD_WITH_COS})
    message(FATAL_ERROR "freemine: not implemented yet")
    # cat("${TD_SUPPORT_DIR}/mxml_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    # cat("${TD_SUPPORT_DIR}/apr_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    # cat("${TD_SUPPORT_DIR}/apr-util_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    cat("${TD_SUPPORT_DIR}/cos_CMakeLists.txt.in" ${CONTRIB_TMP_FILE})
    add_definitions(-DUSE_COS)
endif()

# lemon
add_subdirectory(lemon)

# Force specify CC=cc on MacOS. Because the default CC setting in the generated Makefile has issues finding standard library headers
IF(${TD_DARWIN})
    SET(CONTRIB_CONFIG_ENV "CC=cc")
ENDIF()

# download dependencies
configure_file(${CONTRIB_TMP_FILE} "${TD_CONTRIB_DIR}/deps-download/CMakeLists.txt")
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
    WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download"
    RESULT_VARIABLE result)
IF(NOT result EQUAL "0")
    message(FATAL_ERROR "CMake step for dowloading dependencies failed: ${result}")
ENDIF()

execute_process(COMMAND "${CMAKE_COMMAND}" --build .
    WORKING_DIRECTORY "${TD_CONTRIB_DIR}/deps-download"
    RESULT_VARIABLE result)
IF(NOT result EQUAL "0")
    message(FATAL_ERROR "CMake step for building dependencies failed: ${result}")
ENDIF()

# ================================================================================================
# Build
# ================================================================================================
# googletest
if(${BUILD_TEST})
    # freemine: set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    # freemine: add_subdirectory(googletest EXCLUDE_FROM_ALL)
    # freemine: target_include_directories(
    # freemine:     gtest
    # freemine:     PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src>
    # freemine: )

    # freemine: if(${TD_WINDOWS})
    # freemine:     target_include_directories(
    # freemine:         gtest
    # freemine:         PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_win>
    # freemine:     )
    # freemine: endif(${TD_WINDOWS})

    # freemine: if(${TD_LINUX})
    # freemine:     target_include_directories(
    # freemine:         gtest
    # freemine:         PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_linux>
    # freemine:     )
    # freemine: endif(${TD_LINUX})

    # freemine: if(${TD_DARWIN})
    # freemine:     target_include_directories(
    # freemine:         gtest
    # freemine:         PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cpp-stub/src_darwin>
    # freemine:     )
    # freemine: endif(${TD_DARWIN})
endif(${BUILD_TEST})

# cJson
# see https://stackoverflow.com/questions/37582508/silence-cmp0048-warnings-in-vendored-projects
# freemine: set(CMAKE_PROJECT_INCLUDE_BEFORE "${TD_SUPPORT_DIR}/EnableCMP0048.txt.in")
# freemine: option(ENABLE_CJSON_TEST "Enable building cJSON test" OFF)
# freemine: option(CJSON_OVERRIDE_BUILD_SHARED_LIBS "Override BUILD_SHARED_LIBS with CJSON_BUILD_SHARED_LIBS" ON)
# freemine: option(CJSON_BUILD_SHARED_LIBS "Overrides BUILD_SHARED_LIBS if CJSON_OVERRIDE_BUILD_SHARED_LIBS is enabled" OFF)
# freemine: add_subdirectory(cJson EXCLUDE_FROM_ALL)
# freemine: target_include_directories(
# freemine:     cjson
# freemine: 
# freemine:     # see https://stackoverflow.com/questions/25676277/cmake-target-include-directories-prints-an-error-when-i-try-to-add-the-source
# freemine:     PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/cJson>
# freemine: )
# freemine: unset(CMAKE_PROJECT_INCLUDE_BEFORE)

# xml2
# if(${BUILD_WITH_S3})
# add_subdirectory(xml2 EXCLUDE_FROM_ALL)
# endif()

# lz4
# freemine: add_subdirectory(lz4/build/cmake EXCLUDE_FROM_ALL)
# freemine: target_include_directories(
# freemine:     lz4_static
# freemine:     PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/lz4/lib
# freemine: )

# freemine: unset(CMAKE_PROJECT_INCLUDE_BEFORE)

# add_subdirectory(xz EXCLUDE_FROM_ALL)
# target_include_directories(
# xz
# PUBLIC ${CMAKE_CURRENT_BINARY_DIR}/xz
# PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/xz
# )

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
if(${BUILD_WITH_UV})
    if(${TD_LINUX})
        set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS_REL}")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL}")

        if("${CMAKE_BUILD_TYPE}" STREQUAL "")
            SET(CMAKE_BUILD_TYPE Release)
        endif()
    endif(${TD_LINUX})
endif(${BUILD_WITH_UV})

if(${BUILD_WITH_S3})
    INCLUDE_DIRECTORIES($ENV{HOME}/.cos-local.2/include)
    MESSAGE("build with s3: ${BUILD_WITH_S3}")

# cos
elseif(${BUILD_WITH_COS})
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
endif()

# pthread
if(${BUILD_PTHREAD})
    # freemine: if("${CMAKE_BUILD_TYPE}" STREQUAL "")
    # freemine:     SET(CMAKE_BUILD_TYPE Release)
    # freemine: endif()

    # freemine: add_definitions(-DPTW32_STATIC_LIB)
    # freemine: add_subdirectory(pthread EXCLUDE_FROM_ALL)
    # freemine: set_target_properties(libpthreadVC3 PROPERTIES OUTPUT_NAME pthread)
    # freemine: add_library(pthread INTERFACE)
    # freemine: target_link_libraries(pthread INTERFACE libpthreadVC3)
endif()

# jemalloc
if(${JEMALLOC_ENABLED})
    include(ExternalProject)
    ExternalProject_Add(jemalloc
        PREFIX "jemalloc"
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND ./autogen.sh COMMAND ./configure --prefix=${CMAKE_BINARY_DIR}/build/ --disable-initial-exec-tls
        BUILD_COMMAND ${MAKE}
    )
    INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
endif()

# crashdump
if(${BUILD_CRASHDUMP})
    # freemine: add_executable(dumper "crashdump/dumper/dumper.c")
    # freemine: target_link_libraries(dumper User32.lib dbghelp.lib)
    # freemine: file(READ "crashdump/crasher/crasher.c" CRASHDUMP_CONTENT)
    # freemine: string(REPLACE "main(" "main_crashdump(" CRASHDUMP_CONTENT "${CRASHDUMP_CONTENT}")
    # freemine: file(WRITE "crashdump/crasher/crasher.c" "${CRASHDUMP_CONTENT}")
    # freemine: add_library(crashdump STATIC "crashdump/crasher/crasher.c")
endif()

# iconv
if(${BUILD_WITH_ICONV})
    # freemine: add_library(iconv STATIC iconv/win_iconv.c)
endif(${BUILD_WITH_ICONV})

# wingetopt
if(${BUILD_WINGETOPT})
    # freemine: add_subdirectory(wingetopt EXCLUDE_FROM_ALL)
endif(${BUILD_WINGETOPT})

# msvcregex
if(${BUILD_MSVCREGEX})
    # freemine: add_library(msvcregex STATIC "")
    # freemine: target_sources(msvcregex
    # freemine:     PRIVATE "msvcregex/regex.c"
    # freemine: )
    # freemine: target_include_directories(msvcregex
    # freemine:     PRIVATE "msvcregex"
    # freemine: )
    # freemine: target_link_libraries(msvcregex
    # freemine:     INTERFACE Shell32
    # freemine: )
    # freemine: SET_TARGET_PROPERTIES(msvcregex PROPERTIES OUTPUT_NAME msvcregex)
endif(${BUILD_MSVCREGEX})

# wcwidth
if(${BUILD_WCWIDTH})
    # freemine: add_library(wcwidth STATIC "")
    # freemine: target_sources(wcwidth
    # freemine:     PRIVATE "wcwidth/wcwidth.c"
    # freemine: )
    # freemine: target_include_directories(wcwidth
    # freemine:     PRIVATE "wcwidth"
    # freemine: )
    # freemine: SET_TARGET_PROPERTIES(wcwidth PROPERTIES OUTPUT_NAME wcwidth)
endif(${BUILD_WCWIDTH})

# LIBUV
if(${BUILD_WITH_UV})
    # freemine: if(TD_WINDOWS)
    # freemine:     # There is no GetHostNameW function on win7.
    # freemine:     file(READ "libuv/src/win/util.c" LIBUV_WIN_UTIL_CONTENT)
    # freemine:     string(REPLACE "if (GetHostNameW(buf, UV_MAXHOSTNAMESIZE" "DWORD  nSize = UV_MAXHOSTNAMESIZE;\n  if (GetComputerNameW(buf, &nSize" LIBUV_WIN_UTIL_CONTENT "${LIBUV_WIN_UTIL_CONTENT}")
    # freemine:     file(WRITE "libuv/src/win/util.c" "${LIBUV_WIN_UTIL_CONTENT}")
    # freemine: endif()

    # freemine: add_subdirectory(libuv EXCLUDE_FROM_ALL)
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
    # freemine: if(NOT ${TD_WINDOWS})
    # freemine:     check_include_file("sys/types.h" HAVE_SYS_TYPES_H)
    # freemine:     check_include_file("sys/stat.h" HAVE_SYS_STAT_H)
    # freemine:     check_include_file("inttypes.h" HAVE_INTTYPES_H)
    # freemine:     check_include_file("stddef.h" HAVE_STDDEF_H)
    # freemine:     check_include_file("stdlib.h" HAVE_STDLIB_H)
    # freemine:     check_include_file("string.h" HAVE_STRING_H)
    # freemine:     check_include_file("memory.h" HAVE_MEMORY_H)
    # freemine:     check_include_file("strings.h" HAVE_STRINGS_H)
    # freemine:     check_include_file("stdint.h" HAVE_STDINT_H)
    # freemine:     check_include_file("unistd.h" HAVE_UNISTD_H)
    # freemine:     check_include_file("sgidefs.h" HAVE_SGIDEFS_H)
    # freemine:     check_include_file("stdafx.h" HAVE_STDAFX_H)
    # freemine:     check_include_file("elf.h" HAVE_ELF_H)
    # freemine:     check_include_file("libelf.h" HAVE_LIBELF_H)
    # freemine:     check_include_file("libelf/libelf.h" HAVE_LIBELF_LIBELF_H)
    # freemine:     check_include_file("alloca.h" HAVE_ALLOCA_H)
    # freemine:     check_include_file("elfaccess.h" HAVE_ELFACCESS_H)
    # freemine:     check_include_file("sys/elf_386.h" HAVE_SYS_ELF_386_H)
    # freemine:     check_include_file("sys/elf_amd64.h" HAVE_SYS_ELF_AMD64_H)
    # freemine:     check_include_file("sys/elf_sparc.h" HAVE_SYS_ELF_SPARC_H)
    # freemine:     check_include_file("sys/ia64/elf.h" HAVE_SYS_IA64_ELF_H)
    # freemine:     set(VERSION 0.3.1)
    # freemine:     set(PACKAGE_VERSION "\"${VERSION}\"")
    # freemine:     configure_file(libdwarf/cmake/config.h.cmake config.h)
    # freemine:     file(GLOB_RECURSE LIBDWARF_SOURCES "libdwarf/src/lib/libdwarf/*.c")
    # freemine:     add_library(libdwarf STATIC ${LIBDWARF_SOURCES})
    # freemine:     set_target_properties(libdwarf PROPERTIES OUTPUT_NAME "libdwarf")

    # freemine:     if(HAVE_LIBELF_H OR HAVE_LIBELF_LIBELF_H)
    # freemine:         target_link_libraries(libdwarf PUBLIC libelf)
    # freemine:     endif()

    # freemine:     target_include_directories(libdwarf SYSTEM PUBLIC "libdwarf/src/lib/libdwarf" ${CMAKE_CURRENT_BINARY_DIR})
    # freemine:     file(READ "addr2line/addr2line.c" ADDR2LINE_CONTENT)
    # freemine:     string(REPLACE "static int" "int" ADDR2LINE_CONTENT "${ADDR2LINE_CONTENT}")
    # freemine:     string(REPLACE "static void" "void" ADDR2LINE_CONTENT "${ADDR2LINE_CONTENT}")
    # freemine:     string(REPLACE "main(" "main_addr2line(" ADDR2LINE_CONTENT "${ADDR2LINE_CONTENT}")
    # freemine:     file(WRITE "addr2line/addr2line.c" "${ADDR2LINE_CONTENT}")
    # freemine:     add_library(addr2line STATIC "addr2line/addr2line.c")
    # freemine:     target_link_libraries(addr2line PUBLIC libdwarf dl z)
    # freemine:     target_include_directories(addr2line PUBLIC "libdwarf/src/lib/libdwarf")
    # freemine: endif(NOT ${TD_WINDOWS})
endif(${BUILD_ADDR2LINE})

# geos
if(${BUILD_GEOS})
    # freemine: if(${TD_LINUX})
    # freemine:     set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS_REL}")
    # freemine:     set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS_REL}")

    # freemine:     if("${CMAKE_BUILD_TYPE}" STREQUAL "")
    # freemine:         SET(CMAKE_BUILD_TYPE Release)
    # freemine:     endif()
    # freemine: endif(${TD_LINUX})

    # freemine: option(BUILD_SHARED_LIBS "Build GEOS with shared libraries" OFF)
    # freemine: add_subdirectory(geos EXCLUDE_FROM_ALL)

    # freemine: if(${TD_WINDOWS})
    # freemine:     SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    # freemine:     SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
    # freemine: else()
    # freemine:     unset(CMAKE_CXX_STANDARD CACHE) # undo libgeos's setting of global CMAKE_CXX_STANDARD
    # freemine: endif(${TD_WINDOWS})

    # freemine: target_include_directories(
    # freemine:     geos_c
    # freemine:     PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/geos/include>
    # freemine: )
endif(${BUILD_GEOS})

if(${BUILD_PCRE2})
    # freemine: add_subdirectory(pcre2 EXCLUDE_FROM_ALL)
endif(${BUILD_PCRE2})

if(${TD_LINUX} AND ${BUILD_WITH_S3})
  set(ORIG_CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
  string(REPLACE " -Werror " " " CMAKE_C_FLAGS "${CMAKE_C_FLAGS}")
  add_subdirectory(xml2-cmake)
  set(CMAKE_C_FLAGS ${ORIG_CMAKE_C_FLAGS})

  add_subdirectory(azure-cmake)
endif()

IF(TD_LINUX)
    SET(TZ_OUTPUT_PATH /usr/share/zoneinfo)
ELSEIF(TD_DARWIN)
    SET(TZ_OUTPUT_PATH /var/db/timezone/zoneinfo)
ENDIF()


if(NOT ${TD_WINDOWS})
    # freemine: MESSAGE(STATUS "timezone file path: " ${TZ_OUTPUT_PATH})

    # freemine: execute_process(
    # freemine:         COMMAND make TZDIR=${TZ_OUTPUT_PATH}/ clean tzdir.h
    # freemine:         WORKING_DIRECTORY "${TD_CONTRIB_DIR}/tz"
    # freemine: )

    # freemine: set(TZ_SRC_DIR "${TD_SOURCE_DIR}/source/os/src/timezone")
    # freemine: file(REMOVE_RECURSE ${TZ_SRC_DIR})
    # freemine: file(MAKE_DIRECTORY ${TZ_SRC_DIR})
    # freemine: file(COPY ${TD_CONTRIB_DIR}/tz/private.h ${TD_CONTRIB_DIR}/tz/tzdir.h ${TD_CONTRIB_DIR}/tz/tzfile.h
    # freemine:         ${TD_CONTRIB_DIR}/tz/localtime.c ${TD_CONTRIB_DIR}/tz/strftime.c
    # freemine:         DESTINATION ${TZ_SRC_DIR})
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
