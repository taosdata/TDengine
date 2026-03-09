# Conan integration for TDengine
# This file provides compatibility layer between Conan and the existing build system

message(STATUS "Loading Conan dependencies...")

# Find all required packages from Conan
# Note: CMakeDeps generator will create Find*.cmake files

# Core dependencies
find_package(ZLIB REQUIRED)
find_package(lz4 REQUIRED)
find_package(fast-lzma2 REQUIRED)
find_package(xxHash REQUIRED)
find_package(LibLZMA REQUIRED)
find_package(cJSON REQUIRED)  # Note: package name is cJSON, not cjson

# Networking
find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)

find_package(PCRE2 QUIET)

# Optional dependencies based on build options
#if(${BUILD_WITH_UV})
    find_package(libuv REQUIRED)
    #endif()

# Database/Storage
if(${BUILD_CONTRIB} OR NOT ${TD_LINUX})
    find_package(RocksDB REQUIRED)
endif()

# Testing
if(${BUILD_TEST})
    find_package(GTest REQUIRED)
    find_package(cppstub QUIET)  # Header-only stub library for unit tests
    if(NOT cppstub_FOUND)
        message(STATUS "cppstub not found in Conan packages, will use ExternalProject")
    endif()
endif()

# Optional features (use QUIET to not fail if not provided by Conan)
if(${BUILD_GEOS})
    find_package(GEOS QUIET)
    if(NOT GEOS_FOUND)
        message(STATUS "GEOS not found in Conan packages, will use ExternalProject")
    endif()
endif()


if(${JEMALLOC_ENABLED})
    find_package(jemalloc QUIET)
    if(NOT jemalloc_FOUND)
        message(STATUS "jemalloc not found in Conan packages, will use ExternalProject")
    endif()
endif()

# Taos-tools dependencies
if(TD_TAOS_TOOLS)
    find_package(jansson QUIET)
    find_package(Snappy QUIET)
    find_package(avro-c QUIET)
    if(NOT jansson_FOUND OR NOT Snappy_FOUND OR NOT avro-c_FOUND)
        message(STATUS "taos-tools dependencies not found in Conan packages, will use ExternalProject")
    endif()
endif()

# S3 dependencies
if(${BUILD_WITH_S3})
    find_package(LibXml2 QUIET)
    if(NOT LibXml2_FOUND)
        message(STATUS "LibXml2 not found in Conan packages, will use ExternalProject")
    endif()
endif()

message(STATUS "All Conan dependencies loaded successfully")

# ============================================================================
# Compatibility macros to minimize changes to existing code
# ============================================================================

# Create variables similar to external.cmake for backward compatibility
set(ext_zlib_build_contrib FALSE)
set(ext_lz4_build_contrib FALSE)
set(ext_lzma2_build_contrib FALSE)
set(ext_xxhash_build_contrib FALSE)
set(ext_cjson_build_contrib FALSE)
set(ext_xz_build_contrib FALSE)
set(ext_ssl_build_contrib FALSE)
set(ext_curl_build_contrib FALSE)
set(ext_libuv_build_contrib FALSE)
set(ext_rocksdb_build_contrib FALSE)
set(ext_gtest_build_contrib FALSE)
set(ext_geos_build_contrib FALSE)
set(ext_pcre2_build_contrib FALSE)
set(ext_jemalloc_build_contrib FALSE)
set(ext_jansson_build_contrib FALSE)
set(ext_snappy_build_contrib FALSE)
set(ext_libxml2_build_contrib FALSE)

# Compatibility macros for dependency injection
# These macros provide the same interface as the original DEP_ext_* macros

macro(DEP_ext_zlib tgt)
    target_link_libraries(${tgt} PUBLIC ZLIB::ZLIB)
endmacro()

macro(DEP_ext_zlib_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_zlib_LIB tgt)
    target_link_libraries(${tgt} PRIVATE ZLIB::ZLIB)
endmacro()

macro(DEP_ext_lz4 tgt)
    target_link_libraries(${tgt} PUBLIC lz4::lz4)
endmacro()

macro(DEP_ext_lz4_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_lz4_LIB tgt)
    target_link_libraries(${tgt} PRIVATE lz4::lz4)
endmacro()

macro(DEP_ext_lzma2 tgt)
    target_link_libraries(${tgt} PUBLIC fast-lzma2::fast-lzma2)
endmacro()

macro(DEP_ext_lzma2_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_lzma2_LIB tgt)
    target_link_libraries(${tgt} PRIVATE fast-lzma2::fast-lzma2)
endmacro()

macro(DEP_ext_cjson tgt)
    target_link_libraries(${tgt} PUBLIC cjson::cjson)
    # Get cjson include directory and add cjson/ subdirectory for backward compatibility
    # This allows both #include "cJSON.h" and #include "cjson/cJSON.h"
    target_include_directories(${tgt} PUBLIC "${cJSON_INCLUDE_DIR}/cjson")
endmacro()

macro(DEP_ext_cjson_INC tgt)
    # Handled by target_link_libraries and DEP_ext_cjson
endmacro()

macro(DEP_ext_cjson_LIB tgt)
    target_link_libraries(${tgt} PRIVATE cjson::cjson)
    # Get cjson include directory and add cjson/ subdirectory for backward compatibility
    get_target_property(_cjson_includes cjson::cjson INTERFACE_INCLUDE_DIRECTORIES)
    if(_cjson_includes)
        foreach(_inc_dir ${_cjson_includes})
            if(EXISTS "${_inc_dir}/cjson")
                target_include_directories(${tgt} PRIVATE "${_inc_dir}/cjson")
            endif()
        endforeach()
    endif()
endmacro()

macro(DEP_ext_xz tgt)
    target_link_libraries(${tgt} PUBLIC LibLZMA::LibLZMA)
endmacro()

macro(DEP_ext_xz_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_xz_LIB tgt)
    target_link_libraries(${tgt} PRIVATE LibLZMA::LibLZMA)
endmacro()

macro(DEP_ext_ssl tgt)
    target_link_libraries(${tgt} PUBLIC OpenSSL::SSL OpenSSL::Crypto)
endmacro()

macro(DEP_ext_ssl_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_ssl_LIB tgt)
    target_link_libraries(${tgt} PRIVATE OpenSSL::SSL OpenSSL::Crypto)
endmacro()

macro(DEP_ext_curl tgt)
    target_link_libraries(${tgt} PUBLIC CURL::libcurl)
endmacro()

macro(DEP_ext_curl_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_curl_LIB tgt)
    target_link_libraries(${tgt} PRIVATE CURL::libcurl)
endmacro()

macro(DEP_ext_libuv tgt)
    target_link_libraries(${tgt} PUBLIC libuv::uv_a)
    if(NOT ${TD_WINDOWS})
        target_link_libraries(${tgt} PUBLIC dl)
    endif()
endmacro()

macro(DEP_ext_libuv_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_libuv_LIB tgt)
    target_link_libraries(${tgt} PRIVATE libuv::uv_a)
    if(NOT ${TD_WINDOWS})
        target_link_libraries(${tgt} PUBLIC dl)
    endif()
endmacro()

macro(DEP_ext_rocksdb tgt)
    if(TARGET RocksDB::rocksdb)
        target_link_libraries(${tgt} PUBLIC RocksDB::rocksdb)
    endif()
endmacro()

macro(DEP_ext_rocksdb_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_rocksdb_LIB tgt)
    if(TARGET RocksDB::rocksdb)
        target_link_libraries(${tgt} PRIVATE RocksDB::rocksdb)
    endif()
endmacro()

macro(DEP_ext_gtest tgt)
    if(TARGET GTest::gtest)
        target_link_libraries(${tgt} PUBLIC GTest::gtest GTest::gtest_main)
        target_compile_features(${tgt} PUBLIC cxx_std_11)
        target_link_libraries(${tgt} PRIVATE Threads::Threads)
    endif()
endmacro()

macro(DEP_ext_gtest_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_gtest_LIB tgt)
    if(TARGET GTest::gtest)
        target_link_libraries(${tgt} PRIVATE GTest::gtest GTest::gtest_main)
    endif()
endmacro()

macro(DEP_ext_geos tgt)
    if(TARGET GEOS::geos_c)
        target_link_libraries(${tgt} PUBLIC GEOS::geos_c GEOS::geos)
    endif()
endmacro()

macro(DEP_ext_geos_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_geos_LIB tgt)
    if(TARGET GEOS::geos_c)
        target_link_libraries(${tgt} PRIVATE GEOS::geos_c GEOS::geos)
    endif()
endmacro()

macro(DEP_ext_pcre2 tgt)
    if(TARGET pcre2::pcre2)
        target_link_libraries(${tgt} PUBLIC pcre2::pcre2)
    endif()
endmacro()

macro(DEP_ext_pcre2_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_pcre2_LIB tgt)
    if(TARGET pcre2::pcre2)
        target_link_libraries(${tgt} PRIVATE pcre2::pcre2)
    endif()
endmacro()

macro(DEP_ext_jemalloc tgt)
    if(TARGET jemalloc::jemalloc)
        target_link_libraries(${tgt} PUBLIC jemalloc::jemalloc)
    endif()
endmacro()

macro(DEP_ext_jemalloc_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_jemalloc_LIB tgt)
    if(TARGET jemalloc::jemalloc)
        target_link_libraries(${tgt} PRIVATE jemalloc::jemalloc)
    endif()
endmacro()

macro(DEP_ext_jansson tgt)
    if(TARGET jansson::jansson)
        target_link_libraries(${tgt} PUBLIC jansson::jansson)
    endif()
endmacro()

macro(DEP_ext_jansson_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_jansson_LIB tgt)
    if(TARGET jansson::jansson)
        target_link_libraries(${tgt} PRIVATE jansson::jansson)
    endif()
endmacro()

macro(DEP_ext_snappy tgt)
    if(TARGET Snappy::snappy)
        target_link_libraries(${tgt} PUBLIC Snappy::snappy)
    endif()
endmacro()

macro(DEP_ext_snappy_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_snappy_LIB tgt)
    if(TARGET Snappy::snappy)
        target_link_libraries(${tgt} PRIVATE Snappy::snappy)
    endif()
endmacro()

macro(DEP_ext_libxml2 tgt)
    if(TARGET LibXml2::LibXml2)
        target_link_libraries(${tgt} PUBLIC LibXml2::LibXml2)
    endif()
endmacro()

macro(DEP_ext_libxml2_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_libxml2_LIB tgt)
    if(TARGET LibXml2::LibXml2)
        target_link_libraries(${tgt} PRIVATE LibXml2::LibXml2)
    endif()
endmacro()

# For libraries not yet migrated to Conan, add stdc++ as before
macro(ADD_STDCPP_LINK tgt)
    if(NOT ${TD_WINDOWS})
        target_link_libraries(${tgt} PUBLIC stdc++)
    endif()
endmacro()

# Note: The following dependencies are NOT handled by Conan and should remain
# as ExternalProject or be handled separately:
# - xxHash (may need custom recipe)
# - fast-lzma2 (not in ConanCenter)
# - libdwarf, addr2line (debugging, not in ConanCenter)
# - libs3, azure-sdk, cos-sdk (cloud storage SDKs)
# - mxml, apr, apr-util (COS dependencies)
# - Windows-specific: pthread-win32, iconv, msvcregex, wcwidth, wingetopt, crashdump
# - Internal libraries: TSZ, libaes, libmqtt (in contrib/)
# - avro-c (may not be in ConanCenter)
# - sqlite (if BUILD_WITH_SQLITE is used)
# - taosws (Rust-based, special handling)
#
# Successfully migrated to Conan:
# - cppstub (testing stub library) - now available as Conan package
# - fast-lzma2 - now available as Conan package

# Stub macros for dependencies not yet migrated to Conan
# These will be handled by the existing build system
macro(DEP_ext_xxhash tgt)
    if(TARGET xxHash::xxhash)
        target_link_libraries(${tgt} PUBLIC xxHash::xxhash)
    endif()
endmacro()

macro(DEP_ext_xxhash_INC tgt)
endmacro()

macro(DEP_ext_xxhash_LIB tgt)
    if(TARGET xxHash::xxhash)
        target_link_libraries(${tgt} PRIVATE xxHash::xxhash)
    endif()
endmacro()

macro(DEP_ext_tz tgt)
    # tz not migrated yet
endmacro()

macro(DEP_ext_tz_INC tgt)
endmacro()

macro(DEP_ext_tz_LIB tgt)
endmacro()

macro(DEP_ext_dwarf tgt)
    # libdwarf not migrated yet
endmacro()

macro(DEP_ext_dwarf_INC tgt)
endmacro()

macro(DEP_ext_dwarf_LIB tgt)
endmacro()

macro(DEP_ext_addr2line tgt)
    # addr2line not migrated yet
endmacro()

macro(DEP_ext_addr2line_INC tgt)
endmacro()

macro(DEP_ext_addr2line_LIB tgt)
endmacro()

macro(DEP_ext_avro tgt)
    if(TARGET avro-c::avro-c)
        target_link_libraries(${tgt} PUBLIC avro-c::avro-c)
    endif()
endmacro()

macro(DEP_ext_avro_INC tgt)
    # Handled by target_link_libraries
endmacro()

macro(DEP_ext_avro_LIB tgt)
    if(TARGET avro-c::avro-c)
        target_link_libraries(${tgt} PRIVATE avro-c::avro-c)
    endif()
endmacro()

macro(DEP_ext_libs3 tgt)
    # libs3 not migrated yet
endmacro()

macro(DEP_ext_libs3_INC tgt)
endmacro()

macro(DEP_ext_libs3_LIB tgt)
endmacro()

macro(DEP_ext_azure tgt)
    # azure not migrated yet
endmacro()

macro(DEP_ext_azure_INC tgt)
endmacro()

macro(DEP_ext_azure_LIB tgt)
endmacro()

macro(DEP_ext_cos tgt)
    # cos not migrated yet
endmacro()

macro(DEP_ext_cos_INC tgt)
endmacro()

macro(DEP_ext_cos_LIB tgt)
endmacro()

macro(DEP_ext_cppstub tgt)
    # cppstub is now available as a Conan package (header-only library)
    if(TARGET cppstub::cppstub)
        target_link_libraries(${tgt} PUBLIC cppstub::cppstub)
    endif()
endmacro()

macro(DEP_ext_cppstub_INC tgt)
    # Header-only library, handled by target_link_libraries above
    if(TARGET cppstub::cppstub)
        target_link_libraries(${tgt} INTERFACE cppstub::cppstub)
    endif()
endmacro()

macro(DEP_ext_cppstub_LIB tgt)
    # Header-only library, no libs to link
endmacro()

macro(DEP_ext_sqlite tgt)
    # sqlite not migrated yet
endmacro()

macro(DEP_ext_sqlite_INC tgt)
endmacro()

macro(DEP_ext_sqlite_LIB tgt)
endmacro()

# Windows-specific stubs
macro(DEP_ext_pthread tgt)
endmacro()

macro(DEP_ext_pthread_INC tgt)
endmacro()

macro(DEP_ext_pthread_LIB tgt)
endmacro()

macro(DEP_ext_iconv tgt)
endmacro()

macro(DEP_ext_iconv_INC tgt)
endmacro()

macro(DEP_ext_iconv_LIB tgt)
endmacro()

macro(DEP_ext_msvcregex tgt)
endmacro()

macro(DEP_ext_msvcregex_INC tgt)
endmacro()

macro(DEP_ext_msvcregex_LIB tgt)
endmacro()

macro(DEP_ext_wcwidth tgt)
endmacro()

macro(DEP_ext_wcwidth_INC tgt)
endmacro()

macro(DEP_ext_wcwidth_LIB tgt)
endmacro()

macro(DEP_ext_wingetopt tgt)
endmacro()

macro(DEP_ext_wingetopt_INC tgt)
endmacro()

macro(DEP_ext_wingetopt_LIB tgt)
endmacro()

macro(DEP_ext_crashdump tgt)
endmacro()

macro(DEP_ext_crashdump_INC tgt)
endmacro()

macro(DEP_ext_crashdump_LIB tgt)
endmacro()

message(STATUS "Conan compatibility layer loaded")
