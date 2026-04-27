# patch_arrow_grpc_ssl.cmake
# Called as: cmake -DZLIB_INSTALL=<path> -P patch_arrow_grpc_ssl.cmake <ThirdpartyToolchain.cmake>
# On Windows, Arrow passes -DgRPC_SSL_PROVIDER=package (requires OpenSSL) to gRPC's cmake,
# and does NOT pass ZLIB paths. This patch:
#   1. Changes gRPC_SSL_PROVIDER to boringssl (gRPC's bundled SSL)
#   2. Injects ZLIB_ROOT / ZLIB_INCLUDE_DIR / ZLIB_LIBRARY into gRPC cmake args
#   3. Removes OpenSSL::SSL / OpenSSL::Crypto from Arrow target_link_libraries

if(NOT CMAKE_ARGV3)
    message(FATAL_ERROR "Usage: cmake -DZLIB_INSTALL=<path> -P patch_arrow_grpc_ssl.cmake <file>")
endif()

set(_file "${CMAKE_ARGV3}")
if(NOT EXISTS "${_file}")
    message(STATUS "[patch] ${_file} not found - skipping")
    return()
endif()

file(READ "${_file}" _content)

string(FIND "${_content}" "# [patched: grpc-boringssl-v3]" _done)
if(NOT _done EQUAL -1)
    message(STATUS "[patch] already patched - skipping")
    return()
endif()

# 1. SSL -> BoringSSL
string(REPLACE "-DgRPC_SSL_PROVIDER=package" "-DgRPC_SSL_PROVIDER=boringssl" _content "${_content}")

# 2. Inject ZLIB paths
if(ZLIB_INSTALL)
    if(EXISTS "${ZLIB_INSTALL}/lib/zlibstaticd.lib")
        set(_zlib_lib "${ZLIB_INSTALL}/lib/zlibstaticd.lib")
    elseif(EXISTS "${ZLIB_INSTALL}/lib/zlibstatic.lib")
        set(_zlib_lib "${ZLIB_INSTALL}/lib/zlibstatic.lib")
    else()
        set(_zlib_lib "${ZLIB_INSTALL}/lib/zlib.lib")
    endif()
    string(REPLACE "-DgRPC_ZLIB_PROVIDER=package"
        "-DgRPC_ZLIB_PROVIDER=package\n      -DZLIB_ROOT=${ZLIB_INSTALL}\n      -DZLIB_INCLUDE_DIR=${ZLIB_INSTALL}/include\n      -DZLIB_LIBRARY=${_zlib_lib}"
        _content "${_content}")
    message(STATUS "[patch] Injected ZLIB_ROOT=${ZLIB_INSTALL}")
endif()

# 3. Remove OpenSSL link deps
string(REPLACE " OpenSSL::SSL" "" _content "${_content}")
string(REPLACE " OpenSSL::Crypto" "" _content "${_content}")
string(REPLACE "OpenSSL::SSL " "" _content "${_content}")
string(REPLACE "OpenSSL::Crypto " "" _content "${_content}")

string(APPEND _content "\n# [patched: grpc-boringssl-v3]\n")
file(WRITE "${_file}" "${_content}")
message(STATUS "[patch] Done: SSL=BoringSSL, ZLIB injected, OpenSSL links removed")
