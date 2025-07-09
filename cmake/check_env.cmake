# NOTE: if you change this option later, that'll NOT effect unless you remove CMakeCache.txt beforehand
option(TD_CHECK_SYSTEM_EXTERNALS "if check and externals installed on the system or not" OFF)

if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  if(NOT BREW_PREFIX)
      execute_process(COMMAND brew --prefix OUTPUT_VARIABLE BREW_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE)
      set(BREW_PREFIX "${BREW_PREFIX}" CACHE STRING "Homebrew installation prefix")
  endif()
  message(STATUS "Homebrew prefix: ${BREW_PREFIX}")
endif()

macro(check_lib)
  set(options)
  set(oneValueArgs NAME HEADER_FILE LIBNAME SYMBOL)
  set(multiValueArgs ACCOMPANIES)
  cmake_parse_arguments(arg_check_lib
      "${options}" "${oneValueArgs}" "${multiValueArgs}"
      ${ARGN}
  )
  if(NOT DEFINED arg_check_lib_NAME)
    message(FATAL_ERROR "`NAME` must be set")
  endif()
  if(NOT DEFINED arg_check_lib_HEADER_FILE)
    message(FATAL_ERROR "`HEADER_FILE` must be set")
  endif()
  if(NOT DEFINED arg_check_lib_LIBNAME)
    message(FATAL_ERROR "`LIBNAME` must be set")
  endif()
  if(NOT DEFINED arg_check_lib_SYMBOL)
    message(FATAL_ERROR "`SYMBOL` must be set")
  endif()
  set(_name     ${arg_check_lib_NAME})
  set(_hdr      ${arg_check_lib_HEADER_FILE})
  set(_lib      ${arg_check_lib_LIBNAME})
  set(_symbol   ${arg_check_lib_SYMBOL})

  if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
    set(CMAKE_REQUIRED_FLAGS "-I${BREW_PREFIX}/include -L${BREW_PREFIX}/lib")
  endif()

  check_library_exists(${_lib} ${_symbol} "" HAVE_LIB_${_name})
  set(${_name}_LIBNAMES         "")
  if(${HAVE_LIB_${_name}})
    set(CMAKE_REQUIRED_LIBRARIES ${_lib})
    check_symbol_exists(${_symbol} ${_hdr} HAVE_DEV_${_name})
    set(CMAKE_REQUIRED_LIBRARIES)
    if(${HAVE_DEV_${_name}})
      set(${_name}_LIBNAMES         "${_lib}" "${arg_check_lib_ACCOMPANIES}")
    endif()
  endif()

  if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
    # cleanup
    set(CMAKE_REQUIRED_FLAGS)
  endif()
endmacro()

if(NOT TD_CHECK_SYSTEM_EXTERNALS)
  message(STATUS "does not check and use externals installed on the system")
  return()
endif()

# sudo apt install libgeos-dev
check_lib(
  NAME             GEOS
  HEADER_FILE      geos_c.h
  LIBNAME          geos_c
  SYMBOL           GEOS_init_r
)
message(STATUS "HAVE_LIB_GEOS:${HAVE_LIB_GEOS}")
message(STATUS "HAVE_DEV_GEOS:${HAVE_DEV_GEOS}")

# sudo apt install libjansson-dev
check_lib(
  NAME             JANSSON
  HEADER_FILE      jansson.h
  LIBNAME          jansson.so
  SYMBOL           jansson_version_str
)
message(STATUS "HAVE_LIB_JANSSON:${HAVE_LIB_JANSSON}")
message(STATUS "HAVE_DEV_JANSSON:${HAVE_DEV_JANSSON}")

# sudo apt install liblzma-dev
check_lib(
  NAME             LZMA
  HEADER_FILE      lzma.h
  LIBNAME          lzma
  SYMBOL           lzma_version_number
)
message(STATUS "HAVE_LIB_LZMA:${HAVE_LIB_LZMA}")
message(STATUS "HAVE_DEV_LZMA:${HAVE_DEV_LZMA}")

# sudo apt install libsnappy-dev
check_lib(
  NAME             SNAPPY
  HEADER_FILE      snappy-c.h
  LIBNAME          snappy
  SYMBOL           snappy_compress
)
message(STATUS "HAVE_LIB_SNAPPY:${HAVE_LIB_SNAPPY}")
message(STATUS "HAVE_DEV_SNAPPY:${HAVE_DEV_SNAPPY}")

# sudo apt install libssl-dev
check_lib(
  NAME             SSL
  HEADER_FILE      openssl/ssl.h
  LIBNAME          ssl
  # SYMBOL           SSL_CTX_get_options
  SYMBOL           SSL_get_version
  ACCOMPANIES      crypto
)
message(STATUS "HAVE_LIB_SSL:${HAVE_LIB_SSL}")
message(STATUS "HAVE_DEV_SSL:${HAVE_DEV_SSL}")

# sudo apt install zlib1g-dev
check_lib(
  NAME             ZLIB
  HEADER_FILE      zlib.h
  LIBNAME          z
  SYMBOL           zlibVersion
)
message(STATUS "HAVE_LIB_ZLIB:${HAVE_LIB_ZLIB}")
message(STATUS "HAVE_DEV_ZLIB:${HAVE_DEV_ZLIB}")

# sudo apt install libzstd-dev
check_lib(
  NAME             ZSTD
  HEADER_FILE      zstd.h
  LIBNAME          zstd
  SYMBOL           ZSTD_versionNumber
)
message(STATUS "HAVE_LIB_ZSTD:${HAVE_LIB_ZSTD}")
message(STATUS "HAVE_DEV_ZSTD:${HAVE_DEV_ZSTD}")

# sudo apt install liblz4-dev
check_lib(
  NAME             LZ4
  HEADER_FILE      lz4.h
  LIBNAME          lz4
  SYMBOL           LZ4_versionNumber
)
message(STATUS "HAVE_LIB_LZ4:${HAVE_LIB_LZ4}")
message(STATUS "HAVE_DEV_LZ4:${HAVE_DEV_LZ4}")

# sudo apt install libuv1-dev
check_lib(
  NAME             LIBUV
  HEADER_FILE      uv.h
  LIBNAME          uv
  SYMBOL           uv_version
)
message(STATUS "HAVE_LIB_LIBUV:${HAVE_LIB_LIBUV}")
message(STATUS "HAVE_DEV_LIBUV:${HAVE_DEV_LIBUV}")

# sudo apt install libsqlite3-dev
check_lib(
  NAME             SQLITE3
  HEADER_FILE      sqlite3.h
  LIBNAME          sqlite3
  SYMBOL           sqlite3_libversion
)
message(STATUS "HAVE_LIB_SQLITE3:${HAVE_LIB_SQLITE3}")
message(STATUS "HAVE_DEV_SQLITE3:${HAVE_DEV_SQLITE3}")

# sudo apt install libcurl4-openssl-dev
check_lib(
  NAME             CURL4_OPENSSL
  HEADER_FILE      curl/curl.h
  LIBNAME          curl
  SYMBOL           curl_version
)
message(STATUS "HAVE_LIB_CURL4_OPENSSL:${HAVE_LIB_CURL4_OPENSSL}")
message(STATUS "HAVE_DEV_CURL4_OPENSSL:${HAVE_DEV_CURL4_OPENSSL}")

# sudo apt install libavro-dev
check_lib(
  NAME             AVRO
  HEADER_FILE      avro.h
  LIBNAME          avro
  SYMBOL           avro_generic_class_from_schema
)
message(STATUS "HAVE_LIB_AVRO:${HAVE_LIB_AVRO}")
message(STATUS "HAVE_DEV_AVRO:${HAVE_DEV_AVRO}")

