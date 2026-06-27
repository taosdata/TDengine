if(POLICY CMP0053)
    cmake_policy(SET CMP0053 NEW)
endif()

if(NOT DEFINED ARROW_THIRDPARTY_TOOLCHAIN OR
   "${ARROW_THIRDPARTY_TOOLCHAIN}" STREQUAL "" OR
   NOT EXISTS "${ARROW_THIRDPARTY_TOOLCHAIN}")
    message(FATAL_ERROR
        "patch_arrow_thirdparty_mirror.cmake requires "
        "-DARROW_THIRDPARTY_TOOLCHAIN=<path-to-ThirdpartyToolchain.cmake>")
endif()

if(NOT DEFINED LOCAL_URL OR "${LOCAL_URL}" STREQUAL "")
    message(STATUS "Arrow mirror patch skipped: LOCAL_URL is empty")
    return()
endif()

file(READ "${ARROW_THIRDPARTY_TOOLCHAIN}" _arrow_thirdparty_content)

string(REGEX REPLACE
    "set\\(THIRDPARTY_MIRROR_URL \"[^\"]*\"\\)"
    "set(THIRDPARTY_MIRROR_URL \"${LOCAL_URL}\")"
    _arrow_thirdparty_content "${_arrow_thirdparty_content}")

set(_absl_old [=[
if(DEFINED ENV{ARROW_ABSL_URL})
  set(ABSL_SOURCE_URL "$ENV{ARROW_ABSL_URL}")
else()
  set_urls(ABSL_SOURCE_URL
           "https://github.com/abseil/abseil-cpp/archive/${ARROW_ABSL_BUILD_VERSION}.tar.gz"
  )
endif()
]=])
set(_absl_new_template [=[
# TDENGINE_LOCAL_MIRROR_ABSL_BEGIN
if(DEFINED ENV{ARROW_ABSL_URL})
  set(ABSL_SOURCE_URL "$ENV{ARROW_ABSL_URL}")
else()
  set_urls(ABSL_SOURCE_URL
           "@LOCAL_URL@/absl-${ARROW_ABSL_BUILD_VERSION}.tar.gz"
           "https://github.com/abseil/abseil-cpp/archive/${ARROW_ABSL_BUILD_VERSION}.tar.gz")
endif()
# TDENGINE_LOCAL_MIRROR_ABSL_END
]=])

set(_snappy_old [=[
if(DEFINED ENV{ARROW_SNAPPY_URL})
  set(SNAPPY_SOURCE_URL "$ENV{ARROW_SNAPPY_URL}")
else()
  set_urls(SNAPPY_SOURCE_URL
           "https://github.com/google/snappy/archive/${ARROW_SNAPPY_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/snappy-${ARROW_SNAPPY_BUILD_VERSION}.tar.gz")
endif()
]=])
set(_snappy_new_template [=[
# TDENGINE_LOCAL_MIRROR_SNAPPY_BEGIN
if(DEFINED ENV{ARROW_SNAPPY_URL})
  set(SNAPPY_SOURCE_URL "$ENV{ARROW_SNAPPY_URL}")
else()
  set_urls(SNAPPY_SOURCE_URL
           "@LOCAL_URL@/snappy-${ARROW_SNAPPY_BUILD_VERSION}.tar.gz"
           "https://github.com/google/snappy/archive/${ARROW_SNAPPY_BUILD_VERSION}.tar.gz")
endif()
# TDENGINE_LOCAL_MIRROR_SNAPPY_END
]=])

set(_cares_old [=[
if(DEFINED ENV{ARROW_CARES_URL})
  set(CARES_SOURCE_URL "$ENV{ARROW_CARES_URL}")
else()
  string(REPLACE "." "_" ARROW_CARES_BUILD_VERSION_UNDERSCORES
                 ${ARROW_CARES_BUILD_VERSION})
  set_urls(CARES_SOURCE_URL
           "https://github.com/c-ares/c-ares/releases/download/cares-${ARROW_CARES_BUILD_VERSION_UNDERSCORES}/c-ares-${ARROW_CARES_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/cares-${ARROW_CARES_BUILD_VERSION}.tar.gz")
endif()
]=])
set(_cares_new_template [=[
# TDENGINE_LOCAL_MIRROR_CARES_BEGIN
if(DEFINED ENV{ARROW_CARES_URL})
  set(CARES_SOURCE_URL "$ENV{ARROW_CARES_URL}")
else()
  string(REPLACE "." "_" ARROW_CARES_BUILD_VERSION_UNDERSCORES
                 ${ARROW_CARES_BUILD_VERSION})
  set_urls(CARES_SOURCE_URL
           "@LOCAL_URL@/cares-${ARROW_CARES_BUILD_VERSION}.tar.gz"
           "https://github.com/c-ares/c-ares/releases/download/cares-${ARROW_CARES_BUILD_VERSION_UNDERSCORES}/c-ares-${ARROW_CARES_BUILD_VERSION}.tar.gz")
endif()
# TDENGINE_LOCAL_MIRROR_CARES_END
]=])

set(_gflags_old [=[
if(DEFINED ENV{ARROW_GFLAGS_URL})
  set(GFLAGS_SOURCE_URL "$ENV{ARROW_GFLAGS_URL}")
else()
  set_urls(GFLAGS_SOURCE_URL
           "https://github.com/gflags/gflags/archive/${ARROW_GFLAGS_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/gflags-${ARROW_GFLAGS_BUILD_VERSION}.tar.gz")
endif()
]=])
set(_gflags_new_template [=[
# TDENGINE_LOCAL_MIRROR_GFLAGS_BEGIN
if(DEFINED ENV{ARROW_GFLAGS_URL})
  set(GFLAGS_SOURCE_URL "$ENV{ARROW_GFLAGS_URL}")
else()
  set_urls(GFLAGS_SOURCE_URL
           "@LOCAL_URL@/gflags-${ARROW_GFLAGS_BUILD_VERSION}.tar.gz"
           "https://github.com/gflags/gflags/archive/${ARROW_GFLAGS_BUILD_VERSION}.tar.gz")
endif()
# TDENGINE_LOCAL_MIRROR_GFLAGS_END
]=])

set(_grpc_old [=[
if(DEFINED ENV{ARROW_GRPC_URL})
  set(GRPC_SOURCE_URL "$ENV{ARROW_GRPC_URL}")
else()
  set_urls(GRPC_SOURCE_URL
           "https://github.com/grpc/grpc/archive/${ARROW_GRPC_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/grpc-${ARROW_GRPC_BUILD_VERSION}.tar.gz")
endif()
]=])
set(_grpc_new_template [=[
# TDENGINE_LOCAL_MIRROR_GRPC_BEGIN
if(DEFINED ENV{ARROW_GRPC_URL})
  set(GRPC_SOURCE_URL "$ENV{ARROW_GRPC_URL}")
else()
  set_urls(GRPC_SOURCE_URL
           "@LOCAL_URL@/grpc-${ARROW_GRPC_BUILD_VERSION}.tar.gz"
           "https://github.com/grpc/grpc/archive/${ARROW_GRPC_BUILD_VERSION}.tar.gz")
endif()
# TDENGINE_LOCAL_MIRROR_GRPC_END
]=])

set(_mimalloc_old [=[
if(DEFINED ENV{ARROW_MIMALLOC_URL})
  set(MIMALLOC_SOURCE_URL "$ENV{ARROW_MIMALLOC_URL}")
else()
  set_urls(MIMALLOC_SOURCE_URL
           "https://github.com/microsoft/mimalloc/archive/${ARROW_MIMALLOC_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/mimalloc-${ARROW_MIMALLOC_BUILD_VERSION}.tar.gz")
endif()
]=])
set(_mimalloc_new_template [=[
# TDENGINE_LOCAL_MIRROR_MIMALLOC_BEGIN
if(DEFINED ENV{ARROW_MIMALLOC_URL})
  set(MIMALLOC_SOURCE_URL "$ENV{ARROW_MIMALLOC_URL}")
else()
  set_urls(MIMALLOC_SOURCE_URL
           "@LOCAL_URL@/mimalloc-${ARROW_MIMALLOC_BUILD_VERSION}.tar.gz"
           "https://github.com/microsoft/mimalloc/archive/${ARROW_MIMALLOC_BUILD_VERSION}.tar.gz")
endif()
# TDENGINE_LOCAL_MIRROR_MIMALLOC_END
]=])

set(_protobuf_old [=[
if(DEFINED ENV{ARROW_PROTOBUF_URL})
  set(PROTOBUF_SOURCE_URL "$ENV{ARROW_PROTOBUF_URL}")
else()
  string(SUBSTRING ${ARROW_PROTOBUF_BUILD_VERSION} 1 -1
                   ARROW_PROTOBUF_STRIPPED_BUILD_VERSION)
  # strip the leading `v`
  set_urls(PROTOBUF_SOURCE_URL
           "https://github.com/protocolbuffers/protobuf/releases/download/${ARROW_PROTOBUF_BUILD_VERSION}/protobuf-all-${ARROW_PROTOBUF_STRIPPED_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/protobuf-${ARROW_PROTOBUF_BUILD_VERSION}.tar.gz")
endif()
]=])
set(_protobuf_new_template [=[
# TDENGINE_LOCAL_MIRROR_PROTOBUF_BEGIN
if(DEFINED ENV{ARROW_PROTOBUF_URL})
  set(PROTOBUF_SOURCE_URL "$ENV{ARROW_PROTOBUF_URL}")
else()
  string(SUBSTRING ${ARROW_PROTOBUF_BUILD_VERSION} 1 -1
                   ARROW_PROTOBUF_STRIPPED_BUILD_VERSION)
  # strip the leading `v`
  set_urls(PROTOBUF_SOURCE_URL
           "@LOCAL_URL@/protobuf-${ARROW_PROTOBUF_BUILD_VERSION}.tar.gz"
           "https://github.com/protocolbuffers/protobuf/releases/download/${ARROW_PROTOBUF_BUILD_VERSION}/protobuf-all-${ARROW_PROTOBUF_STRIPPED_BUILD_VERSION}.tar.gz")
endif()
# TDENGINE_LOCAL_MIRROR_PROTOBUF_END
]=])

set(_re2_old [=[
if(DEFINED ENV{ARROW_RE2_URL})
  set(RE2_SOURCE_URL "$ENV{ARROW_RE2_URL}")
else()
  set_urls(RE2_SOURCE_URL
           "https://github.com/google/re2/archive/${ARROW_RE2_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/re2-${ARROW_RE2_BUILD_VERSION}.tar.gz")
endif()
]=])
set(_re2_new_template [=[
# TDENGINE_LOCAL_MIRROR_RE2_BEGIN
if(DEFINED ENV{ARROW_RE2_URL})
  set(RE2_SOURCE_URL "$ENV{ARROW_RE2_URL}")
else()
  set_urls(RE2_SOURCE_URL
           "@LOCAL_URL@/re2-${ARROW_RE2_BUILD_VERSION}.tar.gz"
           "https://github.com/google/re2/archive/${ARROW_RE2_BUILD_VERSION}.tar.gz")
endif()
# TDENGINE_LOCAL_MIRROR_RE2_END
]=])

set(_thrift_old [=[
if(DEFINED ENV{ARROW_THRIFT_URL})
  set(THRIFT_SOURCE_URL "$ENV{ARROW_THRIFT_URL}")
else()
  set(THRIFT_SOURCE_URL
      "https://www.apache.org/dyn/closer.lua/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz?action=download"
      "https://dlcdn.apache.org/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
  )
endif()
]=])
set(_thrift_new_template [=[
# TDENGINE_LOCAL_MIRROR_THRIFT_BEGIN
if(DEFINED ENV{ARROW_THRIFT_URL})
  set(THRIFT_SOURCE_URL "$ENV{ARROW_THRIFT_URL}")
else()
  set(THRIFT_SOURCE_URL
      "@LOCAL_URL@/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
      "https://www.apache.org/dyn/closer.lua/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz?action=download"
      "https://dlcdn.apache.org/thrift/${ARROW_THRIFT_BUILD_VERSION}/thrift-${ARROW_THRIFT_BUILD_VERSION}.tar.gz"
  )
endif()
# TDENGINE_LOCAL_MIRROR_THRIFT_END
]=])

set(_zlib_old [=[
if(DEFINED ENV{ARROW_ZLIB_URL})
  set(ZLIB_SOURCE_URL "$ENV{ARROW_ZLIB_URL}")
else()
  set_urls(ZLIB_SOURCE_URL
           "https://zlib.net/fossils/zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz"
           "${THIRDPARTY_MIRROR_URL}/zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz")
endif()
]=])
set(_zlib_new_template [=[
# TDENGINE_LOCAL_MIRROR_ZLIB_BEGIN
if(DEFINED ENV{ARROW_ZLIB_URL})
  set(ZLIB_SOURCE_URL "$ENV{ARROW_ZLIB_URL}")
else()
  set_urls(ZLIB_SOURCE_URL
           "@LOCAL_URL@/zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz"
           "https://zlib.net/fossils/zlib-${ARROW_ZLIB_BUILD_VERSION}.tar.gz")
endif()
# TDENGINE_LOCAL_MIRROR_ZLIB_END
]=])

set(_absl_new "${_absl_new_template}")
set(_snappy_new "${_snappy_new_template}")
set(_cares_new "${_cares_new_template}")
set(_gflags_new "${_gflags_new_template}")
set(_grpc_new "${_grpc_new_template}")
set(_mimalloc_new "${_mimalloc_new_template}")
set(_protobuf_new "${_protobuf_new_template}")
set(_re2_new "${_re2_new_template}")
set(_thrift_new "${_thrift_new_template}")
set(_zlib_new "${_zlib_new_template}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _absl_new "${_absl_new}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _snappy_new "${_snappy_new}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _cares_new "${_cares_new}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _gflags_new "${_gflags_new}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _grpc_new "${_grpc_new}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _mimalloc_new "${_mimalloc_new}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _protobuf_new "${_protobuf_new}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _re2_new "${_re2_new}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _thrift_new "${_thrift_new}")
string(REPLACE "@LOCAL_URL@" "${LOCAL_URL}" _zlib_new "${_zlib_new}")

function(_replace_arrow_block content_var old_block begin_marker end_marker new_block)
    set(_content "${${content_var}}")
    string(FIND "${_content}" "${old_block}" _old_pos)
    if(NOT _old_pos EQUAL -1)
        string(REPLACE "${old_block}" "${new_block}" _content "${_content}")
        set(${content_var} "${_content}" PARENT_SCOPE)
        return()
    endif()

    string(FIND "${_content}" "${begin_marker}" _begin_pos)
    string(FIND "${_content}" "${end_marker}" _end_pos)
    if(_begin_pos EQUAL -1 OR _end_pos EQUAL -1)
        message(FATAL_ERROR
            "Arrow third-party mirror patch failed: expected block not found in "
            "${ARROW_THIRDPARTY_TOOLCHAIN}")
    endif()

    string(LENGTH "${end_marker}" _end_marker_len)
    math(EXPR _suffix_pos "${_end_pos} + ${_end_marker_len}")
    string(SUBSTRING "${_content}" 0 ${_begin_pos} _prefix)
    string(SUBSTRING "${_content}" ${_suffix_pos} -1 _suffix)
    set(${content_var} "${_prefix}${new_block}${_suffix}" PARENT_SCOPE)
endfunction()

_replace_arrow_block(
    _arrow_thirdparty_content
    "${_snappy_old}"
    "# TDENGINE_LOCAL_MIRROR_SNAPPY_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_SNAPPY_END"
    "${_snappy_new}"
)
_replace_arrow_block(
    _arrow_thirdparty_content
    "${_absl_old}"
    "# TDENGINE_LOCAL_MIRROR_ABSL_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_ABSL_END"
    "${_absl_new}"
)
_replace_arrow_block(
    _arrow_thirdparty_content
    "${_cares_old}"
    "# TDENGINE_LOCAL_MIRROR_CARES_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_CARES_END"
    "${_cares_new}"
)
_replace_arrow_block(
    _arrow_thirdparty_content
    "${_gflags_old}"
    "# TDENGINE_LOCAL_MIRROR_GFLAGS_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_GFLAGS_END"
    "${_gflags_new}"
)
_replace_arrow_block(
    _arrow_thirdparty_content
    "${_grpc_old}"
    "# TDENGINE_LOCAL_MIRROR_GRPC_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_GRPC_END"
    "${_grpc_new}"
)
_replace_arrow_block(
    _arrow_thirdparty_content
    "${_mimalloc_old}"
    "# TDENGINE_LOCAL_MIRROR_MIMALLOC_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_MIMALLOC_END"
    "${_mimalloc_new}"
)
_replace_arrow_block(
    _arrow_thirdparty_content
    "${_protobuf_old}"
    "# TDENGINE_LOCAL_MIRROR_PROTOBUF_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_PROTOBUF_END"
    "${_protobuf_new}"
)
_replace_arrow_block(
    _arrow_thirdparty_content
    "${_re2_old}"
    "# TDENGINE_LOCAL_MIRROR_RE2_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_RE2_END"
    "${_re2_new}"
)
_replace_arrow_block(
    _arrow_thirdparty_content
    "${_thrift_old}"
    "# TDENGINE_LOCAL_MIRROR_THRIFT_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_THRIFT_END"
    "${_thrift_new}"
)
_replace_arrow_block(
    _arrow_thirdparty_content
    "${_zlib_old}"
    "# TDENGINE_LOCAL_MIRROR_ZLIB_BEGIN"
    "# TDENGINE_LOCAL_MIRROR_ZLIB_END"
    "${_zlib_new}"
)

file(WRITE "${ARROW_THIRDPARTY_TOOLCHAIN}" "${_arrow_thirdparty_content}")
message(STATUS
    "Patched Arrow third-party downloads to use mirror ${LOCAL_URL}")
