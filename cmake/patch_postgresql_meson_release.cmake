if(NOT DEFINED POSTGRESQL_SOURCE_DIR)
    message(FATAL_ERROR "patch_postgresql_meson_release.cmake requires -DPOSTGRESQL_SOURCE_DIR=<path>")
endif()

set(_meson_build "${POSTGRESQL_SOURCE_DIR}/meson.build")
if(NOT EXISTS "${_meson_build}")
    message(FATAL_ERROR "PostgreSQL meson.build not found: ${_meson_build}")
endif()

file(READ "${_meson_build}" _content)
string(REPLACE
    "if conflicting_files.length() > 0"
    "if false # patched: release tarballs include generated files that conflict with Meson out-of-source checks"
    _patched
    "${_content}")

if(_patched STREQUAL _content)
    message(FATAL_ERROR "Failed to patch PostgreSQL Meson release-tarball conflict check")
endif()

file(WRITE "${_meson_build}" "${_patched}")
