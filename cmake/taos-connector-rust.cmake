# Processing taos-connector-rust compilation
message(STATUS "")
message(STATUS "Processing taos-connector-rust compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taos-connector-rust build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_RUST              = ${BUILD_RUST}")
message(STATUS "  CMAKE_BUILD_TYPE        = ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_CONNECTOR_RUST_DIR   = ${TD_CONNECTOR_RUST_DIR}")
message(STATUS "  CMAKE_BINARY_DIR        = ${CMAKE_BINARY_DIR}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "===============================================")
message(STATUS "")

# Verify connector-rust repo exists
if(NOT EXISTS "${TD_CONNECTOR_RUST_DIR}/Cargo.toml")
  message(FATAL_ERROR "TD_CONNECTOR_RUST_DIR is not a taos-connector-rust repo: ${TD_CONNECTOR_RUST_DIR}")
endif()

find_program(CARGO_EXECUTABLE cargo REQUIRED)

file(GLOB_RECURSE _connector_rust_sources CONFIGURE_DEPENDS
  "${TD_CONNECTOR_RUST_DIR}/*.rs"
)

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-connector-rust) ─
set(_rust_base_dir "${CMAKE_BINARY_DIR}/build/taos-connector-rust")
set(_rust_target_dir "${_rust_base_dir}/target")
set(_rust_output_dir "${_rust_base_dir}/output")

# ── Map CMAKE_BUILD_TYPE to a cargo profile ────────────────────────────────
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(_cargo_build_args build --target-dir "${_rust_target_dir}")
  set(_cargo_profile_dir "debug")
  set(_cargo_profile_label "debug")
else()
  # Release / RelWithDebInfo / MinSizeRel all map to cargo --release
  set(_cargo_build_args build --release --target-dir "${_rust_target_dir}")
  set(_cargo_profile_dir "release")
  set(_cargo_profile_label "release")
endif()

set(_rust_artifact_dir "${_rust_target_dir}/${_cargo_profile_dir}")

# ── Output artifact paths ──────────────────────────────────────────────────
# On Windows, Rust cdylib produces taosws.dll (no lib prefix)
# On Linux/macOS, Rust cdylib produces libtaosws.so / libtaosws.dylib
if(WIN32)
  set(_taosws_artifact_name "taosws${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
  set(_taosws_artifact_name "libtaosws${CMAKE_SHARED_LIBRARY_SUFFIX}")
endif()
set(_taosws_so "${_rust_output_dir}/${_taosws_artifact_name}")
set(_taosws_a  "${_rust_output_dir}/${CMAKE_STATIC_LIBRARY_PREFIX}taosws${CMAKE_STATIC_LIBRARY_SUFFIX}")

message(STATUS "Rust build config:")
message(STATUS "  base dir                = ${_rust_base_dir}")
message(STATUS "  cargo target dir        = ${_rust_target_dir}")
message(STATUS "  cargo profile           = ${_cargo_profile_label}")
message(STATUS "  artifact dir            = ${_rust_artifact_dir}")
message(STATUS "  output dir              = ${_rust_output_dir}")
message(STATUS "  shared lib              = ${_taosws_so}")
message(STATUS "  static lib              = ${_taosws_a}")

add_custom_command(
  OUTPUT "${_taosws_so}" "${_taosws_a}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_rust_target_dir}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_rust_output_dir}"
  COMMAND "${CMAKE_COMMAND}" -E env
          "CARGO_TARGET_DIR=${_rust_target_dir}"
          "${CARGO_EXECUTABLE}" ${_cargo_build_args}
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_rust_artifact_dir}/${_taosws_artifact_name}"
          "${_taosws_so}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_rust_artifact_dir}/${CMAKE_STATIC_LIBRARY_PREFIX}taosws${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${_taosws_a}"
  WORKING_DIRECTORY "${TD_CONNECTOR_RUST_DIR}"
  DEPENDS "${TD_CONNECTOR_RUST_DIR}/Cargo.toml"
          "${TD_CONNECTOR_RUST_DIR}/Cargo.lock"
          ${_connector_rust_sources}
  VERBATIM
  COMMENT "Building taos-connector-rust (${_cargo_profile_label}) → ${_rust_output_dir}"
)

add_custom_target(taos_connector_rust ALL
  DEPENDS "${_taosws_so}" "${_taosws_a}"
)
