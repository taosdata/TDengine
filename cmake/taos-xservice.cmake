# Processing taosx compilation
message(STATUS "Processing taosx compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taosx build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_TAOSX             = ${BUILD_TAOSX}")
message(STATUS "  CMAKE_BUILD_TYPE        = ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_TAOSX_DIR            = ${TD_TAOSX_DIR}")
message(STATUS "  CMAKE_BINARY_DIR        = ${CMAKE_BINARY_DIR}")
message(STATUS "")
message(STATUS "Input version variables:")
message(STATUS "  BUILD_VER_NUMBER        = ${BUILD_VER_NUMBER}")
message(STATUS "  BUILD_CUS_NAME          = ${BUILD_CUS_NAME}")
message(STATUS "  BUILD_CUS_PROMPT        = ${BUILD_CUS_PROMPT}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "  CMAKE_EXECUTABLE_SUFFIX = ${CMAKE_EXECUTABLE_SUFFIX}")
message(STATUS "====================================")
message(STATUS "")

# Verify taosx repo exists
if(NOT EXISTS "${TD_TAOSX_DIR}/Cargo.toml")
  message(FATAL_ERROR "TD_TAOSX_DIR is not a taos-xservice repo: ${TD_TAOSX_DIR}")
endif()

find_program(CARGO_EXECUTABLE cargo REQUIRED)

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-xservice) ───
set(_taosx_base_dir "${CMAKE_BINARY_DIR}/build/taos-xservice")
set(_taosx_target_dir "${_taosx_base_dir}/target")
set(_taosx_output_dir "${_taosx_base_dir}/output")

# ── Map CMAKE_BUILD_TYPE to a cargo profile ────────────────────────────────
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(_taosx_cargo_profile "dev")
  set(_taosx_cargo_profile_dir "debug")
  set(_taosx_cargo_profile_args "")
else()
  set(_taosx_cargo_profile "release")
  set(_taosx_cargo_profile_dir "release")
  set(_taosx_cargo_profile_args "--profile;release")
endif()

set(_taosx_artifact_dir "${_taosx_target_dir}/${_taosx_cargo_profile_dir}")

# ── Output artifact paths (same as taosd: build/bin) ───────────────────────
set(_taosx_bin_output_dir "${CMAKE_BINARY_DIR}/build/bin")
set(_taosx_binary_output "${_taosx_bin_output_dir}/taosx${CMAKE_EXECUTABLE_SUFFIX}")
set(_taosx_agent_binary_output "${_taosx_bin_output_dir}/taosx-agent${CMAKE_EXECUTABLE_SUFFIX}")
set(_taosx_explorer_binary_output "${_taosx_bin_output_dir}/taos-explorer${CMAKE_EXECUTABLE_SUFFIX}")

# ── Explorer frontend paths ────────────────────────────────────────────────
set(_explorer_ui_dir "${TD_TAOSX_DIR}/explorer")
set(_explorer_dist_dir "${_explorer_ui_dir}/dist")

message(STATUS "taosx build config:")
message(STATUS "  base dir                = ${_taosx_base_dir}")
message(STATUS "  cargo target dir        = ${_taosx_target_dir}")
message(STATUS "  cargo profile           = ${_taosx_cargo_profile}")
message(STATUS "  artifact dir            = ${_taosx_artifact_dir}")
message(STATUS "  output dir              = ${_taosx_output_dir}")
message(STATUS "  taosx binary            = ${_taosx_binary_output}")
message(STATUS "  taosx-agent binary      = ${_taosx_agent_binary_output}")
message(STATUS "  taos-explorer binary    = ${_taosx_explorer_binary_output}")

file(GLOB_RECURSE _taosx_rust_sources CONFIGURE_DEPENDS
  "${TD_TAOSX_DIR}/src/*.rs"
  "${TD_TAOSX_DIR}/crates/**/*.rs"
  "${TD_TAOSX_DIR}/taosx-agent/**/*.rs"
  "${TD_TAOSX_DIR}/taosx-core/**/*.rs"
  "${TD_TAOSX_DIR}/taosx-ipc/**/*.rs"
  "${TD_TAOSX_DIR}/taosx-metrics/**/*.rs"
  "${TD_TAOSX_DIR}/explorer/server/**/*.rs"
)

set(_taosx_dep_files
  ${_taosx_rust_sources}
  "${TD_TAOSX_DIR}/Cargo.toml"
  "${TD_TAOSX_DIR}/Cargo.lock"
  "${TD_TAOSX_DIR}/explorer/server/Cargo.toml"
)

# ── Build explorer frontend UI ──────────────────────────────────────────────
option(BUILD_EXPLORER_UI "Build taos-explorer frontend with pnpm" ON)

if(BUILD_EXPLORER_UI)
  find_program(PNPM_EXECUTABLE pnpm REQUIRED)
  add_custom_command(
    OUTPUT "${_explorer_dist_dir}/index.html"
    COMMAND "${PNPM_EXECUTABLE}" install --frozen-lockfile
    COMMAND "${PNPM_EXECUTABLE}" run build
    WORKING_DIRECTORY "${_explorer_ui_dir}"
    COMMENT "Building taos-explorer frontend UI"
    VERBATIM
  )
else()
  add_custom_command(
    OUTPUT "${_explorer_dist_dir}/index.html"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${_explorer_dist_dir}"
    COMMAND "${CMAKE_COMMAND}" -E touch "${_explorer_dist_dir}/index.html"
    COMMENT "Creating empty explorer dist placeholder (BUILD_EXPLORER_UI=OFF)"
    VERBATIM
  )
endif()

add_custom_target(explorer_ui
  DEPENDS "${_explorer_dist_dir}/index.html"
)

# ── Build taosx, taosx-agent, taos-explorer (cargo) ────────────────────────
add_custom_command(
  OUTPUT "${_taosx_binary_output}" "${_taosx_agent_binary_output}" "${_taosx_explorer_binary_output}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_target_dir}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_bin_output_dir}"
  COMMAND "${CMAKE_COMMAND}" -E env
          "CARGO_TARGET_DIR=${_taosx_target_dir}"
          "CUS_PROMPT=${BUILD_CUS_PROMPT}"
          "CUS_NAME=${BUILD_CUS_NAME}"
          "VER_NUMBER=${BUILD_VER_NUMBER}"
          "${CARGO_EXECUTABLE}" build -p taosx ${_taosx_cargo_profile_args}
                                --target-dir "${_taosx_target_dir}"
  COMMAND "${CMAKE_COMMAND}" -E env
          "CARGO_TARGET_DIR=${_taosx_target_dir}"
          "CUS_PROMPT=${BUILD_CUS_PROMPT}"
          "CUS_NAME=${BUILD_CUS_NAME}"
          "VER_NUMBER=${BUILD_VER_NUMBER}"
          "${CARGO_EXECUTABLE}" build -p taosx-agent ${_taosx_cargo_profile_args}
                                --target-dir "${_taosx_target_dir}"
  COMMAND "${CMAKE_COMMAND}" -E env
          "CARGO_TARGET_DIR=${_taosx_target_dir}"
          "CUS_PROMPT=${BUILD_CUS_PROMPT}"
          "CUS_NAME=${BUILD_CUS_NAME}"
          "VER_NUMBER=${BUILD_VER_NUMBER}"
          "${CARGO_EXECUTABLE}" build -p taos-explorer ${_taosx_cargo_profile_args}
                                --target-dir "${_taosx_target_dir}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_taosx_artifact_dir}/taosx${CMAKE_EXECUTABLE_SUFFIX}"
          "${_taosx_binary_output}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_taosx_artifact_dir}/taosx-agent${CMAKE_EXECUTABLE_SUFFIX}"
          "${_taosx_agent_binary_output}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_taosx_artifact_dir}/taos-explorer${CMAKE_EXECUTABLE_SUFFIX}"
          "${_taosx_explorer_binary_output}"
  WORKING_DIRECTORY "${TD_TAOSX_DIR}"
  DEPENDS ${_taosx_dep_files}
  VERBATIM
  COMMENT "Building taosx, taosx-agent, and taos-explorer (${_taosx_cargo_profile}) → ${_taosx_bin_output_dir}"
)

add_custom_target(taosx ALL
  DEPENDS "${_taosx_binary_output}" "${_taosx_agent_binary_output}" "${_taosx_explorer_binary_output}"
)
add_dependencies(taosx explorer_ui)
