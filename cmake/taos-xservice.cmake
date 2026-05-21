# Processing taosx compilation
message(STATUS "Processing taosx compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taosx build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_TAOSX             = ${BUILD_TAOSX}")
message(STATUS "  BUILD_TAOSX_BINARY      = ${BUILD_TAOSX_BINARY}")
message(STATUS "  BUILD_TAOSX_AGENT       = ${BUILD_TAOSX_AGENT}")
message(STATUS "  BUILD_EXPLORER          = ${BUILD_EXPLORER}")
message(STATUS "  BUILD_EXPLORER_UI       = ${BUILD_EXPLORER_UI}")
message(STATUS "  BUILD_EXPLORER_DOCS     = ${BUILD_EXPLORER_DOCS}")
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
message(STATUS "  BUILD_CUS_EMAIL         = ${BUILD_CUS_EMAIL}")
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

# Derive convenience flag: any Rust binary needs building
set(_taosx_need_binaries OFF)
if(BUILD_TAOSX_BINARY OR BUILD_TAOSX_AGENT OR BUILD_EXPLORER)
  set(_taosx_need_binaries ON)
endif()

# Multi-config generators (VS, Xcode, Ninja Multi-Config) are unsupported for
# Rust binary builds because deploy staging needs a deterministic build type.
if(_taosx_need_binaries AND CMAKE_CONFIGURATION_TYPES)
  message(FATAL_ERROR "taosx staged build requires a single-config generator so Debug and Release deploy behavior stay deterministic.")
endif()

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-xservice) ───
set(_taosx_base_dir "${CMAKE_BINARY_DIR}/build/taos-xservice")
set(_taosx_target_dir "${_taosx_base_dir}/target")
set(_taosx_deploy_dir "${_taosx_base_dir}/target/deploy")
set(_taosx_output_dir "${_taosx_base_dir}/output")

# ── Map BUILD_TAOSX_PROFILE to a cargo profile ────────────────────────────
set(_taosx_effective_profile "${BUILD_TAOSX_PROFILE}")
if(BUILD_TAOSX_PROFILE STREQUAL "dev")
  set(_taosx_cargo_profile "dev")
  set(_taosx_cargo_profile_dir "debug")
  set(_taosx_cargo_profile_args "")
elseif(BUILD_TAOSX_PROFILE STREQUAL "release")
  set(_taosx_cargo_profile "release")
  set(_taosx_cargo_profile_dir "release")
  set(_taosx_cargo_profile_args "--profile;release")
else()
  message(WARNING "Unsupported BUILD_TAOSX_PROFILE: ${BUILD_TAOSX_PROFILE}; falling back to release")
  set(_taosx_effective_profile "release")
  set(_taosx_cargo_profile "release")
  set(_taosx_cargo_profile_dir "release")
  set(_taosx_cargo_profile_args "--profile;release")
endif()

set(_taosx_artifact_dir "${_taosx_target_dir}/${_taosx_cargo_profile_dir}")

# ── Output artifact paths (same as taosd: build/bin) ───────────────────────
set(_taosx_bin_output_dir "${CMAKE_BINARY_DIR}/build/bin")
set(_taosx_binary_output "${_taosx_bin_output_dir}/taosx${CMAKE_EXECUTABLE_SUFFIX}")
set(_taosx_xnoded_binary_output "${_taosx_bin_output_dir}/xnoded${CMAKE_EXECUTABLE_SUFFIX}")
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
  "${TD_TAOSX_DIR}/xnoded/**/*.rs"
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

# ── Build explorer docs assets (stamp-file based for correct dependencies) ──
set(_explorer_docs_stamp "${CMAKE_BINARY_DIR}/build/taos-xservice/explorer_docs.stamp")

if(BUILD_EXPLORER_UI)
  if(BUILD_EXPLORER_DOCS AND BUILD_ENTERPRISE)
    find_program(UNZIP_EXECUTABLE unzip REQUIRED)
    if(NOT EXISTS "${TD_TAOSX_DIR}/docs-zh.zip")
      message(FATAL_ERROR "Missing local explorer docs zip: ${TD_TAOSX_DIR}/docs-zh.zip")
    endif()
    if(NOT EXISTS "${TD_TAOSX_DIR}/docs-en.zip")
      message(FATAL_ERROR "Missing local explorer docs zip: ${TD_TAOSX_DIR}/docs-en.zip")
    endif()
    add_custom_command(
      OUTPUT "${_explorer_docs_stamp}"
      COMMAND "${CMAKE_COMMAND}" -E remove_directory "${_explorer_ui_dir}/public/docs"
      COMMAND "${CMAKE_COMMAND}" -E remove_directory "${_explorer_ui_dir}/public/docs-en"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_explorer_ui_dir}/public/docs"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_explorer_ui_dir}/public/docs-en"
      COMMAND "${UNZIP_EXECUTABLE}" -u "${TD_TAOSX_DIR}/docs-zh.zip" -d "${_explorer_ui_dir}/public/docs"
      COMMAND "${UNZIP_EXECUTABLE}" -u "${TD_TAOSX_DIR}/docs-en.zip" -d "${_explorer_ui_dir}/public/docs-en"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/build/taos-xservice"
      COMMAND "${CMAKE_COMMAND}" -E touch "${_explorer_docs_stamp}"
      WORKING_DIRECTORY "${TD_TAOSX_DIR}"
      DEPENDS "${TD_TAOSX_DIR}/docs-zh.zip" "${TD_TAOSX_DIR}/docs-en.zip"
      COMMENT "Preparing taos-explorer docs assets"
      VERBATIM
    )
  else()
    add_custom_command(
      OUTPUT "${_explorer_docs_stamp}"
      COMMAND "${CMAKE_COMMAND}" -E remove_directory "${_explorer_ui_dir}/public/docs"
      COMMAND "${CMAKE_COMMAND}" -E remove_directory "${_explorer_ui_dir}/public/docs-en"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/build/taos-xservice"
      COMMAND "${CMAKE_COMMAND}" -E touch "${_explorer_docs_stamp}"
      COMMENT "Removing taos-explorer docs assets"
      VERBATIM
    )
  endif()

  # pnpm build depends on docs stamp — guarantees docs are ready before build
  find_program(PNPM_EXECUTABLE pnpm REQUIRED)
  add_custom_command(
    OUTPUT "${_explorer_dist_dir}/index.html"
    COMMAND "${CMAKE_COMMAND}" -E env CI=true "${PNPM_EXECUTABLE}" install --frozen-lockfile
    COMMAND "${CMAKE_COMMAND}" -E env
            CI=true
            "CUS_PROMPT=${BUILD_CUS_PROMPT}"
            "CUS_NAME=${BUILD_CUS_NAME}"
            "VER_NUMBER=${BUILD_VER_NUMBER}"
            "${PNPM_EXECUTABLE}" run build
    WORKING_DIRECTORY "${_explorer_ui_dir}"
    DEPENDS "${_explorer_docs_stamp}"
    COMMENT "Building taos-explorer frontend UI"
    VERBATIM
  )
endif()

if(_taosx_need_binaries AND NOT BUILD_EXPLORER_UI)
  if(NOT EXISTS "${_explorer_dist_dir}/index.html")
    message(FATAL_ERROR "Missing prebuilt taos-explorer dist: ${_explorer_dist_dir}/index.html")
  endif()
endif()

if(BUILD_EXPLORER_UI OR _taosx_need_binaries)
  add_custom_target(explorer_ui
    DEPENDS "${_explorer_dist_dir}/index.html"
  )
else()
  add_custom_target(explorer_ui
    COMMAND "${CMAKE_COMMAND}" -E echo "Skipping taos-explorer frontend UI"
    COMMENT "Skipping taos-explorer frontend UI"
    VERBATIM
  )
endif()

# ── UPX compression setup (shared by all binary targets) ───────────────────
set(_taosx_upx_binary "")
set(_taosx_enable_upx FALSE)
set(_taosx_require_upx FALSE)

if(BUILD_TAOSX_UPX STREQUAL "AUTO")
  if(_taosx_effective_profile STREQUAL "release")
    set(_taosx_enable_upx TRUE)
  endif()
elseif(BUILD_TAOSX_UPX STREQUAL "ON")
  if(NOT _taosx_effective_profile STREQUAL "release")
    message(FATAL_ERROR "BUILD_TAOSX_UPX=ON requires BUILD_TAOSX_PROFILE=release")
  endif()
  set(_taosx_enable_upx TRUE)
  set(_taosx_require_upx TRUE)
elseif(BUILD_TAOSX_UPX STREQUAL "OFF")
  set(_taosx_enable_upx FALSE)
else()
  message(FATAL_ERROR "Unsupported BUILD_TAOSX_UPX: ${BUILD_TAOSX_UPX}")
endif()

if(_taosx_need_binaries AND _taosx_enable_upx)
  set(_taosx_upx_version "5.0.1")
  set(_taosx_upx_dir "${_taosx_base_dir}/upx")

  if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
    set(_taosx_upx_binary "${_taosx_upx_dir}/upx.exe")
    set(_taosx_upx_archive "${_taosx_upx_dir}/upx.zip")
    set(_taosx_upx_filename "upx-${_taosx_upx_version}-win64.zip")
    if(DEFINED BUILD_DEPS_MIRROR_URL AND NOT "${BUILD_DEPS_MIRROR_URL}" STREQUAL "")
      set(_taosx_upx_url "${BUILD_DEPS_MIRROR_URL}/${_taosx_upx_filename}")
    else()
      set(_taosx_upx_url "https://github.com/upx/upx/releases/download/v${_taosx_upx_version}/${_taosx_upx_filename}")
    endif()
    set(_taosx_upx_sha256 "c288989437ce70646a62799a4dcf25b4ec7ad8fbb4f93a29e25c14856659c1a4")
    find_program(WGET_EXECUTABLE wget REQUIRED)
    find_program(POWERSHELL_EXECUTABLE powershell REQUIRED)
    add_custom_command(
      OUTPUT "${_taosx_upx_binary}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_upx_dir}"
      COMMAND "${WGET_EXECUTABLE}" -O "${_taosx_upx_archive}" "${_taosx_upx_url}"
      COMMAND "${CMAKE_COMMAND}" -E sha256sum "${_taosx_upx_archive}" > "${_taosx_upx_dir}/_upx_checksum.txt"
      COMMAND "${CMAKE_COMMAND}" -E echo "${_taosx_upx_sha256}  ${_taosx_upx_archive}"
      COMMAND "${POWERSHELL_EXECUTABLE}" -command "Expand-Archive -Force '${_taosx_upx_archive}' '${_taosx_upx_dir}/'"
      COMMAND "${POWERSHELL_EXECUTABLE}" -command "Move-Item -Force '${_taosx_upx_dir}/upx-${_taosx_upx_version}-win64/upx.exe' '${_taosx_upx_binary}'"
      COMMAND "${CMAKE_COMMAND}" -E rm -f "${_taosx_upx_archive}"
      COMMENT "Downloading UPX ${_taosx_upx_version}"
      VERBATIM
    )
  elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    find_program(_taosx_upx_binary upx)
    if(NOT _taosx_upx_binary)
      if(_taosx_require_upx)
        message(FATAL_ERROR "BUILD_TAOSX_UPX=ON requires a local upx binary on Darwin")
      endif()
      message(WARNING "UPX is not available for taosx deploy compression on Darwin; copying uncompressed deploy binaries.")
    endif()
  else()
    set(_taosx_upx_binary "${_taosx_upx_dir}/upx")
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)")
      set(_taosx_upx_arch "arm64")
      set(_taosx_upx_sha256 "25afbf685163a04e336d94565ef8e6970b0b2736f4c9a6f2ebc446623c08b75f")
    else()
      set(_taosx_upx_arch "amd64")
      set(_taosx_upx_sha256 "7b9f0634c8b7bce06d88811c85686050ba29534e40371f23d062115176cc7a07")
    endif()
    set(_taosx_upx_archive "${_taosx_upx_dir}/upx.tar.xz")
    set(_taosx_upx_filename "upx-${_taosx_upx_version}-${_taosx_upx_arch}_linux.tar.xz")
    if(DEFINED BUILD_DEPS_MIRROR_URL AND NOT "${BUILD_DEPS_MIRROR_URL}" STREQUAL "")
      set(_taosx_upx_url "${BUILD_DEPS_MIRROR_URL}/${_taosx_upx_filename}")
    else()
      set(_taosx_upx_url "https://github.com/upx/upx/releases/download/v${_taosx_upx_version}/${_taosx_upx_filename}")
    endif()
    if(EXISTS "${_taosx_upx_binary}")
      message(STATUS "UPX binary already exists: ${_taosx_upx_binary}")
    else()
      find_program(WGET_EXECUTABLE wget REQUIRED)
      find_program(TAR_EXECUTABLE tar REQUIRED)
      add_custom_command(
        OUTPUT "${_taosx_upx_binary}"
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_upx_dir}"
        COMMAND "${WGET_EXECUTABLE}" -O "${_taosx_upx_archive}" "${_taosx_upx_url}"
        COMMAND "${CMAKE_COMMAND}" -E sha256sum "${_taosx_upx_archive}" > "${_taosx_upx_dir}/_upx_checksum.txt"
        COMMAND "${TAR_EXECUTABLE}" -xf "${_taosx_upx_archive}" --strip-components=1 -C "${_taosx_upx_dir}"
        COMMAND "${CMAKE_COMMAND}" -E rm -f "${_taosx_upx_archive}"
        COMMENT "Downloading UPX ${_taosx_upx_version}"
        VERBATIM
      )
    endif()
  endif()
  if(_taosx_upx_binary AND NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_custom_target(taosx_upx
      DEPENDS "${_taosx_upx_binary}"
    )
  endif()
endif()

# ── Helper: deploy command for a single binary (UPX or plain copy) ─────────
# Sets parent-scope variable: _deploy_cmd_<name>
macro(_taosx_deploy_command name src dst)
  if(_taosx_upx_binary)
    set(_deploy_cmd_${name}
      COMMAND "${_taosx_upx_binary}" -f "${src}" -o "${dst}"
    )
  else()
    set(_deploy_cmd_${name}
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${src}" "${dst}"
    )
  endif()
endmacro()

# ── Build taosx, taosx-agent, taos-explorer (cargo) ────────────────────────
if(_taosx_need_binaries)
  find_program(CARGO_EXECUTABLE cargo REQUIRED)

  set(_taosx_binary_extra_deps)
  if(_taosx_upx_binary AND NOT CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    list(APPEND _taosx_binary_extra_deps
      "${_taosx_upx_binary}"
      taosx_upx
    )
  endif()

  set(_taosx_component_targets)

  set(_taosx_cargo_env
    "CARGO_TARGET_DIR=${_taosx_target_dir}"
    "CUS_PROMPT=${BUILD_CUS_PROMPT}"
    "CUS_NAME=${BUILD_CUS_NAME}"
    "CUS_EMAIL=${BUILD_CUS_EMAIL}"
    "VER_NUMBER=${BUILD_VER_NUMBER}"
  )

  if(BUILD_TAOSX_BINARY)
    _taosx_deploy_command(taosx
      "${_taosx_artifact_dir}/taosx${CMAKE_EXECUTABLE_SUFFIX}"
      "${_taosx_deploy_dir}/taosx")
    _taosx_deploy_command(xnoded
      "${_taosx_artifact_dir}/xnoded${CMAKE_EXECUTABLE_SUFFIX}"
      "${_taosx_deploy_dir}/xnoded")
    add_custom_command(
      OUTPUT "${_taosx_binary_output}" "${_taosx_xnoded_binary_output}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_target_dir}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_bin_output_dir}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_deploy_dir}"
      COMMAND "${CMAKE_COMMAND}" -E env ${_taosx_cargo_env}
              "${CARGO_EXECUTABLE}" build -p taosx ${_taosx_cargo_profile_args}
                                    --target-dir "${_taosx_target_dir}"
      COMMAND "${CMAKE_COMMAND}" -E env ${_taosx_cargo_env}
              "${CARGO_EXECUTABLE}" build -p xnoded ${_taosx_cargo_profile_args}
                                    --target-dir "${_taosx_target_dir}"
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
              "${_taosx_artifact_dir}/taosx${CMAKE_EXECUTABLE_SUFFIX}"
              "${_taosx_binary_output}"
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
              "${_taosx_artifact_dir}/xnoded${CMAKE_EXECUTABLE_SUFFIX}"
              "${_taosx_xnoded_binary_output}"
      ${_deploy_cmd_taosx}
      ${_deploy_cmd_xnoded}
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
              "${_taosx_artifact_dir}/${BUILD_CUS_PROMPT}x.service"
              "${_taosx_deploy_dir}/taosx.service"
      WORKING_DIRECTORY "${TD_TAOSX_DIR}"
      DEPENDS ${_taosx_dep_files} ${_taosx_binary_extra_deps}
      VERBATIM
      COMMENT "Building taosx+xnoded binaries (${_taosx_cargo_profile}) → ${_taosx_bin_output_dir}"
    )
    add_custom_target(taosx_binary
      DEPENDS "${_taosx_binary_output}" "${_taosx_xnoded_binary_output}"
    )
    list(APPEND _taosx_component_targets taosx_binary)
  endif()

  if(BUILD_TAOSX_AGENT)
    _taosx_deploy_command(agent
      "${_taosx_artifact_dir}/taosx-agent${CMAKE_EXECUTABLE_SUFFIX}"
      "${_taosx_deploy_dir}/taosx-agent")
    add_custom_command(
      OUTPUT "${_taosx_agent_binary_output}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_target_dir}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_bin_output_dir}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_deploy_dir}"
      COMMAND "${CMAKE_COMMAND}" -E env ${_taosx_cargo_env}
              "${CARGO_EXECUTABLE}" build -p taosx-agent ${_taosx_cargo_profile_args}
                                    --target-dir "${_taosx_target_dir}"
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
              "${_taosx_artifact_dir}/taosx-agent${CMAKE_EXECUTABLE_SUFFIX}"
              "${_taosx_agent_binary_output}"
      ${_deploy_cmd_agent}
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
              "${_taosx_artifact_dir}/${BUILD_CUS_PROMPT}x-agent.service"
              "${_taosx_deploy_dir}/taosx-agent.service"
      WORKING_DIRECTORY "${TD_TAOSX_DIR}"
      DEPENDS ${_taosx_dep_files} ${_taosx_binary_extra_deps}
      VERBATIM
      COMMENT "Building taosx-agent binary (${_taosx_cargo_profile}) → ${_taosx_agent_binary_output}"
    )
    add_custom_target(taosx_agent_binary
      DEPENDS "${_taosx_agent_binary_output}"
    )
    list(APPEND _taosx_component_targets taosx_agent_binary)
  endif()

  if(BUILD_EXPLORER)
    _taosx_deploy_command(explorer
      "${_taosx_artifact_dir}/taos-explorer${CMAKE_EXECUTABLE_SUFFIX}"
      "${_taosx_deploy_dir}/taos-explorer")
    add_custom_command(
      OUTPUT "${_taosx_explorer_binary_output}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_target_dir}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_bin_output_dir}"
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_taosx_deploy_dir}"
      COMMAND "${CMAKE_COMMAND}" -E env ${_taosx_cargo_env}
              "${CARGO_EXECUTABLE}" build -p taos-explorer ${_taosx_cargo_profile_args}
                                    --target-dir "${_taosx_target_dir}"
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
              "${_taosx_artifact_dir}/taos-explorer${CMAKE_EXECUTABLE_SUFFIX}"
              "${_taosx_explorer_binary_output}"
      ${_deploy_cmd_explorer}
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
              "${_taosx_artifact_dir}/${BUILD_CUS_PROMPT}-explorer.service"
              "${_taosx_deploy_dir}/taos-explorer.service"
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
              "${TD_TAOSX_DIR}/explorer/server/examples/explorer.toml"
              "${_taosx_deploy_dir}/explorer.toml"
      WORKING_DIRECTORY "${TD_TAOSX_DIR}"
      DEPENDS ${_taosx_dep_files} ${_taosx_binary_extra_deps} "${_explorer_dist_dir}/index.html"
      VERBATIM
      COMMENT "Building taos-explorer binary (${_taosx_cargo_profile}) → ${_taosx_explorer_binary_output}"
    )
    add_custom_target(taos_explorer_binary
      DEPENDS "${_taosx_explorer_binary_output}"
    )
    add_dependencies(taos_explorer_binary explorer_ui)
    list(APPEND _taosx_component_targets taos_explorer_binary)
  endif()

  add_custom_target(taosx ALL
    DEPENDS ${_taosx_component_targets}
  )
else()
  # No binaries — just ensure explorer UI is built if requested
  add_custom_target(taosx_ui
    DEPENDS explorer_ui
  )
  add_custom_target(taosx ALL
    DEPENDS taosx_ui
  )
endif()
