# Processing taos-insight compilation (Grafana datasource plugin)
message(STATUS "")
message(STATUS "Processing taos-insight compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taos-insight build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_INSIGHT           = ${BUILD_INSIGHT}")
message(STATUS "  CMAKE_BUILD_TYPE        = ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_INSIGHT_DIR          = ${TD_INSIGHT_DIR}")
message(STATUS "  CMAKE_BINARY_DIR        = ${CMAKE_BINARY_DIR}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "=========================================")
message(STATUS "")

# Verify insight repo exists
if(NOT EXISTS "${TD_INSIGHT_DIR}/go.mod")
  message(FATAL_ERROR
    "TD_INSIGHT_DIR is not a taos-insight repo: ${TD_INSIGHT_DIR}")
endif()

if(NOT EXISTS "${TD_INSIGHT_DIR}/package.json")
  message(FATAL_ERROR
    "TD_INSIGHT_DIR missing package.json: ${TD_INSIGHT_DIR}")
endif()

# ── Find build tools ──────────────────────────────────────────────────────
find_program(GO_EXECUTABLE go REQUIRED)
find_program(MAGE_EXECUTABLE mage
  HINTS "$ENV{HOME}/go/bin" "$ENV{GOPATH}/bin"
  REQUIRED)
find_program(YARN_EXECUTABLE NAMES yarn.cmd yarn REQUIRED)

# Node.js >= 22 is required by taos-insight; we just check it exists
find_program(NODE_EXECUTABLE node REQUIRED)

# ── Map CMAKE_BUILD_TYPE to Go / webpack modes ───────────────────────────
string(TOUPPER "${CMAKE_BUILD_TYPE}" _insight_build_type_upper)
if(_insight_build_type_upper STREQUAL "Debug")
  # Go: disable optimisation & inlining so debugger can step through
  set(_insight_go_gcflags "-gcflags=all=-N -l")
  # webpack: development mode (no minify, full source-maps)
  set(_insight_webpack_env "development")
else()
  set(_insight_go_gcflags "")
  # webpack: production mode (minified, tree-shaken)
  set(_insight_webpack_env "production")
endif()
message(STATUS "Build type mapped:")
message(STATUS "    Go gcflags            = ${_insight_go_gcflags}")
message(STATUS "     webpack --env         = ${_insight_webpack_env}")

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-insight) ───
set(_insight_base_dir "${CMAKE_BINARY_DIR}/build/taos-insight")
set(_insight_output_dir "${_insight_base_dir}/output")
set(_insight_stamp "${_insight_base_dir}/taos_insight_built")
set(_insight_yarn_cache "${_insight_base_dir}/yarn-cache")
set(_insight_gopath "${_insight_base_dir}/gopath")
set(_insight_gocache "${_insight_base_dir}/gocache")

message(STATUS "Insight build config:")
message(STATUS "  output dir              = ${_insight_output_dir}")
message(STATUS "  yarn cache dir          = ${_insight_yarn_cache}")
message(STATUS "  GOPATH                  = ${_insight_gopath}")
message(STATUS "  GOCACHE                 = ${_insight_gocache}")

# ── Source dependencies for rebuild detection ─────────────────────────────
file(GLOB_RECURSE _insight_go_sources CONFIGURE_DEPENDS
  "${TD_INSIGHT_DIR}/pkg/*.go"
)
file(GLOB_RECURSE _insight_ts_sources CONFIGURE_DEPENDS
  "${TD_INSIGHT_DIR}/src/*.ts"
  "${TD_INSIGHT_DIR}/src/*.tsx"
)

# ── Build steps ───────────────────────────────────────────────────────────
# 1. mage -v               → Go backend binaries (cross-compiled, output to dist/)
# 2. yarn install          → Node dependencies (with cache redirected)
# 3. yarn build            → webpack frontend (output to dist/)
# 4. copy dist/            → output dir
# 5. clean dist/ and node_modules/ from source tree
add_custom_command(
  OUTPUT "${_insight_stamp}"
  # Create output directories
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_insight_output_dir}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_insight_yarn_cache}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_insight_gopath}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_insight_gocache}"
  # Step 1: Build Go backend via mage (produces multi-platform binaries in dist/)
  #         GOFLAGS passes -gcflags to disable optimisation in Debug mode
  COMMAND "${CMAKE_COMMAND}" -E env
          "GOPATH=${_insight_gopath}"
          "GOCACHE=${_insight_gocache}"
          "${MAGE_EXECUTABLE}" -v
  # Step 2: Install Node dependencies
  COMMAND "${CMAKE_COMMAND}" -E env
          "npm_config_cache=${_insight_yarn_cache}"
          "${YARN_EXECUTABLE}" install
          --cache-folder "${_insight_yarn_cache}"
          --ignore-engines
  # Step 3: Build frontend via webpack (development or production per build type)
  COMMAND "${YARN_EXECUTABLE}" run webpack
          -c ./.config/webpack/webpack.config.ts
          --env "${_insight_webpack_env}"
  # Step 4: Copy dist/ to output directory
  COMMAND "${CMAKE_COMMAND}" -E copy_directory
          "${TD_INSIGHT_DIR}/dist"
          "${_insight_output_dir}"
  # Step 5: Clean source tree
  COMMAND "${CMAKE_COMMAND}" -E rm -rf "${TD_INSIGHT_DIR}/dist"
  COMMAND "${CMAKE_COMMAND}" -E rm -rf "${TD_INSIGHT_DIR}/node_modules"
  COMMAND "${CMAKE_COMMAND}" -E touch "${_insight_stamp}"
  WORKING_DIRECTORY "${TD_INSIGHT_DIR}"
  DEPENDS "${TD_INSIGHT_DIR}/go.mod"
          "${TD_INSIGHT_DIR}/go.sum"
          "${TD_INSIGHT_DIR}/Magefile.go"
          "${TD_INSIGHT_DIR}/package.json"
          "${TD_INSIGHT_DIR}/yarn.lock"
          ${_insight_go_sources}
          ${_insight_ts_sources}
  VERBATIM
  COMMENT "Building taos-insight (Grafana plugin) → ${_insight_output_dir}"
)

add_custom_target(taos_insight ALL
  DEPENDS "${_insight_stamp}"
)
