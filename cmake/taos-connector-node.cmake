# Processing taos-connector-node compilation
message(STATUS "")
message(STATUS "Processing taos-connector-node compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taos-connector-node build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_NODE              = ${BUILD_NODE}")
message(STATUS "  CMAKE_BUILD_TYPE        = ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_CONNECTOR_NODE_DIR   = ${TD_CONNECTOR_NODE_DIR}")
message(STATUS "  CMAKE_BINARY_DIR        = ${CMAKE_BINARY_DIR}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "================================================")
message(STATUS "")

# The actual Node.js package is in the nodejs/ subdirectory
set(_node_pkg_dir "${TD_CONNECTOR_NODE_DIR}/nodejs")

# Verify connector-node repo exists
if(NOT EXISTS "${_node_pkg_dir}/package.json")
  message(FATAL_ERROR
    "TD_CONNECTOR_NODE_DIR is not a taos-connector-node repo: ${TD_CONNECTOR_NODE_DIR}")
endif()

find_program(NPM_EXECUTABLE NAMES npm.cmd npm REQUIRED)

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-connector-node) ─
set(_node_base_dir "${CMAKE_BINARY_DIR}/build/taos-connector-node")
set(_node_output_dir "${_node_base_dir}/output")
set(_node_stamp "${_node_base_dir}/taos_connector_node_built")
set(_node_cache_dir "${_node_base_dir}/npm-cache")

message(STATUS "Node build config:")
message(STATUS "  base dir                = ${_node_base_dir}")
message(STATUS "  node package dir        = ${_node_pkg_dir}")
message(STATUS "  output dir              = ${_node_output_dir}")
message(STATUS "  npm cache dir           = ${_node_cache_dir}")
message(STATUS "  intermediate dirs       = ${_node_pkg_dir}/{node_modules,lib} (npm default)")

file(GLOB_RECURSE _node_sources CONFIGURE_DEPENDS
  "${_node_pkg_dir}/src/*.ts"
  "${_node_pkg_dir}/index.ts"
)

# Build steps:
# 1. npm install (with cache redirected to build tree)
# 2. npm run build (tsc → lib/)
# 3. npm pack (produces .tgz in output dir)
# Intermediate files (node_modules, lib/) stay in source for incremental builds.
add_custom_command(
  OUTPUT "${_node_stamp}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_node_output_dir}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_node_cache_dir}"
  COMMAND "${CMAKE_COMMAND}" -E env
          "npm_config_cache=${_node_cache_dir}"
          "${NPM_EXECUTABLE}" install --prefer-offline
  COMMAND "${NPM_EXECUTABLE}" run build
  COMMAND "${NPM_EXECUTABLE}" pack --pack-destination "${_node_output_dir}"
  COMMAND "${CMAKE_COMMAND}" -E touch "${_node_stamp}"
  WORKING_DIRECTORY "${_node_pkg_dir}"
  DEPENDS "${_node_pkg_dir}/package.json"
          "${_node_pkg_dir}/tsconfig.json"
          ${_node_sources}
  VERBATIM
  COMMENT "Building taos-connector-node → ${_node_output_dir}"
)

add_custom_target(taos_connector_node ALL
  DEPENDS "${_node_stamp}"
)
