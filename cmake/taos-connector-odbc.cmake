# Processing taos-connector-odbc compilation
message(STATUS "")
message(STATUS "Processing taos-connector-odbc compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taos-connector-odbc build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_ODBC              = ${BUILD_ODBC}")
message(STATUS "  CMAKE_BUILD_TYPE        = ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_CONNECTOR_ODBC_DIR   = ${TD_CONNECTOR_ODBC_DIR}")
message(STATUS "  TD_COMMUNITY_DIR        = ${TD_COMMUNITY_DIR}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "================================================")
message(STATUS "")

# Verify connector-odbc repo exists
if(NOT EXISTS "${TD_CONNECTOR_ODBC_DIR}/CMakeLists.txt")
  message(FATAL_ERROR
    "TD_CONNECTOR_ODBC_DIR is not a taos-connector-odbc repo: ${TD_CONNECTOR_ODBC_DIR}")
endif()

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-connector-odbc) ─
set(_odbc_base_dir "${CMAKE_BINARY_DIR}/build/taos-connector-odbc")
set(_odbc_build_dir "${_odbc_base_dir}/build")
set(_odbc_output_dir "${_odbc_base_dir}/output")

# Map CMAKE_BUILD_TYPE for the sub-build
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(_odbc_build_type "Debug")
else()
  set(_odbc_build_type "Release")
endif()

# ── Output artifact paths ──────────────────────────────────────────────────
set(_odbc_so "${_odbc_output_dir}/${CMAKE_SHARED_LIBRARY_PREFIX}taos_odbc${CMAKE_SHARED_LIBRARY_SUFFIX}")
set(_odbc_a  "${_odbc_output_dir}/${CMAKE_STATIC_LIBRARY_PREFIX}taos_odbc_a${CMAKE_STATIC_LIBRARY_SUFFIX}")

message(STATUS "ODBC build config:")
message(STATUS "  build type              = ${_odbc_build_type}")
message(STATUS "  base dir                = ${_odbc_base_dir}")
message(STATUS "  build dir               = ${_odbc_build_dir}")
message(STATUS "  output dir              = ${_odbc_output_dir}")
message(STATUS "  shared lib              = ${_odbc_so}")
message(STATUS "  static lib              = ${_odbc_a}")

file(GLOB_RECURSE _odbc_sources CONFIGURE_DEPENDS
  "${TD_CONNECTOR_ODBC_DIR}/src/*.c"
  "${TD_CONNECTOR_ODBC_DIR}/src/*.h"
  "${TD_CONNECTOR_ODBC_DIR}/common/*.c"
  "${TD_CONNECTOR_ODBC_DIR}/common/*.h"
  "${TD_CONNECTOR_ODBC_DIR}/inc/*.h"
  "${TD_CONNECTOR_ODBC_DIR}/inc/*.h.in"
)

# Build ODBC as an external cmake project so all its intermediate files
# (CMakeCache, object files, libraries) stay under the build tree.
#
# On Windows the sub-project uses the VS generator (multi-config) which puts
# binaries under src/<Config>/.  On Linux/macOS it uses single-config generators
# which put binaries directly under src/.
if(WIN32)
  set(_odbc_artifact_subdir "${_odbc_build_type}/")
else()
  set(_odbc_artifact_subdir "")
endif()

# On Windows, disable C# test subdirectory (requires VS .NET workload)
if(WIN32)
  set(_odbc_fix_csharp_cmd
    COMMAND "${CMAKE_COMMAND}"
            -DINPUT_FILE=${TD_CONNECTOR_ODBC_DIR}/tests/CMakeLists.txt
            -P "${CMAKE_SOURCE_DIR}/cmake/toolchains/fix-odbc-csharp.cmake"
  )
else()
  set(_odbc_fix_csharp_cmd)
endif()

add_custom_command(
  OUTPUT "${_odbc_so}" "${_odbc_a}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_odbc_build_dir}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_odbc_output_dir}"
  ${_odbc_fix_csharp_cmd}
  COMMAND "${CMAKE_COMMAND}"
          -S "${TD_CONNECTOR_ODBC_DIR}"
          -B "${_odbc_build_dir}"
          -DCMAKE_BUILD_TYPE=${_odbc_build_type}
          -DCMAKE_PREFIX_PATH=${CMAKE_PREFIX_PATH}
          -DCMAKE_C_FLAGS=-I${TD_COMMUNITY_DIR}/include/client\ -I${TD_COMMUNITY_DIR}/include/util
          -DFAKE_TAOS=ON
          -DBUILD_TESTING=OFF
  COMMAND "${CMAKE_COMMAND}"
          --build "${_odbc_build_dir}"
          --config "${_odbc_build_type}"
          --target taos_odbc taos_odbc_a
          -j4
  COMMAND "${CMAKE_COMMAND}" -E rm -rf "${TD_CONNECTOR_ODBC_DIR}/.externals"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_odbc_build_dir}/src/${_odbc_artifact_subdir}${CMAKE_SHARED_LIBRARY_PREFIX}taos_odbc${CMAKE_SHARED_LIBRARY_SUFFIX}"
          "${_odbc_so}"
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_odbc_build_dir}/src/${_odbc_artifact_subdir}${CMAKE_STATIC_LIBRARY_PREFIX}taos_odbc_a${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${_odbc_a}"
  WORKING_DIRECTORY "${TD_CONNECTOR_ODBC_DIR}"
  DEPENDS "${TD_CONNECTOR_ODBC_DIR}/CMakeLists.txt"
          ${_odbc_sources}
  VERBATIM
  COMMENT "Building taos-connector-odbc (${_odbc_build_type}) → ${_odbc_output_dir}"
)

add_custom_target(taos_connector_odbc ALL
  DEPENDS "${_odbc_so}" "${_odbc_a}"
)
