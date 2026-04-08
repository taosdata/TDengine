# Processing taos-connector-dotnet compilation
message(STATUS "")
message(STATUS "Processing taos-connector-dotnet compilation")

# ============================================================================
# Print input variables from upper layer
# ============================================================================
message(STATUS "")
message(STATUS "=== taos-connector-dotnet build configuration ===")
message(STATUS "Input build flags:")
message(STATUS "  BUILD_DOTNET            = ${BUILD_DOTNET}")
message(STATUS "  CMAKE_BUILD_TYPE        = ${CMAKE_BUILD_TYPE}")
message(STATUS "")
message(STATUS "Input paths:")
message(STATUS "  TD_CONNECTOR_DOTNET_DIR = ${TD_CONNECTOR_DOTNET_DIR}")
message(STATUS "  CMAKE_BINARY_DIR        = ${CMAKE_BINARY_DIR}")
message(STATUS "")
message(STATUS "Platform info:")
message(STATUS "  CMAKE_SYSTEM_NAME       = ${CMAKE_SYSTEM_NAME}")
message(STATUS "  CMAKE_SYSTEM_PROCESSOR  = ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "=================================================")
message(STATUS "")

# Verify connector-dotnet repo exists
if(NOT EXISTS "${TD_CONNECTOR_DOTNET_DIR}/src/TDengine.csproj")
  message(FATAL_ERROR
    "TD_CONNECTOR_DOTNET_DIR is not a taos-connector-dotnet repo: ${TD_CONNECTOR_DOTNET_DIR}")
endif()

find_program(DOTNET_EXECUTABLE dotnet REQUIRED)

# ── Map CMAKE_BUILD_TYPE to dotnet Configuration ───────────────────────────
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(_dotnet_config "Debug")
else()
  set(_dotnet_config "Release")
endif()

# ── Output directories (all under CMAKE_BINARY_DIR/build/taos-connector-dotnet) ─
set(_dotnet_base_dir "${CMAKE_BINARY_DIR}/build/taos-connector-dotnet")
set(_dotnet_output_dir "${_dotnet_base_dir}/output")
set(_dotnet_bin_dir    "${_dotnet_base_dir}/bin/")
set(_dotnet_obj_dir    "${_dotnet_base_dir}/obj/")

# Extract <Version> from .csproj to predict the nupkg filename
file(STRINGS "${TD_CONNECTOR_DOTNET_DIR}/src/TDengine.csproj" _csproj_ver_line
  REGEX "<Version>[^<]+</Version>")
string(REGEX REPLACE ".*<Version>([^<]+)</Version>.*" "\\1" _dotnet_pkg_version "${_csproj_ver_line}")

set(_nupkg_filename "TDengine.Connector.${_dotnet_pkg_version}.nupkg")
set(_nupkg_output "${_dotnet_output_dir}/${_nupkg_filename}")

message(STATUS "Dotnet build config:")
message(STATUS "  base dir                = ${_dotnet_base_dir}")
message(STATUS "  Configuration           = ${_dotnet_config}")
message(STATUS "  bin dir (intermediate)  = ${_dotnet_bin_dir}")
message(STATUS "  obj dir (intermediate)  = ${_dotnet_obj_dir}")
message(STATUS "  output dir              = ${_dotnet_output_dir}")
message(STATUS "  nupkg                   = ${_nupkg_output}")

file(GLOB_RECURSE _connector_dotnet_sources CONFIGURE_DEPENDS
  "${TD_CONNECTOR_DOTNET_DIR}/src/*.cs"
)

# Build the library and produce a NuGet package, all into the build tree.
add_custom_command(
  OUTPUT "${_nupkg_output}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${_dotnet_output_dir}"
  COMMAND "${DOTNET_EXECUTABLE}" build
          "${TD_CONNECTOR_DOTNET_DIR}/src/TDengine.csproj"
          -c "${_dotnet_config}"
          -f netstandard2.0
          -p:TargetFrameworks=netstandard2.0
          "-p:BaseOutputPath=${_dotnet_bin_dir}"
          "-p:BaseIntermediateOutputPath=${_dotnet_obj_dir}"
          --nologo
  COMMAND "${DOTNET_EXECUTABLE}" pack
          "${TD_CONNECTOR_DOTNET_DIR}/src/TDengine.csproj"
          -c "${_dotnet_config}"
          --output "${_dotnet_output_dir}"
          -p:TargetFrameworks=netstandard2.0
          "-p:BaseOutputPath=${_dotnet_bin_dir}"
          "-p:BaseIntermediateOutputPath=${_dotnet_obj_dir}"
          --no-build
          --nologo
  WORKING_DIRECTORY "${TD_CONNECTOR_DOTNET_DIR}"
  DEPENDS "${TD_CONNECTOR_DOTNET_DIR}/src/TDengine.csproj"
          ${_connector_dotnet_sources}
  VERBATIM
  COMMENT "Building taos-connector-dotnet (${_dotnet_config}) → ${_dotnet_output_dir}"
)

add_custom_target(taos_connector_dotnet ALL
  DEPENDS "${_nupkg_output}"
)
