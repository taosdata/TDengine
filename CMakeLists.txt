cmake_minimum_required(VERSION 3.16)

project(
  TDengine
  VERSION 3.0
  DESCRIPTION
    "An open-source big data platform designed and optimized for the Internet of Things(IOT)"
) 

# ── Build-type default ─────────────────────────────────────────────────────
# For single-config generators (Make, Ninja) default to Release when the
# caller does not pass -DCMAKE_BUILD_TYPE=<type>.  Multi-config generators
# (MSVC, Xcode) use CMAKE_CONFIGURATION_TYPES and ignore CMAKE_BUILD_TYPE.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE "Debug" CACHE STRING
    "Build type (Debug | Release | RelWithDebInfo | MinSizeRel)" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
    "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

option(BUILD_ENTERPRISE "Build enterprise edition" ON)

option(BUILD_ENGINE "Build engine component" ON)
option(BUILD_ADAPTER "Build taosadapter component" OFF)
option(BUILD_KEEPER "Build taoskeeper component" OFF)
option(BUILD_TOOLS "Build tools component" ON)

option(BUILD_GEN "Build taos-gen component" OFF)
option(BUILD_TAOSX "Build taos-xservice component" OFF)
option(BUILD_TAOSX_BINARY "Build taosx binary" OFF)
option(BUILD_TAOSX_AGENT "Build taosx-agent binary" OFF)

set(BUILD_TAOSX_PROFILE "release" CACHE STRING "Cargo profile for taosx family (release|dev)")
set_property(CACHE BUILD_TAOSX_PROFILE PROPERTY STRINGS "release" "dev")

set(BUILD_TAOSX_UPX "AUTO" CACHE STRING "UPX policy for taosx family (AUTO|ON|OFF)")
set_property(CACHE BUILD_TAOSX_UPX PROPERTY STRINGS "AUTO" "ON" "OFF")
option(BUILD_EXPLORER "Build taos-explorer binary" OFF)
option(BUILD_EXPLORER_UI "Build taos-explorer frontend with pnpm" OFF)
option(BUILD_EXPLORER_DOCS "Bundle taos-explorer docs assets" OFF)
option(BUILD_INSIGHT "Build taos-insight component" OFF)

option(BUILD_DOTNET "Build taos-connector-dotnet component" OFF)
option(BUILD_GO "Build taos-connector-go component" OFF)
option(BUILD_JDBC "Build taos-connector-jdbc component" OFF)
option(BUILD_NODE "Build taos-connector-node component" OFF)
option(BUILD_ODBC "Build taos-connector-odbc component" OFF)
option(BUILD_PYTHON "Build taos-connector-python component" OFF)
option(BUILD_RUST "Build taos-connector-rust component" OFF)

option(BUILD_TEST "Build unit test" OFF)
option(BUILD_SANITIZER "Enable sanitizers" OFF)
option(BUILD_COVERAGE "Enable code coverage" OFF)
option(BUILD_JEMALLOC "Enable jemalloc allocator" OFF)

set(BUILD_CUS_NAME "TDengine" CACHE STRING "Customer/brand name injected into binary")
set(BUILD_CUS_PROMPT "taos" CACHE STRING "Prompt/brand prefix injected into binary")
set(BUILD_CUS_EMAIL "" CACHE STRING "Customer email injected into binary")
set(BUILD_CUS_PRODUCT_NAME "" CACHE STRING "Product name injected into taoskeeper binary")
set(BUILD_VER_NUMBER "" CACHE STRING "Version number override")
set(BUILD_VER_COMPATIBLE "" CACHE STRING "Compatible version number override")
set(BUILD_VER_TYPE "" CACHE STRING "Version type override (e.g. stable)")
set(BUILD_VER_DATE "" CACHE STRING "Version date override (e.g. %Y-%m-%d %H:%M:%S %z)")
set(BUILD_VER_CPUTYPE "" CACHE STRING "CPU type override")
set(BUILD_VER_OSTYPE "" CACHE STRING "OS type override")
set(BUILD_GITINFO "" CACHE STRING "Git commit ID override")
set(BUILD_GITINFOI "" CACHE STRING "Internal git commit ID override")
set(BUILD_DEPS_MIRROR_URL "https://git.tdengine.net/api/v4/projects/70/packages/generic/externals/latest" CACHE STRING
  "URL prefix for downloading ExternalProject dependency tarballs from internal mirror (e.g. GitLab Package Registry)")

include(cmake/build-config.cmake)

if(BUILD_ENGINE)
  add_subdirectory(source/taos-community)
endif()

if(BUILD_ADAPTER)
  include(cmake/taos-adapter.cmake)
endif()

if(BUILD_KEEPER)
  include(cmake/taos-keeper.cmake)
endif()

if(BUILD_GEN)
  include(cmake/taos-gen.cmake)
endif()

if(BUILD_TAOSX)
  include(cmake/taos-xservice.cmake)
endif()

if(BUILD_INSIGHT)
  include(cmake/taos-insight.cmake)
endif()

if(BUILD_DOTNET)
  include(cmake/taos-connector-dotnet.cmake)
endif()

if(BUILD_GO)
  include(cmake/taos-connector-go.cmake)
endif()

if(BUILD_JDBC)
  include(cmake/taos-connector-jdbc.cmake)
endif()

if(BUILD_NODE)
  include(cmake/taos-connector-node.cmake)
endif()

if(BUILD_ODBC)
  include(cmake/taos-connector-odbc.cmake)
endif()

if(BUILD_PYTHON)
  include(cmake/taos-connector-python.cmake)
endif()

if(BUILD_RUST)
  include(cmake/taos-connector-rust.cmake)
endif()
