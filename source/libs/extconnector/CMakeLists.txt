aux_source_directory(src EXT_CONNECTOR_SRC)

# ──────────────────────────────────────────────────────────────────────────────
# Enterprise extension: pull in provider implementations
# ──────────────────────────────────────────────────────────────────────────────
if(TD_ENTERPRISE)
  LIST(APPEND EXT_CONNECTOR_SRC
    ${TD_ENTERPRISE_DIR}/src/plugins/extsource/src/extConnector.c
    ${TD_ENTERPRISE_DIR}/src/plugins/extsource/src/extConnectorQuery.c
    ${TD_ENTERPRISE_DIR}/src/plugins/extsource/src/extConnectorMySQL.c
    ${TD_ENTERPRISE_DIR}/src/plugins/extsource/src/extConnectorPG.c
    # InfluxDB HTTP path (pure C, always compiled)
    ${TD_ENTERPRISE_DIR}/src/plugins/extsource/src/extConnectorInfluxHttp.c
    # InfluxDB protocol dispatch layer (pure C, always compiled)
    ${TD_ENTERPRISE_DIR}/src/plugins/extsource/src/extConnectorInfluxDispatch.c
  )
  # InfluxDB Arrow path (C++, compiled only when Arrow Flight SQL is available)
  LIST(APPEND EXT_CONNECTOR_SRC
    ${TD_ENTERPRISE_DIR}/src/plugins/extsource/src/extConnectorInflux.cpp
  )
endif()

add_library(extconnector STATIC ${EXT_CONNECTOR_SRC})

target_include_directories(
    extconnector
    PUBLIC  "${TD_SOURCE_DIR}/include/libs/extconnector"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    extconnector
    PRIVATE os util common qcom nodes
)

# ──────────────────────────────────────────────────────────────────────────────
# Enterprise: link external database client libraries (downloaded via CMake
# ExternalProject_Add; see community/cmake/external.cmake for build rules)
# ──────────────────────────────────────────────────────────────────────────────
if(TD_ENTERPRISE)
  # MySQL / MariaDB Connector/C (ext_mariadb, default ON)
  if(BUILD_WITH_MARIADB)
    DEP_ext_mariadb(extconnector)
    target_compile_definitions(extconnector PRIVATE HAVE_MARIADB_C)
    message(STATUS "ExtConnector: MariaDB Connector/C enabled — MySQL provider active")
  else()
    message(STATUS "ExtConnector: BUILD_WITH_MARIADB=OFF — MySQL provider disabled")
  endif()

  # PostgreSQL libpq (ext_libpq, default ON)
  if(BUILD_WITH_LIBPQ)
    DEP_ext_libpq(extconnector)
    target_compile_definitions(extconnector PRIVATE HAVE_LIBPQ)
    message(STATUS "ExtConnector: libpq enabled — PostgreSQL provider active")
  else()
    message(STATUS "ExtConnector: BUILD_WITH_LIBPQ=OFF — PostgreSQL provider disabled")
  endif()

  # Apache Arrow C++ 16.0 with Flight SQL (ext_arrow, default ON)
  if(BUILD_WITH_ARROW)
    DEP_ext_arrow(extconnector)
    target_compile_definitions(extconnector PRIVATE HAVE_ARROW_FLIGHT_SQL)
    message(STATUS "ExtConnector: Apache Arrow Flight SQL enabled — InfluxDB Arrow path active")
  else()
    message(STATUS "ExtConnector: BUILD_WITH_ARROW=OFF — InfluxDB HTTP path only")
  endif()

  # libcurl (InfluxDB HTTP path + any future HTTP connectors)
  DEP_ext_curl(extconnector)

  # cJSON (used by HTTP connector for response parsing)
  DEP_ext_cjson(extconnector)

  # crypt is needed for password decrypt
  target_link_libraries(extconnector PRIVATE crypt)
endif()
