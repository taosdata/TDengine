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
    if(NOT TD_WINDOWS)
      # Flight SQL (gRPC/OpenSSL) is only built on non-Windows platforms.
      # On Windows, only the core Arrow library is available.
      target_compile_definitions(extconnector PRIVATE HAVE_ARROW_FLIGHT_SQL)
      message(STATUS "ExtConnector: Apache Arrow Flight SQL enabled — InfluxDB Arrow path active")
    else()
      message(STATUS "ExtConnector: Apache Arrow (core only, no Flight SQL) — Windows build")
    endif()
  else()
    message(STATUS "ExtConnector: BUILD_WITH_ARROW=OFF — InfluxDB HTTP path only")
  endif()

  # libcurl (InfluxDB HTTP path + any future HTTP connectors)
  DEP_ext_curl(extconnector)

  # cJSON (used by HTTP connector for response parsing)
  DEP_ext_cjson(extconnector)

  # crypt is needed for password decrypt
  target_link_libraries(extconnector PRIVATE crypt)

  # ──────────────────────────────────────────────────────────────────────────
  # Stage external connector shared libraries into build/lib/ so that make_install.sh
  # (and CPack) can find them at install/package time.
  # This staging is only needed on Linux/macOS (Windows uses DLLs placed differently).
  #
  # make_install.sh expects:
  #   build/lib/mariadb/libmariadb.so.3
  #   build/lib/libpq.so.5
  #   build/lib/libarrow.so.1600  (and libarrow_flight*.so.1600)
  # ──────────────────────────────────────────────────────────────────────────
  if(NOT TD_WINDOWS)
  set(_ext_staging_dir "${CMAKE_BINARY_DIR}/build/lib")

  if(BUILD_WITH_MARIADB)
    set(_mariadb_ins "${TD_EXTERNALS_BASE_DIR}/install/ext_mariadb/${TD_CONFIG_NAME}/lib/mariadb")
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_ext_staging_dir}/mariadb"
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
        "${_mariadb_ins}/libmariadb.so.3"
        "${_ext_staging_dir}/mariadb/libmariadb.so.3"
      COMMAND "${CMAKE_COMMAND}" -E create_symlink
        "libmariadb.so.3"
        "${_ext_staging_dir}/mariadb/libmariadb.so"
      COMMENT "Staging libmariadb.so.3 → build/lib/mariadb/"
      VERBATIM
    )
  endif()

  if(BUILD_WITH_LIBPQ)
    set(_libpq_ins "${TD_EXTERNALS_BASE_DIR}/install/ext_libpq/${TD_CONFIG_NAME}/lib")
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
        "${_libpq_ins}/libpq.so.5"
        "${_ext_staging_dir}/libpq.so.5"
      COMMAND "${CMAKE_COMMAND}" -E create_symlink
        "libpq.so.5"
        "${_ext_staging_dir}/libpq.so"
      COMMENT "Staging libpq.so.5 → build/lib/"
      VERBATIM
    )
  endif()

  if(BUILD_WITH_ARROW)
    set(_arrow_ins "${TD_EXTERNALS_BASE_DIR}/install/ext_arrow/${TD_CONFIG_NAME}/lib")
    foreach(_arrow_lib libarrow libarrow_flight libarrow_flight_sql)
      add_custom_command(TARGET extconnector POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_arrow_ins}/${_arrow_lib}.so.1600"
          "${_ext_staging_dir}/${_arrow_lib}.so.1600"
        COMMAND "${CMAKE_COMMAND}" -E create_symlink
          "${_arrow_lib}.so.1600"
          "${_ext_staging_dir}/${_arrow_lib}.so"
        COMMENT "Staging ${_arrow_lib}.so.1600 → build/lib/"
        VERBATIM
      )
    endforeach()
  endif()

  endif() # NOT TD_WINDOWS

  # ──────────────────────────────────────────────────────────────────────────
  # Windows: stage external connector DLLs into build/bin/ so that
  # make_install.bat can find and copy them to C:\TDengine\.
  # ──────────────────────────────────────────────────────────────────────────
  if(TD_WINDOWS)
    set(_win_bin_dir "${CMAKE_BINARY_DIR}/build/bin")

    if(BUILD_WITH_MARIADB)
      set(_mariadb_dll "${TD_EXTERNALS_BASE_DIR}/install/ext_mariadb/${TD_CONFIG_NAME}/lib/mariadb/libmariadb.dll")
      add_custom_command(TARGET extconnector POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${_win_bin_dir}"
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${_mariadb_dll}" "${_win_bin_dir}/libmariadb.dll"
        COMMENT "Staging libmariadb.dll → build/bin/"
        VERBATIM
      )
    endif()

    if(BUILD_WITH_LIBPQ)
      set(_libpq_dll "${TD_EXTERNALS_BASE_DIR}/install/ext_libpq/${TD_CONFIG_NAME}/bin/libpq.dll")
      add_custom_command(TARGET extconnector POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${_libpq_dll}" "${_win_bin_dir}/libpq.dll"
        COMMENT "Staging libpq.dll → build/bin/"
        VERBATIM
      )
    endif()

    if(BUILD_WITH_ARROW)
      set(_arrow_bin "${TD_EXTERNALS_BASE_DIR}/install/ext_arrow/${TD_CONFIG_NAME}/bin")
      add_custom_command(TARGET extconnector POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${_arrow_bin}/arrow.dll" "${_win_bin_dir}/arrow.dll"
        COMMENT "Staging arrow.dll → build/bin/"
        VERBATIM
      )
      # arrow_flight and arrow_flight_sql only exist when Flight SQL is built (non-Windows)
      # so these are intentionally omitted here.
    endif()
  endif() # TD_WINDOWS

endif()
