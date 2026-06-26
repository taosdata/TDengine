aux_source_directory(src EXT_CONNECTOR_SRC)

# ──────────────────────────────────────────────────────────────────────────────
# Enterprise extension: pull in provider implementations
# ──────────────────────────────────────────────────────────────────────────────
if(TD_ENTERPRISE)
  LIST(APPEND EXT_CONNECTOR_SRC
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnector.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorQuery.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorMySQL.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorPG.c
    # InfluxDB HTTP path (C++, uses format=parquet — DS §5.1)
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInfluxHttp.cpp
    # InfluxDB protocol dispatch layer (pure C, always compiled)
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInfluxDispatch.c
  )
  # InfluxDB Arrow path (C++, compiled only when Arrow Flight SQL is available)
  LIST(APPEND EXT_CONNECTOR_SRC
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInflux.cpp
  )
endif()

add_library(extconnector STATIC ${EXT_CONNECTOR_SRC})

# Arrow's flight headers (arrow/flight/types.h) declare FlightInfo with
# std::vector<FlightEndpoint> as a member while FlightEndpoint is only
# forward-declared earlier in the same file. Under libc++ in C++20 mode
# (e.g. the MacOSX15.5 SDK), std::vector<T>'s destructor is constexpr and
# therefore eagerly instantiated when the enclosing class is parsed,
# which fails because T is incomplete. Compiling these TUs in C++17 keeps
# the vector destructor non-constexpr and avoids the eager instantiation.
# arrow::util::span is a polyfill that does not require std::span, so C++17
# is sufficient for the Arrow APIs we use.
target_compile_features(extconnector PRIVATE cxx_std_17)

target_include_directories(
    extconnector
    PUBLIC  "${TD_SOURCE_DIR}/include/libs/extconnector"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(
    extconnector
    PRIVATE os util common qcom nodes
)

if(TD_ENTERPRISE AND UNIX AND NOT APPLE)
  # Enterprise extconnector references nodesRemotePlanToSQL from libnodes.a.
  # Propagate this to all consumers so static link order does not drop the
  # object before libextconnector.a introduces the reference.
  target_link_options(extconnector INTERFACE "LINKER:--undefined=nodesRemotePlanToSQL")
endif()

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
    target_compile_definitions(extconnector PRIVATE HAVE_ARROW)
    if(NOT TD_WINDOWS)
      # Flight SQL (gRPC/OpenSSL) is only built on non-Windows platforms.
      # On Windows, only the core Arrow library is available.
      target_compile_definitions(extconnector PRIVATE HAVE_ARROW_FLIGHT_SQL)
      message(STATUS "ExtConnector: Apache Arrow Flight SQL enabled — InfluxDB Arrow path active")
    else()
      message(STATUS "ExtConnector: Apache Arrow (core only, no Flight SQL) — Windows build")
    endif()
    target_compile_definitions(extconnector PRIVATE HAVE_ARROW_PARQUET)
    message(STATUS "ExtConnector: Arrow Parquet enabled — InfluxDB HTTP Parquet path active")
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
  #   build/lib/libarrow.so.1900  (and libarrow_flight*.so.1900)
  # ──────────────────────────────────────────────────────────────────────────
  if(NOT TD_WINDOWS)
  set(_ext_staging_dir "${CMAKE_BINARY_DIR}/build/lib")

  if(BUILD_WITH_MARIADB)
    if(EXT_MARIADB_SO_SRCPATH)
      # System MariaDB — copy from system library path
      set(_mariadb_so_src "${EXT_MARIADB_SO_SRCPATH}")
    elseif(TD_DARWIN)
      # ExternalProject MariaDB — macOS dylib naming
      set(_mariadb_so_src "${TD_EXTERNALS_BASE_DIR}/install/ext_mariadb/${TD_CONFIG_NAME}/lib/mariadb/libmariadb.3.dylib")
    else()
      # ExternalProject MariaDB — Linux .so naming
      set(_mariadb_so_src "${TD_EXTERNALS_BASE_DIR}/install/ext_mariadb/${TD_CONFIG_NAME}/lib/mariadb/libmariadb.so.3")
    endif()
    if(TD_DARWIN)
      set(_mariadb_staged_name "libmariadb.3.dylib")
      set(_mariadb_link_name   "libmariadb.dylib")
    else()
      set(_mariadb_staged_name "libmariadb.so.3")
      set(_mariadb_link_name   "libmariadb.so")
    endif()
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_ext_staging_dir}/mariadb"
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
        "${_mariadb_so_src}"
        "${_ext_staging_dir}/mariadb/${_mariadb_staged_name}"
      COMMAND "${CMAKE_COMMAND}" -E create_symlink
        "${_mariadb_staged_name}"
        "${_ext_staging_dir}/mariadb/${_mariadb_link_name}"
      # Symlink in build/lib/ so binaries whose RPATH points to build/lib
      # (e.g. taosd) can also resolve @rpath/libmariadb.3.dylib
      COMMAND "${CMAKE_COMMAND}" -E create_symlink
        "mariadb/${_mariadb_staged_name}"
        "${_ext_staging_dir}/${_mariadb_staged_name}"
      COMMENT "Staging ${_mariadb_staged_name} → build/lib/mariadb/"
      VERBATIM
    )
  endif()

  if(BUILD_WITH_LIBPQ)
    if(EXT_LIBPQ_SO_SRCPATH)
      set(_libpq_so_src "${EXT_LIBPQ_SO_SRCPATH}")
    elseif(TD_DARWIN)
      set(_libpq_so_src "${TD_EXTERNALS_BASE_DIR}/install/ext_libpq/${TD_CONFIG_NAME}/lib/libpq.5.dylib")
    else()
      set(_libpq_so_src "${TD_EXTERNALS_BASE_DIR}/install/ext_libpq/${TD_CONFIG_NAME}/lib/libpq.so.5")
    endif()
    if(TD_DARWIN)
      set(_libpq_staged_name "libpq.5.dylib")
      set(_libpq_link_name   "libpq.dylib")
    else()
      set(_libpq_staged_name "libpq.so.5")
      set(_libpq_link_name   "libpq.so")
    endif()
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
        "${_libpq_so_src}"
        "${_ext_staging_dir}/${_libpq_staged_name}"
      COMMAND "${CMAKE_COMMAND}" -E create_symlink
        "${_libpq_staged_name}"
        "${_ext_staging_dir}/${_libpq_link_name}"
      COMMENT "Staging ${_libpq_staged_name} → build/lib/"
      VERBATIM
    )
  endif()

  if(BUILD_WITH_ARROW)
    if(EXT_ARROW_SYSTEM_LIBDIR)
      # System Arrow — copy from system library directory with system SO suffix
      set(_arrow_libdir "${EXT_ARROW_SYSTEM_LIBDIR}")
      if(TD_DARWIN)
        if(EXISTS "${_arrow_libdir}/libarrow.${EXT_ARROW_SO_SUFFIX}")
          set(_arrow_so_suffix "${EXT_ARROW_SO_SUFFIX}")
        elseif(EXISTS "${_arrow_libdir}/libarrow.dylib")
          set(_arrow_so_suffix "dylib")
        elseif(EXISTS "${_arrow_libdir}/libarrow.2400.dylib")
          # Homebrew Arrow 24 uses libarrow.2400.dylib and can leave a stale
          # Linux-style EXT_ARROW_SO_SUFFIX in CMakeCache when the build dir is reused.
          set(_arrow_so_suffix "2400.dylib")
        else()
          set(_arrow_so_suffix "${EXT_ARROW_SO_SUFFIX}")
        endif()
      else()
        set(_arrow_so_suffix "${EXT_ARROW_SO_SUFFIX}")
      endif()
    elseif(TD_DARWIN)
      set(_arrow_libdir "${TD_EXTERNALS_BASE_DIR}/install/ext_arrow/${TD_CONFIG_NAME}/lib")
      set(_arrow_so_suffix "1900.dylib")
    else()
      set(_arrow_libdir "${TD_EXTERNALS_BASE_DIR}/install/ext_arrow/${TD_CONFIG_NAME}/lib")
      set(_arrow_so_suffix "so.1900")
    endif()
    set(_arrow_stage_libs libarrow libarrow_flight libarrow_flight_sql libparquet)
    foreach(_arrow_lib ${_arrow_stage_libs})
      if(EXISTS "${_arrow_libdir}/${_arrow_lib}.${_arrow_so_suffix}" OR EXT_ARROW_SYSTEM_LIBDIR)
        if(TD_DARWIN)
          set(_arrow_link_name "${_arrow_lib}.dylib")
        else()
          set(_arrow_link_name "${_arrow_lib}.so")
        endif()
        add_custom_command(TARGET extconnector POST_BUILD
          COMMAND "${CMAKE_COMMAND}" -E copy_if_different
            "${_arrow_libdir}/${_arrow_lib}.${_arrow_so_suffix}"
            "${_ext_staging_dir}/${_arrow_lib}.${_arrow_so_suffix}"
          COMMAND "${CMAKE_COMMAND}" -E create_symlink
            "${_arrow_lib}.${_arrow_so_suffix}"
            "${_ext_staging_dir}/${_arrow_link_name}"
          COMMENT "Staging ${_arrow_lib}.${_arrow_so_suffix} → build/lib/"
          VERBATIM
        )
      endif()
    endforeach()
    if(NOT TD_DARWIN)
      file(GLOB _arrow_extra_shared_libs "${_arrow_libdir}/lib*.so.*")
      foreach(_arrow_extra_lib ${_arrow_extra_shared_libs})
        get_filename_component(_arrow_extra_name "${_arrow_extra_lib}" NAME)
        add_custom_command(TARGET extconnector POST_BUILD
          COMMAND "${CMAKE_COMMAND}" -E copy_if_different
            "${_arrow_extra_lib}"
            "${_ext_staging_dir}/${_arrow_extra_name}"
          COMMENT "Staging ${_arrow_extra_name} -> build/lib/"
          VERBATIM
        )
      endforeach()
    endif()
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
