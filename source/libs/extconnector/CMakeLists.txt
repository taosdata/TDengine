# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

aux_source_directory(src EXT_CONNECTOR_SRC)

if(TD_ENTERPRISE)
  list(APPEND EXT_CONNECTOR_SRC
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnector.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorQuery.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorRuntime.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInfluxPlugin.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorMySQL.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorPG.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorSQLServer.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorOracle.c
  )
endif()

add_library(extconnector STATIC ${EXT_CONNECTOR_SRC})

target_include_directories(extconnector BEFORE PRIVATE "${TD_SOURCE_DIR}/include/libs/qcom")

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
  target_link_options(extconnector INTERFACE "LINKER:--undefined=nodesRemotePlanToSQL")
endif()
# ──────────────────────────────────────────────────────────────────────────────
# Enterprise: link external database client libraries (downloaded via CMake
# ExternalProject_Add; see community/cmake/external.cmake for build rules)
# ──────────────────────────────────────────────────────────────────────────────
if(TD_ENTERPRISE)
  if(BUILD_WITH_MARIADB)
    target_compile_definitions(extconnector PRIVATE HAVE_MARIADB_C)
    message(STATUS "ExtConnector: MySQL provider enabled with runtime libmariadb loading")
  else()
    message(STATUS "ExtConnector: BUILD_WITH_MARIADB=OFF — MySQL provider disabled")
  endif()

  if(BUILD_WITH_LIBPQ)
    target_compile_definitions(extconnector PRIVATE HAVE_LIBPQ)
    # Headers only. DEP_ext_libpq() would also link libpq, which Darwin records
    # as LC_LOAD_DYLIB even when no symbols are referenced. Linux GNU ld drops
    # the unused NEEDED entry via --as-needed, so taosd starts without libpq;
    # macOS does not. Keep the same load-time independence as MariaDB.
    DEP_ext_libpq_INC(extconnector)
    message(STATUS "ExtConnector: PostgreSQL provider enabled with runtime libpq loading")
  else()
    message(STATUS "ExtConnector: BUILD_WITH_LIBPQ=OFF — PostgreSQL provider disabled")
  endif()

  if(BUILD_WITH_ARROW)
    target_compile_definitions(extconnector PRIVATE HAVE_ARROW)
    message(STATUS "ExtConnector: InfluxDB provider enabled via optional runtime plugin")
  else()
    message(STATUS "ExtConnector: BUILD_WITH_ARROW=OFF — InfluxDB provider disabled")
  endif()

  if(BUILD_WITH_ODBC)
    target_compile_definitions(extconnector PRIVATE HAVE_ODBC)
    # Headers only: libodbc is loaded at runtime, same load-time independence
    # as the MariaDB / libpq providers.
    DEP_ext_odbc(extconnector)
    if(TD_WINDOWS)
      message(STATUS "ExtConnector: SQL Server provider enabled with runtime odbc32 loading")
    else()
      message(STATUS "ExtConnector: SQL Server provider enabled with runtime unixODBC loading")
    endif()
  else()
    message(STATUS "ExtConnector: BUILD_WITH_ODBC=OFF — SQL Server provider disabled")
  endif()

  target_link_libraries(extconnector PRIVATE crypt)
endif()

if(TD_ENTERPRISE)
  if(TD_WINDOWS)
    # Windows: runtime libraries live next to the executables (DLL search
    # order starts at the application directory), matching the packaging
    # layout used by the installer.
    set(_td_runtime_lib_dir "${CMAKE_BINARY_DIR}/build/bin")
  else()
    set(_td_runtime_lib_dir "${CMAKE_BINARY_DIR}/build/lib")
    set(_stage_runtime_libs_script "${TD_SOURCE_DIR}/cmake/stage_runtime_libs.sh")
  endif()
endif()

if(TD_ENTERPRISE AND NOT TD_WINDOWS AND INSTALL_FQ_RUNTIME_LIBS)
  # $1 = copy mode: "copy-glob" dereferences vendor symlinks, "copy-glob-links"
  # recreates them.  The second is for vendors that ship one physical library
  # under many versioned aliases (Oracle Instant Client).
  function(_stage_fq_provider_runtime_mode mode src_dir)
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_td_runtime_lib_dir}"
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        ${mode}
        "${src_dir}"
        "${_td_runtime_lib_dir}"
        ${ARGN}
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        copy-ldd-glob
        "${src_dir}"
        "${_td_runtime_lib_dir}"
        ${ARGN}
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        fix-darwin-rpaths
        "${_td_runtime_lib_dir}"
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        fix-linux-rpaths
        "${_td_runtime_lib_dir}"
      COMMENT "Staging federated-query provider runtime libraries -> build/lib/"
      VERBATIM
    )
  endfunction()

  function(_stage_fq_provider_runtime src_dir)
    _stage_fq_provider_runtime_mode(copy-glob "${src_dir}" ${ARGN})
  endfunction()

  # Oracle Instant Client: each server generation has its own directory so
  # the provider can select one by the install-time marker (Oracle DS 3.3 /
  # 9.1.7).  Links are preserved: the bundle ships libclntsh and
  # libclntshcore once each under many versioned aliases.
  function(_stage_fq_oracle_runtime src_dir version)
    # src_dir may carry a $<CONFIG> generator expression (ext_oracle_install);
    # it is only resolved when the POST_BUILD command runs, so no configure-time
    # IS_DIRECTORY check here.
    set(_oracle_dst "${CMAKE_BINARY_DIR}/build/driver/oracle/${version}")
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_oracle_dst}"
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        copy-glob-links
        "${src_dir}"
        "${_oracle_dst}"
        "libsqora*.so*" "libclntsh*.so*" "libclntshcore*.so*" "libnnz*.so*"
        "libons*.so*" "libnfsodm*.so*" "liborazt*.so*" "libociei*.so*"
        "legacy.so*" "fips*.so*" "libaio*.so*"
        "libsqora*.dylib*" "libclntsh*.dylib*"
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        copy-ldd-glob
        "${src_dir}"
        "${_oracle_dst}"
        "libsqora*.so*" "libclntsh*.so*" "libclntshcore*.so*" "libnnz*.so*"
        "libons*.so*" "libnfsodm*.so*" "liborazt*.so*" "libociei*.so*"
        "legacy.so*" "fips*.so*" "libaio*.so*"
        "libsqora*.dylib*" "libclntsh*.dylib*"
      COMMENT "Staging Oracle Instant Client ${version} -> build/driver/oracle/${version}"
      VERBATIM)
  endfunction()

  if(BUILD_WITH_MARIADB)
    if(TARGET ext_mariadb)
      if(NOT TD_EXTERNALS_USE_ONLY)
        add_dependencies(extconnector ext_mariadb)
      endif()
      _stage_fq_provider_runtime("${ext_mariadb_install}/lib/mariadb"
        "libmariadb.so*" "libmysqlclient.so*" "libmariadb*.dylib*" "libmysqlclient*.dylib*")
    elseif(DEFINED EXT_MARIADB_SO_SRCPATH AND NOT "${EXT_MARIADB_SO_SRCPATH}" STREQUAL "")
      get_filename_component(_mariadb_runtime_dir "${EXT_MARIADB_SO_SRCPATH}" DIRECTORY)
      _stage_fq_provider_runtime("${_mariadb_runtime_dir}"
        "libmariadb.so*" "libmysqlclient.so*" "libmariadb*.dylib*" "libmysqlclient*.dylib*")
    endif()
  endif()

  if(BUILD_WITH_LIBPQ)
    if(TARGET ext_libpq)
      if(NOT TD_EXTERNALS_USE_ONLY)
        add_dependencies(extconnector ext_libpq)
      endif()
      _stage_fq_provider_runtime("${ext_libpq_install}/lib"
        "libpq.so*" "libpq*.dylib*")
    elseif(DEFINED EXT_LIBPQ_SO_SRCPATH AND NOT "${EXT_LIBPQ_SO_SRCPATH}" STREQUAL "")
      get_filename_component(_libpq_runtime_dir "${EXT_LIBPQ_SO_SRCPATH}" DIRECTORY)
      _stage_fq_provider_runtime("${_libpq_runtime_dir}"
        "libpq.so*" "libpq*.dylib*")
    endif()
  endif()

  if(BUILD_WITH_ODBC)
    if(TARGET ext_odbc)
      if(NOT TD_EXTERNALS_USE_ONLY)
        add_dependencies(extconnector ext_odbc)
      endif()
      _stage_fq_provider_runtime("${ext_odbc_install}/lib"
        "libodbc.so*" "libodbcinst.so*" "libodbc*.dylib*")
    elseif(DEFINED EXT_ODBC_SO_SRCPATH AND NOT "${EXT_ODBC_SO_SRCPATH}" STREQUAL "")
      get_filename_component(_odbc_runtime_dir "${EXT_ODBC_SO_SRCPATH}" DIRECTORY)
      _stage_fq_provider_runtime("${_odbc_runtime_dir}"
        "libodbc.so*" "libodbcinst.so*" "libodbc*.dylib*")
    endif()

    # Microsoft ODBC Driver 18 (SQL Server): staged next to libodbc, the same
    # place the dependency package puts it (`driver/` in the installed tree).
    if(TARGET ext_msodbc)
      if(NOT TD_EXTERNALS_USE_ONLY)
        add_dependencies(extconnector ext_msodbc)
      endif()
      _stage_fq_provider_runtime("${ext_msodbc_install}/lib"
        "libmsodbcsql*.so*" "libmsodbcsql*.dylib*")
    elseif(DEFINED EXT_MSODBC_SO_SRCPATH AND NOT "${EXT_MSODBC_SO_SRCPATH}" STREQUAL "")
      if(IS_DIRECTORY "${EXT_MSODBC_SO_SRCPATH}")
        set(_msodbc_runtime_dir "${EXT_MSODBC_SO_SRCPATH}")
      else()
        get_filename_component(_msodbc_runtime_dir "${EXT_MSODBC_SO_SRCPATH}" DIRECTORY)
      endif()
      _stage_fq_provider_runtime("${_msodbc_runtime_dir}"
        "libmsodbcsql*.so*" "libmsodbcsql*.dylib*")
    endif()

    # Oracle Instant Client ODBC driver: the driver (libsqora) and the Instant
    # Client base libraries it resolves by soname (libclntsh and friends) are
    # staged together, so the provider can preload them with RTLD_LOCAL
    # without ORACLE_HOME or ld.so.conf (Oracle DS 3.3 / 9.1.7).
    # Staged with the link-preserving mode: the bundle ships libclntsh and
    # libclntshcore once each under eight versioned aliases, and dereferencing
    # them would put ~1.2 GB of duplicate copies into build/lib (and from there
    # into the package).
    if(TARGET ext_oracle)
      if(NOT TD_EXTERNALS_USE_ONLY)
        add_dependencies(extconnector ext_oracle)
      endif()
      set(_oracle_runtime_dir "${ext_oracle_install}/lib")
    elseif(DEFINED EXT_ORACLE_SO_SRCPATH AND NOT "${EXT_ORACLE_SO_SRCPATH}" STREQUAL "")
      # No ExternalProject target: EXT_ORACLE_SO_SRCPATH is a user-supplied
      # path, either the driver directory or the driver .so itself.
      if(IS_DIRECTORY "${EXT_ORACLE_SO_SRCPATH}")
        set(_oracle_runtime_dir "${EXT_ORACLE_SO_SRCPATH}")
      else()
        get_filename_component(_oracle_runtime_dir "${EXT_ORACLE_SO_SRCPATH}" DIRECTORY)
      endif()
    endif()

    # A bundle root may carry the two generations as subdirectories; explicit
    # EXT_ORACLE_19C_SRCPATH / EXT_ORACLE_23AI_SRCPATH win over it.
    if(TARGET ext_oracle_19c)
      if(NOT TD_EXTERNALS_USE_ONLY)
        add_dependencies(extconnector ext_oracle_19c)
      endif()
      set(_oracle_19c_dir "${ext_oracle_19c_install}/lib")
    endif()
    if(DEFINED _oracle_runtime_dir AND IS_DIRECTORY "${_oracle_runtime_dir}/19c")
      set(_oracle_19c_dir "${_oracle_runtime_dir}/19c")
    endif()
    if(DEFINED _oracle_runtime_dir AND IS_DIRECTORY "${_oracle_runtime_dir}/23ai")
      set(_oracle_23ai_dir "${_oracle_runtime_dir}/23ai")
    endif()
    if(DEFINED EXT_ORACLE_19C_SRCPATH AND NOT "${EXT_ORACLE_19C_SRCPATH}" STREQUAL "")
      if(IS_DIRECTORY "${EXT_ORACLE_19C_SRCPATH}")
        set(_oracle_19c_dir "${EXT_ORACLE_19C_SRCPATH}")
      else()
        get_filename_component(_oracle_19c_dir "${EXT_ORACLE_19C_SRCPATH}" DIRECTORY)
      endif()
    endif()
    if(DEFINED EXT_ORACLE_23AI_SRCPATH AND NOT "${EXT_ORACLE_23AI_SRCPATH}" STREQUAL "")
      if(IS_DIRECTORY "${EXT_ORACLE_23AI_SRCPATH}")
        set(_oracle_23ai_dir "${EXT_ORACLE_23AI_SRCPATH}")
      else()
        get_filename_component(_oracle_23ai_dir "${EXT_ORACLE_23AI_SRCPATH}" DIRECTORY)
      endif()
    endif()
    if(NOT DEFINED _oracle_23ai_dir AND DEFINED _oracle_runtime_dir)
      set(_oracle_23ai_dir "${_oracle_runtime_dir}")
    endif()

    if(DEFINED _oracle_19c_dir OR DEFINED _oracle_23ai_dir)
      if(DEFINED _oracle_19c_dir)
        _stage_fq_oracle_runtime("${_oracle_19c_dir}" 19c)
      endif()
      if(DEFINED _oracle_23ai_dir)
        _stage_fq_oracle_runtime("${_oracle_23ai_dir}" 23ai)
      endif()
      if(DEFINED _oracle_23ai_dir)
        set(_oracle_active 23ai)
      else()
        set(_oracle_active 19c)
      endif()
      file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/build/driver/oracle")
      file(WRITE "${CMAKE_BINARY_DIR}/build/driver/oracle/active" "${_oracle_active}\n")
    elseif(DEFINED _oracle_runtime_dir)
      # Legacy single-client layout: keep the flat staging so an existing
      # dependency package and build tree continue to work.
      _stage_fq_provider_runtime_mode(copy-glob-links "${_oracle_runtime_dir}"
        "libsqora*.so*" "libclntsh*.so*" "libclntshcore*.so*" "libnnz*.so*"
        "libons*.so*" "libnfsodm*.so*" "liborazt*.so*" "libociei*.so*"
        "legacy.so*" "fips*.so*" "libaio*.so*"
        "libsqora*.dylib*" "libclntsh*.dylib*")
    endif()

    if(TARGET ext_msodbc)
      set(_msodbc_resource_libdir "${ext_msodbc_install}/lib")
    elseif(DEFINED _msodbc_runtime_dir)
      set(_msodbc_resource_libdir "${_msodbc_runtime_dir}")
    endif()
    if(DEFINED _msodbc_resource_libdir)
      add_custom_command(TARGET extconnector POST_BUILD
        COMMAND /bin/bash "${_stage_runtime_libs_script}"
          copy-msodbc-resources "${_msodbc_resource_libdir}" "${_td_runtime_lib_dir}"
        VERBATIM
      )
    endif()
  endif()
endif()

if(TD_ENTERPRISE AND TD_WINDOWS AND INSTALL_FQ_RUNTIME_LIBS)
  # Provider client libraries are loaded at runtime, so the dependency package
  # must ship their DLLs next to taosd.  The list is resolved at build time
  # because MariaDB Connector/C is produced by an ExternalProject.
  set(_fq_dll_dirs "")
  if(BUILD_WITH_MARIADB AND TARGET ext_mariadb)
    if(NOT TD_EXTERNALS_USE_ONLY)
      add_dependencies(extconnector ext_mariadb)
    endif()
    list(APPEND _fq_dll_dirs
      "${ext_mariadb_install}/bin"
      "${ext_mariadb_install}/lib/mariadb"
      "${ext_mariadb_install}/lib/mariadb/plugin")
  endif()
  if(BUILD_WITH_LIBPQ)
    if(DEFINED EXT_LIBPQ_DLL_DIR AND NOT "${EXT_LIBPQ_DLL_DIR}" STREQUAL "")
      list(APPEND _fq_dll_dirs "${EXT_LIBPQ_DLL_DIR}")
    endif()
    if(TARGET ext_libpq)
      if(NOT TD_EXTERNALS_USE_ONLY)
        add_dependencies(extconnector ext_libpq)
      endif()
      list(APPEND _fq_dll_dirs "${ext_libpq_install}/bin" "${ext_libpq_install}/lib")
    endif()
  endif()
  # Oracle Instant Client for Windows: the ODBC driver (sqora32) and the base
  # DLLs it loads (oraociei, orannz, orasql, oci, ...) must sit in the same
  # directory.  The provider finds that directory through the registry
  # registration written by the setup script, then preloads the base DLLs from
  # it before the driver manager loads the driver.  odbc_install.exe is copied
  # too: the dependency package documents it as the official way to register
  # the packaged driver/ directory.
  if(BUILD_WITH_ODBC)
    set(_oracle_windows_dir "")
    if(TARGET ext_oracle)
      if(NOT TD_EXTERNALS_USE_ONLY)
        add_dependencies(extconnector ext_oracle)
      endif()
      set(_oracle_windows_dir "${ext_oracle_install}/lib")
    elseif(DEFINED EXT_ORACLE_SO_SRCPATH AND NOT "${EXT_ORACLE_SO_SRCPATH}" STREQUAL "")
      if(IS_DIRECTORY "${EXT_ORACLE_SO_SRCPATH}")
        set(_oracle_windows_dir "${EXT_ORACLE_SO_SRCPATH}")
      else()
        get_filename_component(_oracle_windows_dir "${EXT_ORACLE_SO_SRCPATH}" DIRECTORY)
      endif()
    endif()
    if(_oracle_windows_dir)
      list(APPEND _fq_dll_dirs "${_oracle_windows_dir}")
    endif()
  endif()
  if(_fq_dll_dirs)
    string(JOIN ";" _fq_dll_dirs_str ${_fq_dll_dirs})
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}"
        "-DOUT_DIR=${_td_runtime_lib_dir}"
        "-DSEARCH_DIRS=${_fq_dll_dirs_str}"
        "-DNAME_PATTERNS=^libmariadb;^libmysql;^libpq;^libssl;^libcrypto;^libintl;^libiconv;^libwinpthread;^caching_sha2_password[.]dll$;^sha256_password[.]dll$;^client_ed25519[.]dll$;^dialog[.]dll$;^mysql_clear_password[.]dll$;^pvio_shmem[.]dll$;^sqora;^sqres;^oraociei;^orannz[.]dll$;^orasql[.]dll$;^oraocci;^oci[.]dll$;^ociw32[.]dll$;^legacy[.]dll$;^fips;^extks[.]dll$;^pkcs11[.]dll$"
        -P "${TD_SOURCE_DIR}/cmake/stage_fq_runtime_windows.cmake"
      COMMENT "Staging federated-query provider runtime DLLs -> build/bin/"
      VERBATIM
    )
  endif()
  if(BUILD_WITH_ODBC AND _oracle_windows_dir AND
     (TARGET ext_oracle OR EXISTS "${_oracle_windows_dir}/odbc_install.exe"))
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_td_runtime_lib_dir}"
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
        "${_oracle_windows_dir}/odbc_install.exe"
        "${_td_runtime_lib_dir}/odbc_install.exe"
      COMMENT "Staging the Oracle Instant Client ODBC registration helper -> build/bin/"
      VERBATIM
    )
  endif()
endif()

if(TD_ENTERPRISE AND TD_WINDOWS AND INSTALL_FQ_RUNTIME_LIBS AND BUILD_WITH_ODBC)
  # Windows federated-query dependency package: the ODBC driver manager resolves
  # drivers through its registry registration, so the package carries the
  # official Microsoft ODBC Driver 18 installer (MSI) next to the other staged
  # provider runtimes, mirroring the Linux libmsodbcsql*.so staging below.
  # EXT_MSODBC_MSI may be produced by the ext_msodbc ExternalProject at build
  # time, so it does not necessarily exist yet while the configure step runs.
  # Depend on that target instead of testing EXISTS here; the copy then runs
  # after the MSI has been downloaded.
  if(DEFINED EXT_MSODBC_MSI AND NOT "${EXT_MSODBC_MSI}" STREQUAL "")
    if(TARGET ext_msodbc)
      add_dependencies(extconnector ext_msodbc)
    endif()
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_td_runtime_lib_dir}"
      COMMAND "${CMAKE_COMMAND}" -E copy_if_different
        "${EXT_MSODBC_MSI}" "${_td_runtime_lib_dir}/msodbcsql18.msi"
      COMMENT "Staging federated-query SQL Server driver (MSI) -> build/bin/"
      VERBATIM
    )
  else()
    message(STATUS
      "ExtConnector: INSTALL_FQ_RUNTIME_LIBS=ON but EXT_MSODBC_MSI is unset — "
      "the Windows federated-query package will not carry the SQL Server ODBC driver")
  endif()
endif()

if(TD_ENTERPRISE AND BUILD_WITH_ARROW)
  # On Windows the Arrow Flight SQL transport is not built (no gRPC / Arrow
  # Flight there), so the plugin is built with only the HTTP transport.  The
  # provider registration and dispatch layer are identical on every platform.
  add_library(taos_ext_influx_arrow SHARED
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInfluxDispatch.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInfluxHttp.cpp
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInflux.cpp
  )

  target_compile_features(taos_ext_influx_arrow PRIVATE cxx_std_17)
  if(TD_WINDOWS)
    # The HTTP transport uses C++20 designated initializers, which MSVC only
    # accepts with /std:c++20 (GCC/Clang accept them as an extension in C++17).
    set_property(TARGET taos_ext_influx_arrow PROPERTY CXX_STANDARD 20)
    set_property(TARGET taos_ext_influx_arrow PROPERTY CXX_STANDARD_REQUIRED ON)
  endif()
  target_include_directories(
    taos_ext_influx_arrow
    PUBLIC  "${TD_SOURCE_DIR}/include/libs/extconnector"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
  )
  target_compile_definitions(
    taos_ext_influx_arrow
    PRIVATE HAVE_ARROW
  )
  if(NOT TD_WINDOWS)
    target_compile_definitions(
      taos_ext_influx_arrow
      PRIVATE HAVE_ARROW_PARQUET HAVE_ARROW_FLIGHT_SQL
    )
  endif()
  target_link_libraries(
    taos_ext_influx_arrow
    PRIVATE extconnector os util common qcom nodes crypt
  )
  if(NOT TD_WINDOWS)
    DEP_ext_arrow(taos_ext_influx_arrow)
  endif()
  DEP_ext_curl(taos_ext_influx_arrow)
  DEP_ext_cjson(taos_ext_influx_arrow)

  set_target_properties(
    taos_ext_influx_arrow
    PROPERTIES
      OUTPUT_NAME "taos_ext_influx_arrow"
      LIBRARY_OUTPUT_DIRECTORY "${_td_runtime_lib_dir}"
      RUNTIME_OUTPUT_DIRECTORY "${_td_runtime_lib_dir}"
  )
  if(TD_DARWIN)
    set_target_properties(taos_ext_influx_arrow PROPERTIES BUILD_RPATH "@loader_path" INSTALL_RPATH "@loader_path")
  elseif(NOT TD_WINDOWS)
    set_target_properties(taos_ext_influx_arrow PROPERTIES BUILD_RPATH "$ORIGIN" INSTALL_RPATH "$ORIGIN")
  endif()

  if(TD_WINDOWS)
    # No Arrow runtime is staged on Windows (HTTP transport only).
  elseif(EXT_ARROW_SYSTEM_LIBDIR)
    set(_arrow_libdir "${EXT_ARROW_SYSTEM_LIBDIR}")
    if(TD_DARWIN)
      if(EXISTS "${_arrow_libdir}/libarrow.${EXT_ARROW_SO_SUFFIX}")
        set(_arrow_so_suffix "${EXT_ARROW_SO_SUFFIX}")
      elseif(EXISTS "${_arrow_libdir}/libarrow.dylib")
        set(_arrow_so_suffix "dylib")
      elseif(EXISTS "${_arrow_libdir}/libarrow.2400.dylib")
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

  if(NOT TD_WINDOWS)
  foreach(_arrow_lib libarrow libarrow_flight libarrow_flight_sql libparquet)
    set(_arrow_stage_name "${_arrow_lib}.${_arrow_so_suffix}")
    if(TD_DARWIN)
      set(_arrow_link_name "${_arrow_lib}.dylib")
    else()
      set(_arrow_link_name "${_arrow_lib}.so")
    endif()
    if(_arrow_stage_name STREQUAL _arrow_link_name)
      add_custom_command(TARGET taos_ext_influx_arrow POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_arrow_libdir}/${_arrow_stage_name}"
          "${_td_runtime_lib_dir}/${_arrow_stage_name}"
        COMMENT "Staging ${_arrow_stage_name} -> build/lib/"
        VERBATIM
      )
    else()
      add_custom_command(TARGET taos_ext_influx_arrow POST_BUILD
        COMMAND "${CMAKE_COMMAND}" -E copy_if_different
          "${_arrow_libdir}/${_arrow_stage_name}"
          "${_td_runtime_lib_dir}/${_arrow_stage_name}"
        COMMAND "${CMAKE_COMMAND}" -E create_symlink
          "${_arrow_stage_name}"
          "${_td_runtime_lib_dir}/${_arrow_link_name}"
        COMMENT "Staging ${_arrow_stage_name} -> build/lib/"
        VERBATIM
      )
    endif()
  endforeach()
  endif()
  if(TD_WINDOWS)
    # Windows: stage curl/cjson runtime next to the plugin (build/bin). The
    # Arrow runtime is not part of the Windows build.
    add_custom_command(TARGET taos_ext_influx_arrow POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_BINARY_DIR}/build/bin"
      COMMENT "Staging Influx HTTP runtime -> build/bin/"
      VERBATIM
    )
  elseif(TD_DARWIN)
    add_custom_command(TARGET taos_ext_influx_arrow POST_BUILD
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        copy-ldd
        "$<TARGET_FILE:taos_ext_influx_arrow>"
        "${_td_runtime_lib_dir}"
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        fix-darwin-rpaths
        "${_td_runtime_lib_dir}"
      COMMENT "Staging Influx Arrow runtime dependencies -> build/lib/"
      VERBATIM
    )
  else()
    add_custom_command(TARGET taos_ext_influx_arrow POST_BUILD
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        copy-ldd
        "$<TARGET_FILE:taos_ext_influx_arrow>"
        "${_td_runtime_lib_dir}"
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        fix-darwin-rpaths
        "${_td_runtime_lib_dir}"
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        fix-linux-rpaths
        "${_td_runtime_lib_dir}"
      COMMENT "Staging Influx Arrow runtime dependencies -> build/lib/"
      VERBATIM
    )
  endif()
  message(STATUS "ExtConnector: building optional libtaos_ext_influx_arrow runtime plugin")
endif()

if(BUILD_TEST AND TD_ENTERPRISE)
  add_subdirectory(test)
endif()
