aux_source_directory(src EXT_CONNECTOR_SRC)

if(TD_ENTERPRISE)
  list(APPEND EXT_CONNECTOR_SRC
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnector.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorQuery.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorRuntime.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInfluxPlugin.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorMySQL.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorPG.c
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

if(TD_ENTERPRISE AND UNIX AND NOT APPLE)
  target_link_options(extconnector INTERFACE "LINKER:--undefined=nodesRemotePlanToSQL")
endif()

if(TD_ENTERPRISE)
  if(BUILD_WITH_MARIADB)
    target_compile_definitions(extconnector PRIVATE HAVE_MARIADB_C)
    message(STATUS "ExtConnector: MySQL provider enabled with runtime libmariadb loading")
  else()
    message(STATUS "ExtConnector: BUILD_WITH_MARIADB=OFF — MySQL provider disabled")
  endif()

  if(BUILD_WITH_LIBPQ)
    target_compile_definitions(extconnector PRIVATE HAVE_LIBPQ)
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

  target_link_libraries(extconnector PRIVATE crypt)
endif()

if(TD_ENTERPRISE AND NOT TD_WINDOWS)
  set(_td_runtime_lib_dir "${CMAKE_BINARY_DIR}/build/lib")
  set(_stage_runtime_libs_script "${TD_SOURCE_DIR}/cmake/stage_runtime_libs.sh")
endif()

if(TD_ENTERPRISE AND NOT TD_WINDOWS AND INSTALL_FQ_RUNTIME_LIBS)
  function(_stage_fq_provider_runtime src_dir)
    add_custom_command(TARGET extconnector POST_BUILD
      COMMAND "${CMAKE_COMMAND}" -E make_directory "${_td_runtime_lib_dir}"
      COMMAND /bin/bash "${_stage_runtime_libs_script}"
        copy-glob
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
      COMMENT "Staging federated-query provider runtime libraries -> build/lib/"
      VERBATIM
    )
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
endif()

if(TD_ENTERPRISE AND BUILD_WITH_ARROW AND NOT TD_WINDOWS)
  add_library(taos_ext_influx_arrow SHARED
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInfluxDispatch.c
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInfluxHttp.cpp
    ${TD_ENTERPRISE_DIR}/source/plugins/extsource/src/extConnectorInflux.cpp
  )

  target_compile_features(taos_ext_influx_arrow PRIVATE cxx_std_17)
  target_include_directories(
    taos_ext_influx_arrow
    PUBLIC  "${TD_SOURCE_DIR}/include/libs/extconnector"
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
  )
  target_compile_definitions(
    taos_ext_influx_arrow
    PRIVATE HAVE_ARROW HAVE_ARROW_PARQUET HAVE_ARROW_FLIGHT_SQL
  )
  target_link_libraries(
    taos_ext_influx_arrow
    PRIVATE extconnector os util common qcom nodes crypt
  )
  DEP_ext_arrow(taos_ext_influx_arrow)
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
  else()
    set_target_properties(taos_ext_influx_arrow PROPERTIES BUILD_RPATH "$ORIGIN" INSTALL_RPATH "$ORIGIN")
  endif()

  if(EXT_ARROW_SYSTEM_LIBDIR)
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
  if(TD_DARWIN)
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
      COMMENT "Staging Influx Arrow runtime dependencies -> build/lib/"
      VERBATIM
    )
  endif()

  message(STATUS "ExtConnector: building optional libtaos_ext_influx_arrow runtime plugin")
endif()
