add_subdirectory(node_mgmt)
add_subdirectory(node_util)
add_subdirectory(mgmt_mnode)
add_subdirectory(mgmt_qnode)
add_subdirectory(mgmt_snode)
add_subdirectory(mgmt_xnode)
add_subdirectory(mgmt_bnode)
add_subdirectory(mgmt_vnode)
add_subdirectory(mgmt_dnode)
add_subdirectory(test)

aux_source_directory(exe EXEC_SRC)
if(BUILD_TAOSD_INTEGRATED)
  add_library(taosd ${EXEC_SRC})
else()
  add_executable(taosd ${EXEC_SRC})
endif()
target_include_directories(
  taosd
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/node_mgmt/inc"
)

target_link_libraries(taosd PUBLIC dnode crypt)

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  include(CheckLinkerFlag)
  check_linker_flag(C "-Wl,-no_warn_duplicate_libraries" HAVE_NO_WARN_DUP_LIBS)
  if(HAVE_NO_WARN_DUP_LIBS)
    target_link_options(taosd PRIVATE -Wl,-no_warn_duplicate_libraries)
  endif()
endif()

if(TD_ENTERPRISE)
  if(BUILD_WITH_S3)
    add_definitions(-DUSE_S3)
  elseif(BUILD_WITH_COS)
    add_definitions(-DUSE_COS)
  endif()
endif()

