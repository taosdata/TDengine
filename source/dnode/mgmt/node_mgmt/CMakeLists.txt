aux_source_directory(src IMPLEMENT_SRC)
add_library(dnode STATIC ${IMPLEMENT_SRC})
target_link_libraries(
    dnode mgmt_mnode mgmt_qnode mgmt_snode mgmt_vnode mgmt_dnode monitorfw
)

IF (TD_STORAGE)

  IF(${BUILD_WITH_S3})
    add_definitions(-DUSE_S3)
  ELSEIF(${BUILD_WITH_COS})
    add_definitions(-DUSE_COS)
  ENDIF()

ENDIF ()

IF (DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
ENDIF()

target_include_directories(
    dnode
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
