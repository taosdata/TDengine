aux_source_directory(src IMPLEMENT_SRC)
add_library(dnode STATIC ${IMPLEMENT_SRC})
target_link_libraries(
  dnode PUBLIC mgmt_mnode mgmt_qnode mgmt_snode mgmt_bnode mgmt_vnode mgmt_dnode monitorfw tss crypt
)

if(TD_ENTERPRISE AND TD_HAS_OPENSSL)
  # Link taosk library for encryption key management (requires OpenSSL)
  target_link_libraries(dnode PUBLIC taosk)
endif()

IF(TD_ENTERPRISE)
  IF(${BUILD_WITH_S3})
    add_definitions(-DUSE_S3)
  ELSEIF(${BUILD_WITH_COS})
    add_definitions(-DUSE_COS)
  ENDIF()
ENDIF()

IF(DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
ENDIF()

target_include_directories(
  dnode
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if(TD_ENTERPRISE AND TD_HAS_OPENSSL)
  # Include taosk headers for encryption key management (requires OpenSSL)
  target_include_directories(
    dnode
    PRIVATE "${TD_ENTERPRISE_DIR}/src/plugins/taosk/inc"
  )
endif()
