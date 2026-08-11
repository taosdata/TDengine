aux_source_directory(src MNODE_SRC)

if(TD_PRIVILEGE)
  ADD_DEFINITIONS(-D_PRIVILEGE)
endif()

if(TD_ENTERPRISE)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/source/plugins/privilege/src/privilege.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/source/plugins/mnode/src/mndDb.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/source/plugins/mnode/src/mndVgroup.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/source/plugins/mnode/src/mndDnode.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/source/plugins/view/src/mndView.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/source/plugins/mnode/src/mndMount.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/source/plugins/mnode/src/mndExtSource.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/source/plugins/token/src/mndTokenImpl.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/source/plugins/xnode/src/mndXnodeImpl.c)

  if(BUILD_WITH_S3)
    add_definitions(-DUSE_S3)
  ELSEIF(BUILD_WITH_COS)
    add_definitions(-DUSE_COS)
  endif()
endif()

if(BUILD_WITH_ANALYSIS)
  add_definitions(-DUSE_ANALYTICS)
endif()

add_library(mnode STATIC ${MNODE_SRC})
target_include_directories(
  mnode
  PUBLIC 
  "${TD_SOURCE_DIR}/include/dnode/mnode"
  "${TD_SOURCE_DIR}/include/dnode/mgmt"
  "${TD_SOURCE_DIR}/include/dnode/xnode"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
  PUBLIC "${TD_SOURCE_DIR}/source/libs/totp/inc"
  "${TD_SOURCE_DIR}/include/libs/txnode/inc"
  PRIVATE "${TD_ENTERPRISE_DIR}/source/plugins/privilege/inc"
)

if(TD_ENTERPRISE)
  target_include_directories(
    mnode
    PRIVATE "${TD_ENTERPRISE_DIR}/source/plugins/taosk/inc"
    PRIVATE "${TD_SOURCE_DIR}/include/libs/extconnector"
    PRIVATE "${TD_SOURCE_DIR}/source/libs/extconnector/inc"
  )
  # extconnector is only referenced by mndExtSource.c (added to MNODE_SRC
  # above, TD_ENTERPRISE-only), so keep the link guarded the same way as its
  # include directories just above.
  target_link_libraries(mnode PUBLIC extconnector)
endif()
target_link_libraries(
  mnode PUBLIC scheduler sdb wal transport sync monitor executor qworker new-stream parser audit monitorfw totp xnode txnode mgmt_mnode
)

# libgsasl: application-layer SCRAM-SHA-256 authentication on the CONNECT handshake
if(${BUILD_LIBGSASL} AND ${TD_LINUX})
  DEP_ext_gsasl(mnode)
  add_definitions(-DUSE_LIBGSASL)
endif()

if(DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
endif()

if(TD_GRANT)
  TARGET_LINK_LIBRARIES(mnode PUBLIC grant)
  ADD_DEFINITIONS(-D_GRANT)
endif()

if(BUILD_TEST)
  add_subdirectory(test)
endif()
