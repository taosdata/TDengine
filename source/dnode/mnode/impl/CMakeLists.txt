aux_source_directory(src MNODE_SRC)

if(TD_PRIVILEGE)
  ADD_DEFINITIONS(-D_PRIVILEGE)
endif()

if(TD_ENTERPRISE)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/src/plugins/privilege/src/privilege.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/src/plugins/mnode/src/mndDb.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/src/plugins/mnode/src/mndVgroup.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/src/plugins/mnode/src/mndDnode.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/src/plugins/view/src/mndView.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/src/plugins/mnode/src/mndMount.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/src/plugins/token/src/mndTokenImpl.c)
  LIST(APPEND MNODE_SRC ${TD_ENTERPRISE_DIR}/src/plugins/xnode/src/mndXnodeImpl.c)

  if(${BUILD_WITH_S3})
    add_definitions(-DUSE_S3)
  ELSEIF(${BUILD_WITH_COS})
    add_definitions(-DUSE_COS)
  endif()
endif()

if(${BUILD_WITH_ANALYSIS})
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
  PRIVATE "${TD_ENTERPRISE_DIR}/src/plugins/privilege/inc"
)

if(TD_ENTERPRISE)
  target_include_directories(
    mnode
    PRIVATE "${TD_ENTERPRISE_DIR}/src/plugins/taosk/inc"
  )
endif()
target_link_libraries(
  mnode scheduler sdb wal transport sync monitor executor qworker new-stream parser audit monitorfw totp xnode txnode mgmt_mnode
)

if(DEFINED GRANT_CFG_INCLUDE_DIR)
  add_definitions(-DGRANTS_CFG)
endif()

if(TD_GRANT)
  TARGET_LINK_LIBRARIES(mnode grant)
  ADD_DEFINITIONS(-D_GRANT)
endif()

if(${BUILD_TEST})
  add_subdirectory(test)
endif(${BUILD_TEST})
