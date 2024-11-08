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

  if(${BUILD_WITH_S3})
    add_definitions(-DUSE_S3)
  ELSEIF(${BUILD_WITH_COS})
    add_definitions(-DUSE_COS)
  endif()

  if(${BUILD_WITH_ANALYSIS})
    add_definitions(-DUSE_ANALYTICS)
  endif()
endif()

add_library(mnode STATIC ${MNODE_SRC})
target_include_directories(
  mnode
  PUBLIC "${TD_SOURCE_DIR}/include/dnode/mnode"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)
target_link_libraries(
  mnode scheduler sdb wal transport cjson sync monitor executor qworker stream parser audit monitorfw
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
