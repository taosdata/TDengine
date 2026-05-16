PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/source/client/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/source/dnode/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/source/mnode/detail/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/source/vnode/detail/inc)
INCLUDE_DIRECTORIES(${TD_ENTERPRISE_DIR}/source/modules/multilevel_storage/inc)
INCLUDE_DIRECTORIES(inc)

IF(TD_LINUX)
  AUX_SOURCE_DIRECTORY(. SRC)
  ADD_EXECUTABLE(taosscan ${SRC})
  TARGET_LINK_LIBRARIES(taosscan taos_static os tutil mstorage trpc)
ENDIF()
