PROJECT(TDengine)

IF (TD_LINUX)
  INCLUDE_DIRECTORIES(. ${TD_COMMUNITY_DIR}/src/inc ${TD_COMMUNITY_DIR}/src/client/inc  ${TD_COMMUNITY_DIR}/inc)
  AUX_SOURCE_DIRECTORY(. SRC)
  ADD_EXECUTABLE(demo apitest.c)
  TARGET_LINK_LIBRARIES(demo taos_static trpc tutil pthread )
ENDIF ()
