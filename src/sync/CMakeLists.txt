CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(inc)
AUX_SOURCE_DIRECTORY(src SRC)
  
IF (TD_LINUX)
  LIST(REMOVE_ITEM SRC src/syncArbitrator.c)
  ADD_LIBRARY(sync ${SRC})
  TARGET_LINK_LIBRARIES(sync tutil pthread common)

  LIST(APPEND BIN_SRC src/syncArbitrator.c)
  LIST(APPEND BIN_SRC src/syncTcp.c)
  ADD_EXECUTABLE(tarbitrator ${BIN_SRC})
  TARGET_LINK_LIBRARIES(tarbitrator sync common osdetail tutil)

  #ADD_SUBDIRECTORY(test)
ENDIF ()
