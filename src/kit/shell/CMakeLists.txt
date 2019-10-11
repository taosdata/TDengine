CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(TDengine)

IF (TD_LINUX)

  INCLUDE_DIRECTORIES(inc ${TD_ROOT_DIR}/src/inc ${TD_ROOT_DIR}/src/client/inc ${TD_OS_DIR}/inc)

  AUX_SOURCE_DIRECTORY(./src SRC)
  LIST(REMOVE_ITEM SRC ./src/shellWindows.c)
  
  ADD_EXECUTABLE(shell ${SRC})  
  TARGET_LINK_LIBRARIES(shell taos_static)
  SET_TARGET_PROPERTIES(shell PROPERTIES OUTPUT_NAME "taos")
  
ELSEIF (TD_WINDOWS)

  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/deps/pthread)
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/deps/regex)
  INCLUDE_DIRECTORIES(inc ${TD_ROOT_DIR}/src/inc ${TD_ROOT_DIR}/src/client/inc ${TD_OS_DIR}/inc)

  LIST(APPEND SRC ./src/shellEngine.c)
  LIST(APPEND SRC ./src/shellMain.c)
  LIST(APPEND SRC ./src/shellWindows.c)

  ADD_EXECUTABLE(shell ${SRC})
  TARGET_LINK_LIBRARIES(shell taos_static)
  SET_TARGET_PROPERTIES(shell PROPERTIES OUTPUT_NAME "taos")

ELSEIF (TD_DARWIN)

  INCLUDE_DIRECTORIES(inc ${TD_ROOT_DIR}/src/inc ${TD_ROOT_DIR}/src/client/inc ${TD_OS_DIR}/inc)

  LIST(APPEND SRC ./src/shellEngine.c)
  LIST(APPEND SRC ./src/shellMain.c)
  LIST(APPEND SRC ./src/shellWindows.c)

  ADD_EXECUTABLE(shell ${SRC})
  TARGET_LINK_LIBRARIES(shell taos_static)
  SET_TARGET_PROPERTIES(shell PROPERTIES OUTPUT_NAME "taos")

ENDIF ()

