PROJECT(TDengine)

IF (TD_WINDOWS_64 OR TD_WINDOWS_32)
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/deps/pthread)
ENDIF ()

INCLUDE_DIRECTORIES(. ${TD_ROOT_DIR}/src/inc ${TD_ROOT_DIR}/src/client/inc  ${TD_OS_DIR}/inc)
AUX_SOURCE_DIRECTORY(. SRC)
#ADD_EXECUTABLE(demo ${SRC})
#TARGET_LINK_LIBRARIES(demo taos_static trpc tutil pthread )






