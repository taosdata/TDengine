CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(./src SRC)
INCLUDE_DIRECTORIES(inc jni ${TD_ROOT_DIR}/src/inc ${TD_OS_DIR}/inc)

IF (TD_LINUX)
  # generate dynamic library (*.so)
  ADD_LIBRARY(taos SHARED ${SRC})
  ADD_LIBRARY(taos_static STATIC ${SRC})
  TARGET_LINK_LIBRARIES(taos_static trpc tutil pthread m rt)
  TARGET_LINK_LIBRARIES(taos trpc tutil pthread m rt) 

  # set the static lib name
  SET_TARGET_PROPERTIES(taos_static PROPERTIES OUTPUT_NAME "taos_static")

  # enable static lib and so exists
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/deps/jni/linux)
  SET_TARGET_PROPERTIES(taos PROPERTIES CLEAN_DIRECT_OUTPUT 1)
  SET_TARGET_PROPERTIES(taos_static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

  TARGET_LINK_LIBRARIES(taos_static trpc tutil pthread)
  TARGET_LINK_LIBRARIES(taos trpc tutil pthread)
  
  #set version of .so
  #VERSION    so version
  #SOVERSION  api version
  execute_process(COMMAND chmod 777 ${PROJECT_SOURCE_DIR}/../../packaging/tools/get_version.sh)
  execute_process(COMMAND ${PROJECT_SOURCE_DIR}/../../packaging/tools/get_version.sh
    OUTPUT_VARIABLE
    VERSION_INFO)
  MESSAGE(STATUS "build version ${VERSION_INFO}")
  SET_TARGET_PROPERTIES(taos PROPERTIES VERSION ${VERSION_INFO} SOVERSION 1)
  
ELSEIF (TD_WINDOWS)
  
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/deps/jni/windows)
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/deps/jni/windows/win32)
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/deps/pthread)  
  
  ADD_LIBRARY(taos_static STATIC ${SRC})
  TARGET_LINK_LIBRARIES(taos_static trpc tutil)
  
  ADD_LIBRARY(taos SHARED ${SRC})
  SET_TARGET_PROPERTIES(taos PROPERTIES LINK_FLAGS /DEF:${TD_ROOT_DIR}/src/client/src/taos.def)
  TARGET_LINK_LIBRARIES(taos trpc)

ENDIF ()

