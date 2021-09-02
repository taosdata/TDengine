CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(inc)
INCLUDE_DIRECTORIES(jni)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/query/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/zlib-1.2.11/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/plugins/http/inc)
AUX_SOURCE_DIRECTORY(src SRC)

IF (TD_LINUX)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/jni/linux)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/cJson/inc)

  # set the static lib name
  ADD_LIBRARY(taos_static STATIC ${SRC})
  TARGET_LINK_LIBRARIES(taos_static common query trpc tutil pthread cJson m rt ${VAR_TSZ})
  SET_TARGET_PROPERTIES(taos_static PROPERTIES OUTPUT_NAME "taos_static")
  SET_TARGET_PROPERTIES(taos_static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

  # generate dynamic library (*.so)
  ADD_LIBRARY(taos SHARED ${SRC})
  TARGET_LINK_LIBRARIES(taos common query trpc cJson tutil pthread m rt)
  IF (TD_LINUX_64)
    TARGET_LINK_LIBRARIES(taos lua cJson)
  ENDIF ()

  SET_TARGET_PROPERTIES(taos PROPERTIES CLEAN_DIRECT_OUTPUT 1)

  #set version of .so
  #VERSION    so version
  #SOVERSION  api version
  #MESSAGE(STATUS "build version ${TD_VER_NUMBER}")
  SET_TARGET_PROPERTIES(taos PROPERTIES VERSION ${TD_VER_NUMBER} SOVERSION 1)

  ADD_SUBDIRECTORY(tests)

ELSEIF (TD_DARWIN)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/jni/linux)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/cJson/inc)

  # set the static lib name
  ADD_LIBRARY(taos_static STATIC ${SRC})
  TARGET_LINK_LIBRARIES(taos_static common query trpc cJson tutil pthread m lua cJson)
  SET_TARGET_PROPERTIES(taos_static PROPERTIES OUTPUT_NAME "taos_static")
  SET_TARGET_PROPERTIES(taos_static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

  # generate dynamic library (*.dylib)
  ADD_LIBRARY(taos SHARED ${SRC})
  TARGET_LINK_LIBRARIES(taos common query cJson trpc tutil pthread m lua)
  SET_TARGET_PROPERTIES(taos PROPERTIES CLEAN_DIRECT_OUTPUT 1)

  #set version of .dylib
  #VERSION    dylib version
  #SOVERSION  dylib version
  #MESSAGE(STATUS "build version ${TD_VER_NUMBER}")
  SET_TARGET_PROPERTIES(taos PROPERTIES VERSION ${TD_VER_NUMBER} SOVERSION 1)

  ADD_SUBDIRECTORY(tests)

ELSEIF (TD_WINDOWS)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/jni/windows)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/jni/windows/win32)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/cJson/inc)

  CONFIGURE_FILE("${TD_COMMUNITY_DIR}/src/client/src/taos.rc.in" "${TD_COMMUNITY_DIR}/src/client/src/taos.rc")

  ADD_LIBRARY(taos_static STATIC ${SRC})
  TARGET_LINK_LIBRARIES(taos_static trpc cJson tutil cJson query)

  # generate dynamic library (*.dll)
  ADD_LIBRARY(taos SHARED ${SRC} ${TD_COMMUNITY_DIR}/src/client/src/taos.rc)
  IF (NOT TD_GODLL)
    SET_TARGET_PROPERTIES(taos PROPERTIES LINK_FLAGS /DEF:${TD_COMMUNITY_DIR}/src/client/src/taos.def)
  ENDIF ()
  TARGET_LINK_LIBRARIES(taos trpc cJson tutil query lua)

ELSEIF (TD_DARWIN)
  SET(CMAKE_MACOSX_RPATH 1)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/jni/linux)
  INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/cJson/inc)

  ADD_LIBRARY(taos_static STATIC ${SRC})
  TARGET_LINK_LIBRARIES(taos_static query trpc cJson tutil pthread cJson m lua)
  SET_TARGET_PROPERTIES(taos_static PROPERTIES OUTPUT_NAME "taos_static")

  # generate dynamic library (*.dylib)
  ADD_LIBRARY(taos SHARED ${SRC})
  TARGET_LINK_LIBRARIES(taos query trpc cJson tutil pthread m lua)

  SET_TARGET_PROPERTIES(taos PROPERTIES CLEAN_DIRECT_OUTPUT 1)

  #set version of .so
  #VERSION    so version
  #SOVERSION  api version
  #MESSAGE(STATUS "build version ${TD_VER_NUMBER}")
  SET_TARGET_PROPERTIES(taos PROPERTIES VERSION ${TD_VER_NUMBER} SOVERSION 1)
ENDIF ()

