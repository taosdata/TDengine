CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/client/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/libcurl/include)

IF (TD_LINUX)
  AUX_SOURCE_DIRECTORY(. SRC)
  ADD_EXECUTABLE(taosdemox ${SRC})

  #find_program(HAVE_CURL NAMES curl)
  IF ((NOT TD_ARM_64) AND (NOT TD_ARM_32))
    ADD_DEFINITIONS(-DTD_LOWA_CURL)
    LINK_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/libcurl/lib)
    ADD_LIBRARY(curl STATIC IMPORTED)
    SET_PROPERTY(TARGET curl PROPERTY IMPORTED_LOCATION ${TD_COMMUNITY_DIR}/deps/libcurl/lib/libcurl.a)
    TARGET_LINK_LIBRARIES(taosdemox curl)
  ENDIF ()

  IF (TD_SOMODE_STATIC)
    TARGET_LINK_LIBRARIES(taosdemox taos_static cJson)
  ELSE ()
    TARGET_LINK_LIBRARIES(taosdemox taos cJson)
  ENDIF ()
ENDIF ()

IF (TD_DARWIN)
  # missing a few dependencies, such as <argp.h>
  # AUX_SOURCE_DIRECTORY(. SRC)
  # ADD_EXECUTABLE(taosdemox ${SRC})
  #
  # #find_program(HAVE_CURL NAMES curl)
  # IF ((NOT TD_ARM_64) AND (NOT TD_ARM_32))
  #   ADD_DEFINITIONS(-DTD_LOWA_CURL)
  #   LINK_DIRECTORIES(${TD_COMMUNITY_DIR}/deps/libcurl/lib)
  #   ADD_LIBRARY(curl STATIC IMPORTED)
  #   SET_PROPERTY(TARGET curl PROPERTY IMPORTED_LOCATION ${TD_COMMUNITY_DIR}/deps/libcurl/lib/libcurl.a)
  #   TARGET_LINK_LIBRARIES(taosdemox curl)
  # ENDIF ()
  #
  # IF (TD_SOMODE_STATIC)
  #   TARGET_LINK_LIBRARIES(taosdemox taos_static cJson)
  # ELSE ()
  #   TARGET_LINK_LIBRARIES(taosdemox taos cJson)
  # ENDIF ()
ENDIF ()

