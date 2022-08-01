CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/client/inc)

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
    add_dependencies(perfMonitor jemalloc)
ENDIF ()

IF (TD_LINUX)
  AUX_SOURCE_DIRECTORY(. SRC)
  ADD_EXECUTABLE(perfMonitor ${SRC})

  IF (TD_SOMODE_STATIC)
      TARGET_LINK_LIBRARIES(perfMonitor taos_static cJson lua)
  ELSE ()
      TARGET_LINK_LIBRARIES(perfMonitor taos cJson)
  ENDIF ()
ENDIF ()

