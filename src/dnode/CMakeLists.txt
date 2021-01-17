CMAKE_MINIMUM_REQUIRED(VERSION 3.5)
PROJECT(TDengine)

INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/query/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/mnode/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/tsdb/inc)
INCLUDE_DIRECTORIES(${TD_COMMUNITY_DIR}/src/sync/inc)
INCLUDE_DIRECTORIES(${TD_ENTERPRISE_DIR}/src/inc)

INCLUDE_DIRECTORIES(inc)
AUX_SOURCE_DIRECTORY(src SRC)

IF (TD_LINUX)
  ADD_EXECUTABLE(taosd ${SRC})
  TARGET_LINK_LIBRARIES(taosd mnode monitor http tsdb twal vnode cJson lz4 balance sync)

  IF (TD_SOMODE_STATIC)
    TARGET_LINK_LIBRARIES(taosd taos_static)
  ELSE ()
    TARGET_LINK_LIBRARIES(taosd taos)
  ENDIF ()

  IF (TD_ACCOUNT)
    TARGET_LINK_LIBRARIES(taosd account)
  ENDIF ()

  IF (TD_GRANT)
    TARGET_LINK_LIBRARIES(taosd grant)
  ENDIF ()

  IF (TD_MQTT)
    TARGET_LINK_LIBRARIES(taosd mqtt)
  ENDIF ()

  SET(PREPARE_ENV_CMD "prepare_env_cmd")
  SET(PREPARE_ENV_TARGET "prepare_env_target")
  ADD_CUSTOM_COMMAND(OUTPUT ${PREPARE_ENV_CMD}
      POST_BUILD
      COMMAND echo "make test directory"
      DEPENDS taosd
      COMMAND ${CMAKE_COMMAND} -E make_directory ${TD_TESTS_OUTPUT_DIR}/cfg/
      COMMAND ${CMAKE_COMMAND} -E make_directory ${TD_TESTS_OUTPUT_DIR}/log/
      COMMAND ${CMAKE_COMMAND} -E make_directory ${TD_TESTS_OUTPUT_DIR}/data/
      COMMAND ${CMAKE_COMMAND} -E echo dataDir ${TD_TESTS_OUTPUT_DIR}/data > ${TD_TESTS_OUTPUT_DIR}/cfg/taos.cfg
      COMMAND ${CMAKE_COMMAND} -E echo logDir ${TD_TESTS_OUTPUT_DIR}/log  >> ${TD_TESTS_OUTPUT_DIR}/cfg/taos.cfg
      COMMAND ${CMAKE_COMMAND} -E echo charset UTF-8  >> ${TD_TESTS_OUTPUT_DIR}/cfg/taos.cfg
      COMMENT "prepare taosd environment")
  ADD_CUSTOM_TARGET(${PREPARE_ENV_TARGET} ALL WORKING_DIRECTORY ${TD_EXECUTABLE_OUTPUT_PATH} DEPENDS ${PREPARE_ENV_CMD})
ENDIF ()

IF (TD_DARWIN)
  ADD_EXECUTABLE(taosd ${SRC})
  TARGET_LINK_LIBRARIES(taosd mnode monitor http tsdb twal vnode cJson lz4 balance sync)

  IF (TD_SOMODE_STATIC)
    TARGET_LINK_LIBRARIES(taosd taos_static)
  ELSE ()
    TARGET_LINK_LIBRARIES(taosd taos)
  ENDIF ()

  IF (TD_ACCOUNT)
    TARGET_LINK_LIBRARIES(taosd account)
  ENDIF ()

  IF (TD_GRANT)
    TARGET_LINK_LIBRARIES(taosd grant)
  ENDIF ()

  IF (TD_MQTT)
    TARGET_LINK_LIBRARIES(taosd mqtt)
  ENDIF ()

  # SET(PREPARE_ENV_CMD "prepare_env_cmd")
  # SET(PREPARE_ENV_TARGET "prepare_env_target")
  # ADD_CUSTOM_COMMAND(OUTPUT ${PREPARE_ENV_CMD}
  #     POST_BUILD
  #     COMMAND echo "make test directory"
  #     DEPENDS taosd
  #     COMMAND ${CMAKE_COMMAND} -E make_directory ${TD_TESTS_OUTPUT_DIR}/cfg/
  #     COMMAND ${CMAKE_COMMAND} -E make_directory ${TD_TESTS_OUTPUT_DIR}/log/
  #     COMMAND ${CMAKE_COMMAND} -E make_directory ${TD_TESTS_OUTPUT_DIR}/data/
  #     COMMAND ${CMAKE_COMMAND} -E echo dataDir ${TD_TESTS_OUTPUT_DIR}/data > ${TD_TESTS_OUTPUT_DIR}/cfg/taos.cfg
  #     COMMAND ${CMAKE_COMMAND} -E echo logDir ${TD_TESTS_OUTPUT_DIR}/log  >> ${TD_TESTS_OUTPUT_DIR}/cfg/taos.cfg
  #     COMMAND ${CMAKE_COMMAND} -E echo charset UTF-8  >> ${TD_TESTS_OUTPUT_DIR}/cfg/taos.cfg
  #     COMMENT "prepare taosd environment")
  # ADD_CUSTOM_TARGET(${PREPARE_ENV_TARGET} ALL WORKING_DIRECTORY ${TD_EXECUTABLE_OUTPUT_PATH} DEPENDS ${PREPARE_ENV_CMD})
ENDIF ()

