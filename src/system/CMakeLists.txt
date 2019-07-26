CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(TDengine)

IF (TD_LINUX)

  AUX_SOURCE_DIRECTORY(./src SRC)
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/src/inc)
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/src/client/inc)
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/src/modules/http/inc)
  INCLUDE_DIRECTORIES(${TD_ROOT_DIR}/src/modules/monitor/inc)
  INCLUDE_DIRECTORIES(${TD_OS_DIR}/inc inc)
  
  ADD_EXECUTABLE(taosd ${SRC})
  TARGET_LINK_LIBRARIES(taosd taos_static trpc tutil sdb monitor pthread http)

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