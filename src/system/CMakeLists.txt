CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(./src SRC)
INCLUDE_DIRECTORIES(${PRJ_HEADER_PATH} ./inc ../taos/inc ../client/inc ../modules/http/inc ../modules/monitor/inc ../../deps/inc)

ADD_EXECUTABLE(taosd ${SRC})
TARGET_LINK_LIBRARIES(taosd taos_static trpc tutil sdb monitor pthread http z)

SET(PREPARE_ENV_CMD "prepare_env_cmd")
SET(PREPARE_ENV_TARGET "prepare_env_target")
ADD_CUSTOM_COMMAND(OUTPUT ${PREPARE_ENV_CMD}
  POST_BUILD 
  COMMAND echo "make test directory"
  DEPENDS taosd
  COMMAND ${CMAKE_COMMAND} -E make_directory ${TESTS_OUTPUT_DIR}/cfg/
  COMMAND ${CMAKE_COMMAND} -E make_directory ${TESTS_OUTPUT_DIR}/log/
  COMMAND ${CMAKE_COMMAND} -E make_directory ${TESTS_OUTPUT_DIR}/data/
  COMMAND ${CMAKE_COMMAND} -E echo dataDir ${TESTS_OUTPUT_DIR}/data > ${TESTS_OUTPUT_DIR}/cfg/taos.cfg
  COMMAND ${CMAKE_COMMAND} -E echo logDir ${TESTS_OUTPUT_DIR}/log  >> ${TESTS_OUTPUT_DIR}/cfg/taos.cfg
  COMMAND ${CMAKE_COMMAND} -E echo charset UTF-8  >> ${TESTS_OUTPUT_DIR}/cfg/taos.cfg
  COMMENT "prepare taosd environment")
ADD_CUSTOM_TARGET(${PREPARE_ENV_TARGET} ALL WORKING_DIRECTORY ${EXECUTABLE_OUTPUT_PATH} DEPENDS ${PREPARE_ENV_CMD})

