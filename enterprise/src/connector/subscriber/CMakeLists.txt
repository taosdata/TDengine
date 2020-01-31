CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(TDengine)

IF (TD_MVN_INSTALLED)
  SET(SUBSCRIBER_CMD_NAME "subscriber_cmd")
  SET(SUBSCRIBER_TARGET_NAME "subscriber_target")
  MESSAGE(STATUS "MVN is installed and subscriber will be compiled")
  ADD_CUSTOM_COMMAND(OUTPUT ${SUBSCRIBER_CMD_NAME}
    POST_BUILD
    COMMAND mvn -Dmaven.test.skip=true install -f ${CMAKE_CURRENT_SOURCE_DIR}/pom.xml
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/target/taos-subscriber-1.0.1-dist.jar ${EXECUTABLE_OUTPUT_PATH}/../lib/
    COMMAND mvn -Dmaven.test.skip=true clean -f ${CMAKE_CURRENT_SOURCE_DIR}/pom.xml
    COMMENT "build java subscriber driver")
  ADD_CUSTOM_TARGET(${SUBSCRIBER_TARGET_NAME} ALL WORKING_DIRECTORY ${EXECUTABLE_OUTPUT_PATH} DEPENDS ${SUBSCRIBER_CMD_NAME})
ELSE ()
  MESSAGE(STATUS "MVN is not installed and subscriber is not compiled")
ENDIF ()


