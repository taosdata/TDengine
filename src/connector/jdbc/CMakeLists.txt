CMAKE_MINIMUM_REQUIRED(VERSION 3.5)

PROJECT(TDengine)

SET(JDBC_CMD_NAME "jdbc_cmd")
SET(JDBC_TARGET_NAME "jdbc_target")

FIND_PATH(MVN_INSTALLED mvn /usr/bin/)
IF (MVN_INSTALLED)
  MESSAGE(STATUS "MVN is installed and JDBC will be compiled")
  ADD_CUSTOM_COMMAND(OUTPUT ${JDBC_CMD_NAME}
    POST_BUILD
    COMMAND mvn -Dmaven.test.skip=true install -f ${CMAKE_CURRENT_SOURCE_DIR}/pom.xml
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_SOURCE_DIR}/target/taos-jdbcdriver*dist.* ${EXECUTABLE_OUTPUT_PATH}/../lib/
    COMMAND mvn -Dmaven.test.skip=true clean -f ${CMAKE_CURRENT_SOURCE_DIR}/pom.xml
    COMMENT "build jdbc driver")
  ADD_CUSTOM_TARGET(${JDBC_TARGET_NAME} ALL WORKING_DIRECTORY ${EXECUTABLE_OUTPUT_PATH} DEPENDS ${JDBC_CMD_NAME})
ELSE ()
  MESSAGE(STATUS "MVN is not installed and JDBC is not compiled")
ENDIF ()


