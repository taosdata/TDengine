CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

IF (TD_LINUX_64)
  FLEX_TARGET(todbcFlexScanner
              todbc_scanner.l
              ${CMAKE_CURRENT_BINARY_DIR}/todbc_scanner.c
  )
  set(todbc_flex_scanner_src
      ${FLEX_todbcFlexScanner_OUTPUTS}
  )
  AUX_SOURCE_DIRECTORY(. SRC)

  # generate dynamic library (*.so)
  ADD_LIBRARY(todbc SHARED ${SRC} ${todbc_flex_scanner_src})
  SET_TARGET_PROPERTIES(todbc PROPERTIES CLEAN_DIRECT_OUTPUT 1)
  SET_TARGET_PROPERTIES(todbc PROPERTIES VERSION ${TD_VER_NUMBER} SOVERSION 1)
  TARGET_LINK_LIBRARIES(todbc taos)
  target_include_directories(todbc PUBLIC
                             .)

  install(CODE "execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/install.sh ${CMAKE_BINARY_DIR})")
ENDIF ()

