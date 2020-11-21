PROJECT(TDengine)

IF (TD_LINUX)
  ADD_EXECUTABLE(todbcinst main.c)
  TARGET_LINK_LIBRARIES(todbcinst odbc odbcinst)
ENDIF ()

IF (TD_WINDOWS_64)
  ADD_EXECUTABLE(todbcinst main.c)
  TARGET_LINK_LIBRARIES(todbcinst odbc32 odbccp32 user32 legacy_stdio_definitions os)
  INSTALL(FILES ${EXECUTABLE_OUTPUT_PATH}/todbcinst.exe DESTINATION .)
ENDIF ()
