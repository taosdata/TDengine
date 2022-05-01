PROJECT(TDengine)

ADD_EXECUTABLE(todbcinst main.c)
ADD_EXECUTABLE(tconv     tconv.c)

IF (TD_LINUX OR TD_DARWIN)
  target_include_directories(todbcinst PRIVATE . ${ODBC_INCLUDE_DIRECTORY} ${ODBCINST_INCLUDE_DIRECTORY})
  target_link_directories(todbcinst PUBLIC ${ODBC_LIBRARY_DIRECTORY} ${ODBCINST_LIBRARY_DIRECTORY})
  TARGET_LINK_LIBRARIES(todbcinst odbc odbcinst)
ENDIF ()

IF (TD_DARWIN)
  TARGET_LINK_LIBRARIES(tconv iconv)
ENDIF ()

IF (TD_WINDOWS_64)
  TARGET_LINK_LIBRARIES(todbcinst odbc32 odbccp32 user32 legacy_stdio_definitions os)
  TARGET_LINK_LIBRARIES(tconv taos)
  INSTALL(FILES ${EXECUTABLE_OUTPUT_PATH}/todbcinst.exe DESTINATION .)
ENDIF ()

