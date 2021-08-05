PROJECT(TDengine)

ADD_EXECUTABLE(todbcinst main.c)
ADD_EXECUTABLE(tconv     tconv.c)

IF (TD_LINUX OR TD_DARWIN)
  TARGET_LINK_LIBRARIES(todbcinst odbc odbcinst)
ENDIF ()

IF (TD_DARWIN)
  target_include_directories(todbcinst PRIVATE /usr/local/include)
  target_link_directories(todbcinst PUBLIC /usr/local/lib)
  target_include_directories(tconv PRIVATE /usr/local/include)
  target_link_directories(tconv PUBLIC /usr/local/lib)
  TARGET_LINK_LIBRARIES(tconv iconv)
ENDIF ()

IF (TD_WINDOWS_64)
  TARGET_LINK_LIBRARIES(todbcinst odbc32 odbccp32 user32 legacy_stdio_definitions os)
  TARGET_LINK_LIBRARIES(tconv taos)
  INSTALL(FILES ${EXECUTABLE_OUTPUT_PATH}/todbcinst.exe DESTINATION .)
ENDIF ()

