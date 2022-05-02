PROJECT(TDengine)

ADD_EXECUTABLE(tcodbc main.c ../../src/todbc_log.c)

IF (TD_LINUX OR TD_DARWIN)
  target_include_directories(tcodbc PRIVATE ${ODBC_INCLUDE_DIRECTORY})
  target_link_directories(tcodbc PUBLIC ${ODBC_LIBRARY_DIRECTORY})
  TARGET_LINK_LIBRARIES(tcodbc taos odbc)
ENDIF ()

IF (TD_WINDOWS_64)
  SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /GL")
  SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /GL")
  TARGET_LINK_LIBRARIES(tcodbc taos_static odbc32 odbccp32 user32 legacy_stdio_definitions os)

  ADD_EXECUTABLE(tms main.cpp)
  TARGET_LINK_LIBRARIES(tms odbc32)
ENDIF ()

