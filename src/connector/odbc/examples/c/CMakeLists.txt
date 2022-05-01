PROJECT(TDengine)

ADD_EXECUTABLE(tcodbc main.c ../../src/todbc_log.c)

IF (TD_LINUX OR TD_DARWIN)
  TARGET_LINK_LIBRARIES(tcodbc taos odbc)
ENDIF ()

IF (TD_DARWIN)
  # TODO: later we shall specify directories according to the specific target intel/arm64
  target_include_directories(tcodbc PRIVATE /usr/local/include /opt/homebrew/include)
  target_link_directories(tcodbc PUBLIC /usr/local/lib /opt/homebrew/lib)
ENDIF ()

IF (TD_WINDOWS_64)
  SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} /GL")
  SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} /GL")
  TARGET_LINK_LIBRARIES(tcodbc taos_static odbc32 odbccp32 user32 legacy_stdio_definitions os)

  ADD_EXECUTABLE(tms main.cpp)
  TARGET_LINK_LIBRARIES(tms odbc32)
ENDIF ()

