CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

IF (TD_LINUX_64)
  find_program(HAVE_ODBCINST NAMES odbcinst)

  IF (HAVE_ODBCINST)
    include(CheckSymbolExists)
    # shall we revert CMAKE_REQUIRED_LIBRARIES and how?
    set(CMAKE_REQUIRED_LIBRARIES odbc)
    check_symbol_exists(SQLExecute "sql.h" HAVE_ODBC_DEV)
    if(NOT (HAVE_ODBC_DEV))
      unset(HAVE_ODBC_DEV CACHE)
      message(WARNING "unixodbc-dev is not installed yet, you may install it under ubuntu by typing: sudo apt install unixodbc-dev")
    else ()
      message(STATUS "unixodbc/unixodbc-dev are installed, and odbc connector will be built")
      AUX_SOURCE_DIRECTORY(src SRC)

      # generate dynamic library (*.so)
      ADD_LIBRARY(todbc SHARED ${SRC})
      SET_TARGET_PROPERTIES(todbc PROPERTIES CLEAN_DIRECT_OUTPUT 1)
      SET_TARGET_PROPERTIES(todbc PROPERTIES VERSION ${TD_VER_NUMBER} SOVERSION 1)
      TARGET_LINK_LIBRARIES(todbc taos)

      install(CODE "execute_process(COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/src/install.sh ${CMAKE_BINARY_DIR})")

      ADD_SUBDIRECTORY(tests)
    endif()
  ELSE ()
    message(WARNING "unixodbc is not installed yet, you may install it under ubuntu by typing: sudo apt install unixodbc")
  ENDIF ()
ENDIF ()

