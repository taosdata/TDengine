CMAKE_MINIMUM_REQUIRED(VERSION 3.0...3.20)
PROJECT(TDengine)

aux_source_directory(. SRC)
add_library(todbc_base STATIC ${SRC})

if (TD_LINUX OR TD_DARWIN)
  target_include_directories(todbc_base PRIVATE ${ODBC_INCLUDE_DIRECTORY})
endif ()

