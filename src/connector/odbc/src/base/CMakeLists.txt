CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

aux_source_directory(. SRC)
add_library(todbc_base STATIC ${SRC})

if (TD_DARWIN)
  target_include_directories(todbc_base PRIVATE /usr/local/include)
endif ()

