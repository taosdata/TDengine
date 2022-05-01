CMAKE_MINIMUM_REQUIRED(VERSION 3.0...3.20)
PROJECT(TDengine)

aux_source_directory(. SRC)
add_library(todbc_base STATIC ${SRC})

if (TD_DARWIN)
  # TODO: later we shall specify directories according to the specific target intel/arm64
  target_include_directories(todbc_base PRIVATE /usr/local/include /opt/homebrew/include)
endif ()

