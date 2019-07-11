CMAKE_MINIMUM_REQUIRED(VERSION 3.5)

PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(./src SRC)
INCLUDE_DIRECTORIES(${PRJ_HEADER_PATH} ./inc ../http/inc ../../../deps/inc ../../client/inc)

ADD_LIBRARY(monitor ${SRC})
TARGET_LINK_LIBRARIES(monitor taos_static trpc tutil z)
