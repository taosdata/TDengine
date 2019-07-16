CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(./src SRC)
INCLUDE_DIRECTORIES(${PRJ_HEADER_PATH} ./inc ./jni ./jni/jni)

# generate dynamic library (*.so)
ADD_LIBRARY(taos SHARED ${SRC})
ADD_LIBRARY(taos_static STATIC ${SRC})
TARGET_LINK_LIBRARIES(taos_static trpc tutil pthread m rt)
TARGET_LINK_LIBRARIES(taos trpc tutil pthread m rt)

# set the static lib name
SET_TARGET_PROPERTIES(taos_static PROPERTIES OUTPUT_NAME "taos")

# enable static lib and so exists
SET_TARGET_PROPERTIES(taos PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(taos_static PROPERTIES CLEAN_DIRECT_OUTPUT 1)

#set version of .so
#VERSION    so version
#SOVERSION  api version

execute_process(COMMAND chmod 777 ${PROJECT_SOURCE_DIR}/../../packaging/tools/get_version.sh)
execute_process(COMMAND ${PROJECT_SOURCE_DIR}/../../packaging/tools/get_version.sh
  OUTPUT_VARIABLE
  VERSION_INFO)
MESSAGE(STATUS "build version ${VERSION_INFO}")

SET_TARGET_PROPERTIES(taos PROPERTIES VERSION ${VERSION_INFO} SOVERSION 1)