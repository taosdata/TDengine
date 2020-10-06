CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(src SRC)

# generate dynamic library (*.so)
ADD_LIBRARY(todbc SHARED ${SRC})
SET_TARGET_PROPERTIES(todbc PROPERTIES CLEAN_DIRECT_OUTPUT 1)
SET_TARGET_PROPERTIES(todbc PROPERTIES VERSION ${TD_VER_NUMBER} SOVERSION 1)
TARGET_LINK_LIBRARIES(todbc taos)

install(CODE "execute_process(COMMAND sudo odbcinst -i -d -f ${CMAKE_CURRENT_SOURCE_DIR}/src/template.ini
                              COMMAND odbcinst -i -s -f ${CMAKE_CURRENT_SOURCE_DIR}/src/template.dsn
                              COMMAND echo odbc install done)")

ADD_SUBDIRECTORY(tests)

