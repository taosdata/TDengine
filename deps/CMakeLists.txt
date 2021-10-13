PROJECT(TDengine)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  CMAKE_MINIMUM_REQUIRED(VERSION 2.8...3.20)
ELSE ()
  CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
ENDIF ()

ADD_SUBDIRECTORY(zlib-1.2.11)
ADD_SUBDIRECTORY(pthread)
ADD_SUBDIRECTORY(regex)
ADD_SUBDIRECTORY(iconv)
ADD_SUBDIRECTORY(lz4)
ADD_SUBDIRECTORY(cJson)
ADD_SUBDIRECTORY(wepoll)
ADD_SUBDIRECTORY(MsvcLibX)
ADD_SUBDIRECTORY(rmonotonic)
ADD_SUBDIRECTORY(lua)

IF (TD_LINUX AND TD_MQTT)
  ADD_SUBDIRECTORY(MQTT-C)
ENDIF ()

IF (TD_DARWIN AND TD_MQTT)
  ADD_SUBDIRECTORY(MQTT-C)
ENDIF ()

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
  MESSAGE("setup deps/jemalloc, current source dir:" ${CMAKE_CURRENT_SOURCE_DIR})
  MESSAGE("binary dir:" ${CMAKE_BINARY_DIR})
  include(ExternalProject)
  ExternalProject_Add(jemalloc
    PREFIX        "jemalloc"
    SOURCE_DIR    ${CMAKE_CURRENT_SOURCE_DIR}/jemalloc
    BUILD_IN_SOURCE     1
    CONFIGURE_COMMAND   ./autogen.sh COMMAND ./configure --prefix=${CMAKE_BINARY_DIR}/build/
    BUILD_COMMAND       ${MAKE}
  )
ENDIF ()

IF (NOT "${TSZ_ENABLED}" MATCHES "false")
  option(NEED_DOWN "need download file" ON)
ENDIF()

#
# below is download submodules
#

function(cat IN_FILE OUT_FILE)
  file(READ ${IN_FILE} CONTENTS)
  file(APPEND ${OUT_FILE} "${CONTENTS}")
endfunction(cat IN_FILE OUT_FILE)

IF (${NEED_DOWN})
  MESSAGE("Start download external projects ...")
  set(CMAKE_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/deps/cmake")
  set(DEPS_DOWN_DIR "${CMAKE_BINARY_DIR}/deps-download")
  set(DEPS_TMP_FILE "${CMAKE_BINARY_DIR}/deps_tmp_CMakeLists.txt.in")

  configure_file("${CMAKE_SUPPORT_DIR}/deps_CMakeLists.txt.in" ${DEPS_TMP_FILE})
  IF (NOT "${TSZ_ENABLED}" MATCHES "false")
    cat("${CMAKE_SUPPORT_DIR}/TSZ_CMakeLists.txt.in" ${DEPS_TMP_FILE})
    MESSAGE(" external project TSZ add to download list.")
  ENDIF()
  configure_file(${DEPS_TMP_FILE} "${DEPS_DOWN_DIR}/CMakeLists.txt")
  execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" WORKING_DIRECTORY "${DEPS_DOWN_DIR}")
  execute_process(COMMAND "${CMAKE_COMMAND}" --build "${DEPS_DOWN_DIR}")
  MESSAGE("Download external projects finished.")
ENDIF()

IF (NOT ${TSZ_ENABLED} MATCHES "false")
  ADD_SUBDIRECTORY(TSZ)  
ENDIF()
