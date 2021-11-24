PROJECT(TDengine)

IF (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  CMAKE_MINIMUM_REQUIRED(VERSION 3.0...3.20)
ELSE ()
  CMAKE_MINIMUM_REQUIRED(VERSION 3.0)
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

IF (TD_BUILD_LUA)
  ADD_SUBDIRECTORY(lua)
ENDIF ()

IF (TD_LINUX AND TD_MQTT)
  ADD_SUBDIRECTORY(MQTT-C)
ENDIF ()

IF (TD_DARWIN AND TD_MQTT)
  ADD_SUBDIRECTORY(MQTT-C)
ENDIF ()

IF (TD_AVRO_SUPPORT)
  MESSAGE("")
  MESSAGE("${Green} ENABLE avro format support ${ColourReset}")
  MESSAGE("")
  include(ExternalProject)
  ExternalProject_Add(
    apache-avro
    PREFIX          "avro"
    SOURCE_DIR      ${CMAKE_CURRENT_SOURCE_DIR}/avro/lang/c
    BUILD_IN_SOURCE 1
    PATCH_COMMAND
        COMMAND   git clean -f -d
        COMMAND   sed -i.bak -e "/TARGETS avroappend/d" ${CMAKE_CURRENT_SOURCE_DIR}/avro/lang/c/src/CMakeLists.txt
        COMMAND   sed -i.bak -e "/TARGETS avrocat/d" ${CMAKE_CURRENT_SOURCE_DIR}/avro/lang/c/src/CMakeLists.txt
        COMMAND   sed -i.bak -e "/TARGETS avromod/d" ${CMAKE_CURRENT_SOURCE_DIR}/avro/lang/c/src/CMakeLists.txt
        COMMAND   sed -i.bak -e "/TARGETS avropipe/d" ${CMAKE_CURRENT_SOURCE_DIR}/avro/lang/c/src/CMakeLists.txt
    CONFIGURE_COMMAND   cmake -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}/build
    )
ELSE ()
  MESSAGE("")
  MESSAGE("${Yellow} NO avro format support ${ColourReset}")
  MESSAGE("")
ENDIF ()

IF (TD_LINUX_64 AND JEMALLOC_ENABLED)
  MESSAGE("")
  MESSAGE("${Green} ENABLE jemalloc ${ColourReset}")
  MESSAGE("")
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
  INCLUDE_DIRECTORIES(${CMAKE_BINARY_DIR}/build/include)
ENDIF ()

IF (${TSZ_ENABLED} MATCHES "true")
  ADD_SUBDIRECTORY(TSZ)
ENDIF()
