CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(./src SRC)
INCLUDE_DIRECTORIES(${PRJ_HEADER_PATH} ./inc)

ADD_LIBRARY(tutil ${SRC})
TARGET_LINK_LIBRARIES(tutil m pthread)

FIND_PATH(ICONV_INCLUDE_EXIST iconv.h /usr/include/ /usr/local/include/)
IF (ICONV_INCLUDE_EXIST)
  ADD_DEFINITIONS(-DUSE_LIBICONV)

  FIND_PATH(ICONV_LIBRARY_A_EXIST libiconv.a /usr/lib/ /usr/local/lib/ /lib64)
  FIND_PATH(ICONV_LIBRARY_SO_EXIST libiconv.so /usr/lib/ /usr/local/lib/ /lib64)
  IF (ICONV_LIBRARY_A_EXIST OR ICONV_LIBRARY_SO_EXIST)
    MESSAGE(STATUS "Use the installed libiconv library")
    TARGET_LINK_LIBRARIES(tutil iconv)
  ELSE ()
    # libiconv library is already included in GLIBC,
    MESSAGE(STATUS "Use the iconv functions in GLIBC")
  ENDIF ()
ELSE ()
  MESSAGE(STATUS "Failed to find iconv, use default encoding method")
ENDIF ()


