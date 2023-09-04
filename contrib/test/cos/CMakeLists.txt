add_executable(cosTest "")
target_sources(cosTest
  PRIVATE
  "${CMAKE_CURRENT_SOURCE_DIR}/main.c"
  )

#find_path(APR_INCLUDE_DIR apr-1/apr_time.h)
#find_path(APR_UTIL_INCLUDE_DIR apr/include/apr-1/apr_md5.h)
#find_path(MINIXML_INCLUDE_DIR mxml.h)
#find_path(CURL_INCLUDE_DIR curl/curl.h)

#include_directories (${MINIXML_INCLUDE_DIR})
#include_directories (${CURL_INCLUDE_DIR})
FIND_PROGRAM(APR_CONFIG_BIN NAMES apr-config apr-1-config PATHS /usr/bin /usr/local/bin /usr/local/apr/bin/)
#FIND_PROGRAM(APU_CONFIG_BIN NAMES apu-config apu-1-config PATHS /usr/bin /usr/local/bin /usr/local/apr/bin/)

IF (APR_CONFIG_BIN)
  EXECUTE_PROCESS(
    COMMAND ${APR_CONFIG_BIN} --includedir
    OUTPUT_VARIABLE APR_INCLUDE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    )
ENDIF()
#IF (APU_CONFIG_BIN)
#  EXECUTE_PROCESS(
#    COMMAND ${APU_CONFIG_BIN} --includedir
#    OUTPUT_VARIABLE APR_UTIL_INCLUDE_DIR
#    OUTPUT_STRIP_TRAILING_WHITESPACE
#    )
#ENDIF()

include_directories (${APR_INCLUDE_DIR})
#include_directories (${APR_UTIL_INCLUDE_DIR})

target_include_directories(
  cosTest
  PUBLIC "${TD_SOURCE_DIR}/contrib/cos-c-sdk-v5/cos_c_sdk"
  )

#find_library(APR_LIBRARY apr-1 PATHS /usr/local/apr/lib/)
#find_library(APR_UTIL_LIBRARY aprutil-1 PATHS /usr/local/apr/lib/)
#find_library(MINIXML_LIBRARY mxml)
#find_library(CURL_LIBRARY curl)

target_link_libraries(cosTest cos_c_sdk)
target_link_libraries(cosTest apr-1})
target_link_libraries(cosTest aprutil-1})
target_link_libraries(cosTest mxml)
target_link_libraries(cosTest curl)
