aux_source_directory(src TOTP_SRC)

IF(TD_ENTERPRISE)
  LIST(APPEND TOTP_SRC ${TD_ENTERPRISE_DIR}/src/plugins/totp/totpImpl.c)
ENDIF()

add_library(totp STATIC ${TOTP_SRC})
target_include_directories(
  totp
  PUBLIC "${TD_SOURCE_DIR}/include/libs/totp"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

if (NOT ${TD_WINDOWS})
  DEP_ext_ssl(totp)
endif()
