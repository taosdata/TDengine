aux_source_directory(src CRYPT_SRC)

IF(TD_ENTERPRISE)
  LIST(APPEND CRYPT_SRC ${TD_ENTERPRISE_DIR}/src/plugins/crypt/cryptImpl.c)
ENDIF()

add_library(crypt STATIC ${CRYPT_SRC})
target_include_directories(
  crypt
  PUBLIC "${TD_SOURCE_DIR}/include/libs/crypt"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(crypt common sm4)
