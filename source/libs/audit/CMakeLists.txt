aux_source_directory(src AUDIT_SRC)
IF (TD_ENTERPRISE)
  LIST(APPEND AUDIT_SRC ${TD_ENTERPRISE_DIR}/src/plugins/audit/src/audit.c)
ENDIF ()

add_library(audit STATIC ${AUDIT_SRC})
target_include_directories(
  audit
  PUBLIC "${TD_SOURCE_DIR}/include/libs/audit"
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/inc"
)

target_link_libraries(audit os util common transport mnode)
