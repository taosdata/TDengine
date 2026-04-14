aux_source_directory(. MNODE_AUDIT_TEST_SRC)
add_executable(mndAuditTest ${MNODE_AUDIT_TEST_SRC})
target_include_directories(
    mndAuditTest
    # White-box access to auditInt.h (internal SAudit struct) for tests.
    PRIVATE "${TD_SOURCE_DIR}/source/libs/audit/inc"
)
target_link_libraries(
    mndAuditTest
    PUBLIC sut
)

if(NOT ${TD_WINDOWS})
    add_test(
        NAME mndAuditTest
        COMMAND mndAuditTest
    )
endif(NOT ${TD_WINDOWS})
