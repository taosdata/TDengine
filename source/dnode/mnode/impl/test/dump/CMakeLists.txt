aux_source_directory(. MNODE_DUMP_TEST_SRC)
add_executable(mndDumpOverlayTest ${MNODE_DUMP_TEST_SRC})
target_link_libraries(
    mndDumpOverlayTest
    PUBLIC sut
)
# mndDump.h lives in the impl/inc directory, which is not covered by sut's default header search paths,
# so explicitly add ../../inc (consistent with the sibling vgroup/CMakeLists.txt pattern).
target_include_directories(
    mndDumpOverlayTest
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
)

# Consistent with the sibling vgroup/stb/CMakeLists.txt: do not register add_test on Windows.
if(NOT TD_WINDOWS)
    add_test(
        NAME mndDumpOverlayTest
        COMMAND mndDumpOverlayTest
    )
endif(NOT TD_WINDOWS)
