add_executable(wlrFormatTest "")
DEP_ext_gtest(wlrFormatTest)
target_sources(wlrFormatTest
    PRIVATE
    "wlrFormatTest.cpp"
)
target_include_directories(wlrFormatTest
    PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
    # For the TSDB_DATA_TYPE_* codes the type-code conformance test checks the
    # library against. Public taos.h only, as in the library itself.
    "${TD_SOURCE_DIR}/include/client"
)

target_link_libraries(wlrFormatTest
    PRIVATE
    wlr
    os
    util
)

# WLR_FORMAT_HAVE_IMPL arrives transitively from `wlr` (it exports it PUBLIC), which
# is what switches wlrFormatTest from reference-model-only into the full conformance
# suite. Stated again here so the coupling is visible at the target that depends on
# it: if this ever stops being defined, 26 conformance cases compile out, and
# wlrFormat.ConformanceSuiteMustBeCompiledIn fails the build's tests rather than
# letting them pass green with the shipped encoder untested.
target_compile_definitions(wlrFormatTest PRIVATE WLR_FORMAT_HAVE_IMPL)

enable_testing()
add_test(
    NAME wlr_format_test
    COMMAND wlrFormatTest
)

# Reads a real recorder-produced segment. Provides its own main() to take the file
# path as argv[1]; skips when no path is given, so it is safe in an unattended run.
add_executable(wlrSegmentReadTest "")
DEP_ext_gtest(wlrSegmentReadTest)
target_sources(wlrSegmentReadTest
    PRIVATE
    "wlrSegmentReadTest.cpp"
)
target_include_directories(wlrSegmentReadTest
    PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/../inc"
)
target_link_libraries(wlrSegmentReadTest
    PRIVATE
    wlr
    os
    util
)
add_test(
    NAME wlr_segment_read_test
    COMMAND wlrSegmentReadTest
)
