aux_source_directory(. MNODE_VGROUP_TEST_SRC)
add_executable(vgroupFormatTest ${MNODE_VGROUP_TEST_SRC})
target_link_libraries(
    vgroupFormatTest
    PUBLIC sut
)
# mndVgroup.h lives in the impl/inc directory, which is not covered by sut's default header search paths,
# so explicitly add ../../inc (i.e. impl/inc) to ensure the test can find the mndBytesToHuman declaration.
target_include_directories(
    vgroupFormatTest
    PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/../../inc"
)

# Consistent with the sibling stb/CMakeLists.txt: do not register add_test on Windows.
if(NOT TD_WINDOWS)
    add_test(
        NAME vgroupFormatTest
        COMMAND vgroupFormatTest
    )
endif(NOT TD_WINDOWS)
